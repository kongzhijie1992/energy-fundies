#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

import requests
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

KEYCLOAK_URL = "https://keycloak.tp.entsoe.eu/realms/tp/protocol/openid-connect/token"
FMS_LIST_URL = "https://fms.tp.entsoe.eu/listFolder"
FMS_DOWNLOAD_URL = "https://fms.tp.entsoe.eu/downloadFileContent"
CLIENT_ID = "tp-fms-public"


def get_token(username: str, password: str) -> str:
    data = {
        "client_id": CLIENT_ID,
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(KEYCLOAK_URL, headers=headers, data=data, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {resp.text}")
    return token


def list_folder(token: str, folder_path: str, page_size: int = 5000) -> dict[str, Any]:
    payload = {
        "path": folder_path,
        "sorterList": [{"key": "periodCovered.from", "ascending": True}],
        "pageInfo": {"pageIndex": 0, "pageSize": page_size},
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(FMS_LIST_URL, headers=headers, data=json.dumps(payload), timeout=60)
    resp.raise_for_status()
    return resp.json()


def _extract_items(listing: dict[str, Any]) -> list[Any]:
    for key in ("items", "fileList", "content", "files", "contentItemList"):
        value = listing.get(key)
        if isinstance(value, list):
            return value
    return []


def iter_files(items: Iterable[Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for item in items:
        if isinstance(item, dict):
            name = item.get("filename") or item.get("name")
            if name:
                yield name, item
        elif isinstance(item, str):
            yield item, {}


def download_file(
    token: str,
    folder: str,
    filename: str,
    out_path: Path,
    *,
    as_zip: bool = False,
    last_update: str | None = None,
) -> None:
    payload = {
        "folder": folder,
        "filename": filename,
        "topLevelFolder": "TP_export",
        "downloadAsZip": as_zip,
    }
    if last_update:
        payload["lastUpdateTimestamp"] = last_update
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(FMS_DOWNLOAD_URL, headers=headers, data=json.dumps(payload), timeout=120)
    resp.raise_for_status()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)


def build_s3_client(region: str | None):
    creds = {}
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    if access_key and secret_key:
        creds["aws_access_key_id"] = access_key
        creds["aws_secret_access_key"] = secret_key
        if session_token:
            creds["aws_session_token"] = session_token
    if region:
        return boto3.client("s3", region_name=region, **creds)
    return boto3.client("s3", **creds)


def s3_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def upload_to_s3(s3, local_path: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(local_path), bucket, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download raw ENTSO-E FMS files.")
    parser.add_argument("--folder", required=True, help="FMS folder path (e.g. /TP_export/PhysicalFlows_12.1.G_r3/)")
    parser.add_argument("--out-dir", default="data/raw/fms", help="Output directory")
    parser.add_argument("--pattern", default="", help="Regex filter for filenames")
    parser.add_argument("--max-files", type=int, default=0, help="Max files to download (0 = all)")
    parser.add_argument("--list-only", action="store_true", help="Only list files, do not download")
    parser.add_argument("--as-zip", action="store_true", help="Ask FMS to return zip content")
    parser.add_argument("--s3-bucket", default=os.getenv("POWER_DATA_BUCKET", ""), help="S3 bucket name")
    parser.add_argument("--s3-prefix", default="raw/entsoe", help="S3 key prefix (folder)")
    parser.add_argument("--s3-only", action="store_true", help="Upload to S3 and remove local files")
    parser.add_argument(
        "--s3-region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", ""),
        help="AWS region for S3 (optional)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip download/upload if the object already exists in S3",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    path = project_root.parent / ".env"
    if path.exists():
        load_dotenv(path, override=False)

    username = os.getenv("ENTSOE_TP_USERNAME", "")
    password = os.getenv("ENTSOE_TP_PASSWORD", "")
    if not username or not password:
        raise RuntimeError("Set ENTSOE_TP_USERNAME and ENTSOE_TP_PASSWORD in your environment.")

    token = get_token(username, password)
    listing = list_folder(token, args.folder)
    items = _extract_items(listing)

    regex = re.compile(args.pattern) if args.pattern else None
    matches: list[tuple[str, dict[str, Any]]] = []
    for name, meta in iter_files(items):
        if regex and not regex.search(name):
            continue
        matches.append((name, meta))

    print(f"Found {len(matches)} file(s) in {args.folder}")
    for name, _ in matches[:20]:
        print(name)

    if args.list_only:
        return

    out_dir = Path(args.out_dir)
    s3 = None
    if args.s3_bucket:
        region = args.s3_region or None
        s3 = build_s3_client(region)
    limit = args.max_files if args.max_files > 0 else len(matches)
    for name, meta in matches[:limit]:
        out_path = out_dir / name
        last_update = meta.get("lastUpdateTimestamp") if isinstance(meta, dict) else None
        key = f"{args.s3_prefix.rstrip('/')}/{name}" if args.s3_bucket else ""

        if args.s3_bucket and args.skip_existing and s3_object_exists(s3, args.s3_bucket, key):
            print(f"Skip existing s3://{args.s3_bucket}/{key}")
            continue

        download_file(
            token,
            args.folder,
            name,
            out_path=out_path,
            as_zip=args.as_zip,
            last_update=last_update,
        )
        print(f"Saved {out_path}")

        if args.s3_bucket:
            upload_to_s3(s3, out_path, args.s3_bucket, key)
            print(f"Uploaded s3://{args.s3_bucket}/{key}")
            if args.s3_only:
                out_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
