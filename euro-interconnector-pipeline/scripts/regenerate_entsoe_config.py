#!/usr/bin/env python3
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import yaml
from entsoe.mappings import NEIGHBOURS, Area


def _zone_token(zone: str) -> str:
    return zone.replace("_", "")


def _all_zones() -> list[str]:
    zones: set[str] = set(NEIGHBOURS.keys())
    for neighs in NEIGHBOURS.values():
        zones.update(neighs)
    missing = sorted([z for z in zones if not Area.has_code(z)])
    if missing:
        raise SystemExit(f"Missing Area mappings for zones: {missing}")
    return sorted(zones)


def _all_directed_edges() -> list[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    for from_zone, neighs in NEIGHBOURS.items():
        for to_zone in neighs:
            edges.add((from_zone, to_zone))
    return sorted(edges)


def _dump_yaml(obj: object) -> str:
    return yaml.safe_dump(obj, sort_keys=False, default_flow_style=False, allow_unicode=True)


def write_config(*, config_dir: Path) -> None:
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    zones: dict[str, dict[str, str]] = {}
    for zone in _all_zones():
        a = Area[zone]
        zones[zone] = {"domain": a.code, "timezone": a.tz}

    borders: list[dict[str, str]] = []
    for from_zone, to_zone in _all_directed_edges():
        borders.append(
            {
                "border_id": f"{_zone_token(from_zone)}_{_zone_token(to_zone)}",
                "from_zone": from_zone,
                "to_zone": to_zone,
                "metric": "physical_flow",
                "direction_convention": "positive_from_to",
            }
        )

    zones_path = config_dir / "zones.yml"
    borders_path = config_dir / "borders.yml"

    zones_header = (
        "# Generated from entsoe-py entsoe.mappings.Area (domain/timezone)\n"
        f"# Timestamp (UTC): {ts}\n"
    )
    borders_header = (
        "# Generated from entsoe-py entsoe.mappings.NEIGHBOURS (directed edges)\n"
        f"# Timestamp (UTC): {ts}\n"
        "# Note: this is a large config; backfills will call ENTSO-E once per border.\n"
    )

    config_dir.mkdir(parents=True, exist_ok=True)
    zones_path.write_text(zones_header + _dump_yaml(zones), encoding="utf-8")
    borders_path.write_text(borders_header + _dump_yaml(borders), encoding="utf-8")

    print(f"Wrote {zones_path} ({len(zones)} zones)")
    print(f"Wrote {borders_path} ({len(borders)} borders)")


def main() -> None:
    write_config(config_dir=Path(__file__).resolve().parents[1] / "config")


if __name__ == "__main__":
    main()
