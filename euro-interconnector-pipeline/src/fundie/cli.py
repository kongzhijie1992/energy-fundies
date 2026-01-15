from __future__ import annotations

import logging

import typer
from rich.logging import RichHandler

from .ftr import cli as ftr_cli

app = typer.Typer(add_completion=False, help="Fundie CLI")
app.add_typer(ftr_cli.app, name="ftr")


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_time=True, show_level=True)],
    )


@app.callback()
def _init(
    verbose: bool = typer.Option(False, "--verbose", help="Verbose logging"),
) -> None:
    _setup_logging(verbose)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
