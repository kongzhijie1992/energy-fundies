"""FTR pricing toolkit."""

# Integration Plan:
# - Install path: src/fundie/ftr/ alongside src/fundie/cli.py entrypoint.
# - Entrypoints: add console script `fundie` with `fundie ftr ...` subcommands.
# - Config updates: extend pyproject.toml packages list + scripts; add .fundie_cache/ to gitignore.
# - Tests: pytest discovers tests under tests/ (see tests/ftr/*).

from .core.types import ContractSpec, ContractType, ValuationResult
from .pricing.engine import price_batch, price_contract
from .version import __version__

__all__ = [
    "ContractSpec",
    "ContractType",
    "ValuationResult",
    "price_batch",
    "price_contract",
]
