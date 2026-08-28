import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def pytest_sessionstart() -> None:
    """Give isolated migration tests a deterministic model configuration."""
    os.environ.setdefault("CONFIG_PATH", str(PROJECT_ROOT / "docker" / "test-config.yaml"))
    os.environ.setdefault(
        "REGION_PATH",
        str(
            PROJECT_ROOT
            / "external"
            / "countries-states-cities-database"
            / "json"
            / "countries.json"
        ),
    )
