"""Configuration helpers shared by Alembic and migration commands."""

from __future__ import annotations

import os
from pathlib import Path

from alembic.config import Config as AlembicConfig
from sqlalchemy import URL, make_url

from boot.bootstrap import init_config, init_env, init_logging

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALEMBIC_INI_PATH = PROJECT_ROOT / "alembic.ini"
SUPPORTED_ASYNC_DRIVERS = {"mariadb+asyncmy", "mysql+asyncmy"}


def initialize_migration_runtime() -> None:
    """Load only env, logging and YAML config; do not initialize OSS or Celery."""
    init_env()
    init_logging()
    init_config()


def get_migration_database_url() -> URL:
    """Return an async SQLAlchemy URL, with a CI/admin override when provided."""
    override = os.getenv("MIGRATION_DATABASE_URL")
    if override:
        url = make_url(override)
    else:
        from boot.config import get_config  # imported after minimal initialization

        config = get_config()
        password = os.getenv("DATABASE_PASSWORD")
        if not password:
            raise ValueError("DATABASE_PASSWORD environment variable is required")
        url = URL.create(
            drivername=f"{config.database.db_type}+asyncmy",
            username=config.database.db_username,
            password=password,
            host=config.database.host,
            port=config.database.port,
            database=config.database.db_name,
            query={"charset": "utf8mb4"},
        )

    if url.drivername not in SUPPORTED_ASYNC_DRIVERS:
        supported = ", ".join(sorted(SUPPORTED_ASYNC_DRIVERS))
        raise ValueError(f"Migration URL must use an asyncmy driver ({supported})")
    if not url.database:
        raise ValueError("Migration URL must include a database name")
    return url


def get_alembic_config() -> AlembicConfig:
    """Load the checked-in Alembic configuration without putting secrets in it."""
    return AlembicConfig(str(ALEMBIC_INI_PATH))
