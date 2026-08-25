from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel

from boot.bootstrap import init_config, init_env, init_logging
from database.migration_config import get_migration_database_url
from database.model_registry import import_all_models

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Models read the validated YAML configuration while modules are imported.
init_env()
init_logging()
init_config()
import_all_models()

target_metadata = SQLModel.metadata


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(
        get_migration_database_url(),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(do_run_migrations)
    finally:
        await engine.dispose()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        do_run_migrations(supplied_connection)
        return

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    raise RuntimeError("Offline migration generation is not supported; use an online MariaDB connection")

run_migrations_online()
