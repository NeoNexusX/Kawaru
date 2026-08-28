"""Safe command-line entry point for schema migrations."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from sqlmodel import SQLModel

from database.migration_config import (
    get_alembic_config,
    get_migration_database_url,
    initialize_migration_runtime,
)
from database.model_registry import import_all_models

MIGRATION_LOCK_NAME = "kawaru_alembic_upgrade"
MIGRATION_LOCK_TIMEOUT_SECONDS = 60


def _prepare_metadata() -> None:
    initialize_migration_runtime()
    import_all_models()


def _script_directory() -> ScriptDirectory:
    return ScriptDirectory.from_config(get_alembic_config())


def get_code_heads() -> tuple[str, ...]:
    return tuple(_script_directory().get_heads())


def require_single_head() -> str:
    heads = get_code_heads()
    if len(heads) != 1:
        raise RuntimeError(f"Expected exactly one Alembic head, found {len(heads)}: {heads}")
    return heads[0]


async def _ensure_target_database_exists() -> None:
    target_url = get_migration_database_url()
    database_name = target_url.database
    if database_name is None:
        raise RuntimeError("Target database name is missing")

    escaped_name = database_name.replace("`", "``")
    system_engine = create_async_engine(
        target_url.set(database="mysql"),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    try:
        async with system_engine.connect() as connection:
            await connection.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{escaped_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
    finally:
        await system_engine.dispose()


async def _get_table_names(connection: AsyncConnection) -> set[str]:
    result = await connection.execute(
        text(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE()"
        )
    )
    return {str(row[0]) for row in result}


def _run_alembic_command(connection: Connection, action: str, revision: str) -> None:
    config = get_alembic_config()
    config.attributes["connection"] = connection
    if action == "upgrade":
        command.upgrade(config, revision)
    elif action == "stamp":
        command.stamp(config, revision)
    else:
        raise ValueError(f"Unsupported Alembic action: {action}")


async def _acquire_lock(connection: AsyncConnection) -> None:
    acquired = await connection.scalar(
        text("SELECT GET_LOCK(:name, :timeout)"),
        {"name": MIGRATION_LOCK_NAME, "timeout": MIGRATION_LOCK_TIMEOUT_SECONDS},
    )
    if acquired != 1:
        raise RuntimeError("Could not acquire the MariaDB migration lock")


async def _release_lock(connection: AsyncConnection) -> None:
    if connection.in_transaction():
        await connection.rollback()
    await connection.execute(
        text("SELECT RELEASE_LOCK(:name)"),
        {"name": MIGRATION_LOCK_NAME},
    )
    await connection.commit()


async def _current_database_heads(connection: AsyncConnection) -> tuple[str, ...]:
    def read_heads(sync_connection: Connection) -> tuple[str, ...]:
        migration_context = MigrationContext.configure(sync_connection)
        return tuple(migration_context.get_current_heads())

    return await connection.run_sync(read_heads)


async def _open_target_connection() -> tuple[AsyncEngine, AsyncConnection]:
    engine = create_async_engine(
        get_migration_database_url(),
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    try:
        connection = await engine.connect()
    except Exception:
        await engine.dispose()
        raise
    return engine, connection


async def upgrade_database(revision: str = "head") -> None:
    """Upgrade an empty or already-versioned database while holding an advisory lock."""
    _prepare_metadata()
    require_single_head()
    await _ensure_target_database_exists()
    engine, connection = await _open_target_connection()
    try:
        await _acquire_lock(connection)
        try:
            tables = await _get_table_names(connection)
            if tables and "alembic_version" not in tables:
                raise RuntimeError(
                    "Refusing to migrate a non-empty, unversioned database. "
                    "Back it up, verify it, then run 'python -m database.migration stamp-baseline --confirm'."
                )
            # Metadata checks start an implicit transaction. End it so Alembic owns
            # and commits the migration transaction; MariaDB advisory locks survive it.
            await connection.commit()
            await connection.run_sync(_run_alembic_command, "upgrade", revision)
            if revision == "head":
                expected = (require_single_head(),)
                actual = await _current_database_heads(connection)
                if actual != expected:
                    raise RuntimeError(
                        f"Migration ended at {actual}, expected code head {expected}"
                    )
        finally:
            await _release_lock(connection)
    finally:
        await connection.close()
        await engine.dispose()


async def verify_database_revision() -> None:
    """Fail unless the target database revision exactly matches the code head."""
    _prepare_metadata()
    expected = (require_single_head(),)
    engine, connection = await _open_target_connection()
    try:
        tables = await _get_table_names(connection)
        if "alembic_version" not in tables:
            raise RuntimeError("Database is not managed by Alembic (alembic_version is missing)")
        actual = await _current_database_heads(connection)
        if actual != expected:
            raise RuntimeError(f"Database revision mismatch: database={actual}, code={expected}")
    finally:
        await connection.close()
        await engine.dispose()


def _schema_differences(connection: Connection) -> list[object]:
    migration_context = MigrationContext.configure(
        connection,
        opts={
            "compare_type": True,
            "compare_server_default": True,
            "target_metadata": SQLModel.metadata,
        },
    )
    return compare_metadata(migration_context, SQLModel.metadata)


async def stamp_existing_database(confirm: bool) -> None:
    """Stamp a matching legacy schema at baseline without executing baseline DDL."""
    if not confirm:
        raise RuntimeError("stamp-baseline requires --confirm after backup and schema review")

    _prepare_metadata()
    script = _script_directory()
    bases = tuple(script.get_bases())
    if len(bases) != 1:
        raise RuntimeError(f"Expected exactly one baseline revision, found: {bases}")
    baseline = bases[0]

    engine, connection = await _open_target_connection()
    try:
        await _acquire_lock(connection)
        try:
            tables = await _get_table_names(connection)
            if not tables:
                raise RuntimeError("Database is empty; run upgrade instead of stamp-baseline")
            if "alembic_version" in tables:
                raise RuntimeError("Database already contains alembic_version; refusing to restamp")

            differences = await connection.run_sync(_schema_differences)
            if differences:
                formatted = "\n".join(f"- {difference!r}" for difference in differences)
                raise RuntimeError(
                    "Existing database does not match SQLModel metadata; baseline stamp refused:\n"
                    f"{formatted}"
                )
            await connection.commit()
            await connection.run_sync(_run_alembic_command, "stamp", baseline)
            actual = await _current_database_heads(connection)
            if actual != (baseline,):
                raise RuntimeError(f"Baseline stamp ended at {actual}, expected {(baseline,)}")
        finally:
            await _release_lock(connection)
    finally:
        await connection.close()
        await engine.dispose()


async def test_upgrade_from_previous() -> None:
    """Build the immediate previous revision, then upgrade it to the current head."""
    _prepare_metadata()
    head = require_single_head()
    script = _script_directory()
    head_revision = script.get_revision(head)
    if head_revision is None:
        raise RuntimeError(f"Cannot resolve Alembic head {head}")

    previous: str
    if head_revision.down_revision is None:
        previous = "base"
    elif isinstance(head_revision.down_revision, str):
        previous = head_revision.down_revision
    else:
        raise RuntimeError(
            "The current head is a merge revision; add a dedicated previous-version migration test"
        )

    await _ensure_target_database_exists()
    engine, connection = await _open_target_connection()
    try:
        await _acquire_lock(connection)
        try:
            tables = await _get_table_names(connection)
            if tables:
                raise RuntimeError("Previous-version test database must start empty")
            await connection.commit()
            await connection.run_sync(_run_alembic_command, "upgrade", previous)
            await connection.run_sync(_run_alembic_command, "upgrade", "head")
            actual = await _current_database_heads(connection)
            if actual != (head,):
                raise RuntimeError(f"Previous-version upgrade ended at {actual}, expected {(head,)}")
        finally:
            await _release_lock(connection)
    finally:
        await connection.close()
        await engine.dispose()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kawaru database migration commands")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check-heads", help="require exactly one Alembic head")
    subparsers.add_parser("upgrade", help="upgrade an empty or versioned database to head")
    subparsers.add_parser("verify", help="verify that the database is at the code head")
    stamp_parser = subparsers.add_parser(
        "stamp-baseline",
        help="stamp a matching non-empty legacy database at the baseline revision",
    )
    stamp_parser.add_argument("--confirm", action="store_true")
    subparsers.add_parser(
        "test-upgrade-from-previous",
        help="build the previous revision and upgrade it to head",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    if args.command == "check-heads":
        head = require_single_head()
        print(f"Alembic head: {head}")
    elif args.command == "upgrade":
        asyncio.run(upgrade_database())
    elif args.command == "verify":
        asyncio.run(verify_database_revision())
    elif args.command == "stamp-baseline":
        asyncio.run(stamp_existing_database(args.confirm))
    elif args.command == "test-upgrade-from-previous":
        asyncio.run(test_upgrade_from_previous())


if __name__ == "__main__":
    main()
