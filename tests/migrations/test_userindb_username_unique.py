"""userindb.username 唯一约束迁移（3840a26d8fa5）的数据测试。

存量数据有重复用户名时必须拒绝升级且不改动结构；无重复时加上约束、保留原有数据，并可正常回滚。
"""
import pytest
from alembic import command
from sqlalchemy import Connection, create_engine, inspect, text
from sqlalchemy.exc import IntegrityError

from database.migration_config import get_alembic_config

PARENT_REVISION = "cbf76b958145"
TARGET_REVISION = "3840a26d8fa5"


def _migrate(connection: Connection, action: str, revision: str) -> None:
    config = get_alembic_config()
    config.attributes["connection"] = connection
    getattr(command, action)(config, revision)


def _insert_user(connection: Connection, username: str, email: str) -> None:
    """按当前表结构插入最小可用的用户行：非空列按类型填占位值"""
    values: dict[str, object] = {"username": username, "email": email}
    for column in inspect(connection).get_columns("userindb"):
        if column["name"] in values or column["name"] == "id" or column["nullable"]:
            continue
        python_type = column["type"].python_type
        values[column["name"]] = True if python_type is bool else 0 if python_type is int else "x"
    connection.execute(
        text(f"INSERT INTO userindb ({', '.join(values)}) VALUES ({', '.join(f':{name}' for name in values)})"),
        values,
    )


def _has_username_unique(connection: Connection) -> bool:
    return ["username"] in [uc["column_names"] for uc in inspect(connection).get_unique_constraints("userindb")]


def _current_revision(connection: Connection) -> str:
    return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()


def test_upgrade_refuses_duplicated_usernames() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        _migrate(connection, "upgrade", PARENT_REVISION)
        _insert_user(connection, "alice", "alice1@example.com")
        _insert_user(connection, "alice", "alice2@example.com")

        with pytest.raises(RuntimeError, match="duplicated username"):
            _migrate(connection, "upgrade", TARGET_REVISION)

        assert _current_revision(connection) == PARENT_REVISION
        assert not _has_username_unique(connection)
        assert connection.execute(text("SELECT COUNT(*) FROM userindb")).scalar_one() == 2


def test_upgrade_adds_constraint_and_keeps_existing_users() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        _migrate(connection, "upgrade", PARENT_REVISION)
        _insert_user(connection, "alice", "alice@example.com")
        _insert_user(connection, "bob", "bob@example.com")

        _migrate(connection, "upgrade", TARGET_REVISION)
        assert _current_revision(connection) == TARGET_REVISION
        assert _has_username_unique(connection)
        usernames = connection.execute(text("SELECT username FROM userindb ORDER BY username")).scalars().all()
        assert usernames == ["alice", "bob"]

        _migrate(connection, "downgrade", PARENT_REVISION)
        assert not _has_username_unique(connection)

        _migrate(connection, "upgrade", TARGET_REVISION)
        with pytest.raises(IntegrityError):
            _insert_user(connection, "alice", "alice.other@example.com")
