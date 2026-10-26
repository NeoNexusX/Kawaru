"""datacollection.member_type / collection_type 转 JSON 列表迁移（3ee8d4008a21）的数据测试。

存量字符串需回填为列表（空串 → []，'MSI' → ['MSI']），列类型变为 JSON；回滚后恢复为字符串。
"""
import json

from alembic import command
from sqlalchemy import Connection, create_engine, inspect, text

from database.migration_config import get_alembic_config

PARENT_REVISION = "3840a26d8fa5"
TARGET_REVISION = "3ee8d4008a21"


def _migrate(connection: Connection, action: str, revision: str) -> None:
    config = get_alembic_config()
    config.attributes["connection"] = connection
    getattr(command, action)(config, revision)


def _placeholder(column: dict) -> object:
    python_type = column["type"].python_type
    if python_type is bool:
        return True
    if python_type is int:
        return 0
    if python_type in (list, dict):
        return "[]"
    return "x"


def _insert_row(connection: Connection, table: str, values: dict[str, object]) -> None:
    """按当前表结构插入最小可用的行：未给出的非空列按类型填占位值"""
    values = dict(values)
    for column in inspect(connection).get_columns(table):
        if column["name"] in values or column["name"] == "id" or column["nullable"]:
            continue
        values[column["name"]] = _placeholder(column)
    connection.execute(
        text(f"INSERT INTO {table} ({', '.join(values)}) VALUES ({', '.join(f':{name}' for name in values)})"),
        values,
    )


def _type_values(connection: Connection) -> list[tuple[object, object]]:
    return [
        tuple(row)
        for row in connection.execute(
            text("SELECT member_type, collection_type FROM datacollection ORDER BY id")
        ).all()
    ]


def _column_type(connection: Connection, column: str) -> str:
    for info in inspect(connection).get_columns("datacollection"):
        if info["name"] == column:
            return str(info["type"]).upper()
    raise AssertionError(f"column {column} not found")


def test_upgrade_backfills_lists_and_downgrade_restores_strings() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        _migrate(connection, "upgrade", PARENT_REVISION)
        _insert_row(connection, "userindb", {"username": "alice", "email": "alice@example.com"})
        owner_id = connection.execute(text("SELECT id FROM userindb")).scalar_one()
        _insert_row(
            connection,
            "datacollection",
            {"owner_id": owner_id, "public_id": "p1", "name": "a", "member_type": "MSI", "collection_type": " series "},
        )
        _insert_row(
            connection,
            "datacollection",
            {"owner_id": owner_id, "public_id": "p2", "name": "b", "member_type": "", "collection_type": ""},
        )

        _migrate(connection, "upgrade", TARGET_REVISION)
        assert _column_type(connection, "member_type") == "JSON"
        assert _column_type(connection, "collection_type") == "JSON"
        decoded = [tuple(json.loads(v) for v in row) for row in _type_values(connection)]
        assert decoded == [(["MSI"], ["series"]), ([], [])]

        connection.execute(
            text("UPDATE datacollection SET member_type = :value WHERE public_id = 'p2'"),
            {"value": json.dumps(["MSI", "H&E", "ST"])},
        )
        _migrate(connection, "downgrade", PARENT_REVISION)
        assert "VARCHAR" in _column_type(connection, "member_type")
        assert _type_values(connection) == [("MSI", "series"), ("MSI, H&E, ST", "")]
