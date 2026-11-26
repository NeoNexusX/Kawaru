"""userindb 处理配额改为次数（d3f8a2c61b47）的数据迁移测试。

迁移后：
- processed_file_size -> processing_count_used，按当前保留的
  “非直转且 output_size > 0”的 run 数回填；
- max_processing_size -> max_processing_count，统一为 50；
- 直转 run 不计；失败/处理中（无 output_size）不计；
  DELETING / DELETE_FAILED 但 output_size > 0 的 run 计入（finalize 时会退 1）。
"""
from alembic import command
from sqlalchemy import Connection, create_engine, inspect, text

from database.migration_config import get_alembic_config

PARENT_REVISION = "b6f2d81c4a97"
TARGET_REVISION = "d3f8a2c61b47"
RAW_CONVERT_MARKER = "__RAW_CONVERT__"


def _migrate(connection: Connection, action: str, revision: str) -> None:
    config = get_alembic_config()
    config.attributes["connection"] = connection
    getattr(command, action)(config, revision)


def _insert_user(connection: Connection, username: str, email: str) -> int:
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
    return connection.execute(
        text("SELECT id FROM userindb WHERE username = :username"),
        {"username": username},
    ).scalar_one()


def _insert_run(
    connection: Connection,
    user_id: int,
    *,
    params_json: str | None,
    status: str,
    output_size: int | None,
) -> None:
    """直插 run；experiment_id/source_file_id 用占位值（SQLite 测试默认不启用外键）"""
    connection.execute(
        text(
            "INSERT INTO experiment_process_run "
            "(params_json, status, experiment_id, source_file_id, user_id, created_at, output_size) "
            "VALUES (:params_json, :status, 1, 1, :user_id, :created_at, :output_size)"
        ),
        {
            "params_json": params_json,
            "status": status,
            "user_id": user_id,
            "created_at": "2026-01-01 00:00:00",
            "output_size": output_size,
        },
    )


def test_upgrade_backfills_processing_count_from_existing_runs() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        _migrate(connection, "upgrade", PARENT_REVISION)
        user_a = _insert_user(connection, "alice", "alice@example.com")
        user_b = _insert_user(connection, "bob", "bob@example.com")
        user_c = _insert_user(connection, "carol", "carol@example.com")

        # user_a：4 条计入（完成 2 条 + DELETING 1 条 + DELETE_FAILED 1 条）
        _insert_run(connection, user_a, params_json=None, status="completed", output_size=4096)
        _insert_run(connection, user_a, params_json='{"peak_pick":{}}', status="completed", output_size=2048)
        _insert_run(connection, user_a, params_json='{"normalization":{}}', status="deleting", output_size=1024)
        _insert_run(connection, user_a, params_json='{"noise_reduction":{}}', status="delete_failed", output_size=512)
        # 4 条不计：直转、失败、处理中、output_size 为 0
        _insert_run(connection, user_a, params_json=RAW_CONVERT_MARKER, status="completed", output_size=4096)
        _insert_run(connection, user_a, params_json=None, status="failed", output_size=None)
        _insert_run(connection, user_a, params_json=None, status="processing", output_size=None)
        _insert_run(connection, user_a, params_json=None, status="completed", output_size=0)
        # user_b 只有直转 run；user_c 没有 run
        _insert_run(connection, user_b, params_json=RAW_CONVERT_MARKER, status="completed", output_size=4096)

        _migrate(connection, "upgrade", TARGET_REVISION)

        rows = connection.execute(
            text("SELECT processing_count_used, max_processing_count FROM userindb ORDER BY id")
        ).all()
        assert [tuple(row) for row in rows] == [(4, 50), (0, 50), (0, 50)]

        # 降级：恢复旧列名；旧“已用大小”不可恢复，处理次数置 0
        _migrate(connection, "downgrade", PARENT_REVISION)
        columns = {column["name"] for column in inspect(connection).get_columns("userindb")}
        assert {"processed_file_size", "max_processing_size"} <= columns
        assert "processing_count_used" not in columns
        assert "max_processing_count" not in columns

        old_values = connection.execute(
            text("SELECT processed_file_size, max_processing_size FROM userindb ORDER BY id")
        ).all()
        assert [tuple(row) for row in old_values] == [(0, 50), (0, 50), (0, 50)]
