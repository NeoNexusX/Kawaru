"""switch processing quota from output size to run count

Revision ID: d3f8a2c61b47
Revises: b6f2d81c4a97
Create Date: 2026-11-25 12:00:00.000000

处理配额由“成功输出的累计大小（字节）”改为“当前保留的非直转已完成 run 数”：

- processed_file_size -> processing_count_used
- max_processing_size -> max_processing_count
- 已用次数按存量 run 回填，谓词与运行时退还逻辑保持一致：
  output_size > 0 且 params_json 不是直转标记。
  用 output_size > 0 而不是 status='completed'：删除流程会把状态改成
  DELETING / DELETE_FAILED，但这些 run 仍然占用次数，finalize 时要退 1。
- 限额统一为 50 次（选 A：严格 50 次总量）。
- 直转 run 虽然也有 output_size，但不占处理次数，迁移和退还都排除 marker。

注意（升级前人工确认）：
- 旧口径的“已用大小”不参与换算，直接丢弃；旧 run 不补记大小。
- 迁移会覆盖所有用户的 max_processing_size 为 50 次。
- 建议停服/排空回调后执行，避免回填期间有 run 完成导致计数漂移；
  部署完成后可重跑 processing_count_used 的回填 UPDATE 做校准。
- 用 batch_alter_table 兼容 SQLite（空库迁移测试）与 MariaDB（生产为普通 ALTER）
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3f8a2c61b47'
down_revision: str | Sequence[str] | None = 'b6f2d81c4a97'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# 与 models.processing_models.RAW_CONVERT_MARKER 保持一致；迁移脚本保持自包含
_RAW_CONVERT_MARKER = "__RAW_CONVERT__"


def upgrade() -> None:
    """Upgrade schema: size-based quota -> run-count quota."""
    with op.batch_alter_table("userindb") as batch_op:
        batch_op.alter_column(
            "processed_file_size",
            new_column_name="processing_count_used",
            existing_type=sa.BigInteger(),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "max_processing_size",
            new_column_name="max_processing_count",
            existing_type=sa.BigInteger(),
            existing_nullable=False,
        )

    # 选 A：所有用户统一为 50 次总量
    op.execute("UPDATE userindb SET max_processing_count = 50")

    # 已用次数 = 当前保留的“非直转且 output_size > 0”的 run 数。
    # 与 api_helpers.processing_helper.run_counts_toward_quota 口径一致。
    op.execute(
        sa.text(
            """
            UPDATE userindb
            SET processing_count_used = (
                SELECT COUNT(*)
                FROM experiment_process_run r
                WHERE r.user_id = userindb.id
                  AND r.output_size > 0
                  AND (r.params_json IS NULL OR r.params_json <> :raw_convert_marker)
            )
            """
        ).bindparams(raw_convert_marker=_RAW_CONVERT_MARKER)
    )


def downgrade() -> None:
    """Downgrade schema: restore old column names.

    旧口径的“已用大小”已在 upgrade 丢弃，无法恢复；这里把次数置 0，
    避免旧代码把次数当成字节使用。max_processing_count 的 50 会原样
    写回 max_processing_size（旧代码语义为 GB）。
    """
    op.execute("UPDATE userindb SET processing_count_used = 0")

    with op.batch_alter_table("userindb") as batch_op:
        batch_op.alter_column(
            "processing_count_used",
            new_column_name="processed_file_size",
            existing_type=sa.BigInteger(),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "max_processing_count",
            new_column_name="max_processing_size",
            existing_type=sa.BigInteger(),
            existing_nullable=False,
        )
