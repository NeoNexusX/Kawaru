"""shorten datacollection public_id

Revision ID: a1f4c7d92e36
Revises: 7e4a2c9d1b58
Create Date: 2026-09-10 17:05:00.000000

public_id 由 uuid4 hex（32 位）改为 base62 随机串（16 位），公开页 URL 减半：
- 生成逻辑改为 tools.generate_public_id（62^16 ≈ 2^95，防枚举余量充足）
- 列收窄至 VARCHAR(16)：收窄要求存量行长度均 ≤16，否则 MariaDB 报错

注意（升级前人工确认）：
- datacollection 中长度 >16 的历史行（旧 uuid4 hex）无法容纳宽 16 的列，
  升级前需先清理这些行，或先手工回填为 16 位新 id
- 用 batch_alter_table 以兼容 SQLite（空库迁移测试）与 MariaDB（生产为普通 ALTER）
"""
from collections.abc import Sequence

from alembic import op
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'a1f4c7d92e36'
down_revision: str | Sequence[str] | None = '7e4a2c9d1b58'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema: public_id uuid4 hex(32) -> base62(16)"""
    with op.batch_alter_table('datacollection') as batch_op:
        batch_op.alter_column(
            'public_id',
            existing_type=sqlmodel.sql.sqltypes.AutoString(length=32),
            type_=sqlmodel.sql.sqltypes.AutoString(length=16),
            existing_nullable=False,
        )


def downgrade() -> None:
    """Downgrade schema: 放宽回 uuid4 hex(32)，已有 16 位值不受影响"""
    with op.batch_alter_table('datacollection') as batch_op:
        batch_op.alter_column(
            'public_id',
            existing_type=sqlmodel.sql.sqltypes.AutoString(length=16),
            type_=sqlmodel.sql.sqltypes.AutoString(length=32),
            existing_nullable=False,
        )
