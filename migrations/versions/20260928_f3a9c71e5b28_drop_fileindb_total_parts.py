"""drop fileindb total_parts column

Revision ID: f3a9c71e5b28
Revises: 9b1d4e6f8a2c
Create Date: 2026-09-28 12:00:00.000000

分片上传早已下线，total_parts 属于兼容遗留：
- 请求模型 FileCreate 与表模型 File 的同名字段已从代码中移除
- 本迁移删除数据库残留列（可空，无业务读取）；降级按可空列恢复
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f3a9c71e5b28'
down_revision: str | Sequence[str] | None = '9b1d4e6f8a2c'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_column('fileindb', 'total_parts')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        'fileindb',
        sa.Column('total_parts', sa.Integer(), nullable=True),
    )
