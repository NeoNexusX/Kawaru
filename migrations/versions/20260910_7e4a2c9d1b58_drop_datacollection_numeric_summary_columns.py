"""drop datacollection numeric summary columns

Revision ID: 7e4a2c9d1b58
Revises: 26bd12b5522b
Create Date: 2026-09-10 12:00:00.000000

集合数值型仪器汇总列下线（需求确认集合层不展示数值汇总）：
- 删除 datacollection 的 pixel_size_horizontal / pixel_size_vertical / resolving_power / mz
- 文件级同名字段（fileindb）不受影响，需要汇总时由成员文件实时聚合
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7e4a2c9d1b58'
down_revision: str | Sequence[str] | None = '26bd12b5522b'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# 下线的数值型仪器汇总列（升级删列 / 降级按 JSON 空列表恢复）
DROPPED_COLUMNS = (
    'pixel_size_horizontal',
    'pixel_size_vertical',
    'resolving_power',
    'mz',
)


def upgrade() -> None:
    """Upgrade schema."""
    for column in DROPPED_COLUMNS:
        op.drop_column('datacollection', column)


def downgrade() -> None:
    """Downgrade schema."""
    # JSON 列 NOT NULL：回滚补列时给已有行一个空列表默认值
    for column in DROPPED_COLUMNS:
        op.add_column(
            'datacollection',
            sa.Column(column, sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        )
