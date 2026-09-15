"""add datacollection.publish_time for article publish time

Revision ID: 9b1d4e6f8a2c
Revises: d4a1f7c93e62
Create Date: 2026-09-15 16:00:00.000000

集合学术信息新增「文章发表时间」：
- datacollection 新增 publish_time（DATETIME，可空），记录数据集对应文章的发表时间
- 存量集合无此信息，保持 NULL；纯展示字段，不参与筛选、不参与聚合
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '9b1d4e6f8a2c'
down_revision: str | Sequence[str] | None = 'd4a1f7c93e62'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('datacollection', sa.Column('publish_time', sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('datacollection', 'publish_time')
