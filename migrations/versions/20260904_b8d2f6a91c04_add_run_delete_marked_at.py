"""add experiment_process_run.delete_marked_at for deletion status bit

Revision ID: b8d2f6a91c04
Revises: c4e9a1f27b03
Create Date: 2026-09-04 12:00:00.000000

删除状态位改造（场景 2：删除算法/聚类 run）：
- ProcessingStatus 新增 DELETING / DELETE_FAILED 枚举值（status 为 str 列，无 DDL）
- experiment_process_run 新增 delete_marked_at，记录删除登记时间，
  retry_failed_deletions 兜底任务据此判定「停留过久的 DELETING」
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b8d2f6a91c04'
down_revision: str | Sequence[str] | None = 'c4e9a1f27b03'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('experiment_process_run', sa.Column('delete_marked_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('experiment_process_run', 'delete_marked_at')
