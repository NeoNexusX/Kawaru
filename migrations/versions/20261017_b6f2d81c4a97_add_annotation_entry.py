"""add annotation_entry table

Revision ID: b6f2d81c4a97
Revises: f3a9c71e5b28
Create Date: 2026-10-17 12:00:00.000000

SI 注释数据库（单表）：
- 只建表，不播种数据；CSV 由 tools/annotation_import.py 导入（表结构走迁移，数据走工具）
- mass 用 DOUBLE：MySQL 的 FLOAT 为单精度，会丢掉源 CSV 的 10 位小数
- 本表只存"当前那份 CSV"的内容，不含版本/路径信息（路径在 config.yaml 的 oss_si_db_key）
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b6f2d81c4a97'
down_revision: str | Sequence[str] | None = 'f3a9c71e5b28'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'annotation_entry',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('entry_key', sa.String(length=16), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('formula', sa.String(length=16), nullable=False),
        sa.Column('mass', sa.Double(), nullable=False),
        sa.Column('origin', sa.String(length=16), nullable=False),
        sa.Column('inchi', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_annotation_entry_entry_key'), 'annotation_entry', ['entry_key'], unique=True)
    op.create_index(op.f('ix_annotation_entry_name'), 'annotation_entry', ['name'], unique=False)
    op.create_index(op.f('ix_annotation_entry_mass'), 'annotation_entry', ['mass'], unique=False)
    op.create_index(op.f('ix_annotation_entry_origin'), 'annotation_entry', ['origin'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_annotation_entry_origin'), table_name='annotation_entry')
    op.drop_index(op.f('ix_annotation_entry_mass'), table_name='annotation_entry')
    op.drop_index(op.f('ix_annotation_entry_name'), table_name='annotation_entry')
    op.drop_index(op.f('ix_annotation_entry_entry_key'), table_name='annotation_entry')
    op.drop_table('annotation_entry')
