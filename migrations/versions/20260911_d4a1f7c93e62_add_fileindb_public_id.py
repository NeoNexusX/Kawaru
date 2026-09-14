"""add fileindb public_id

Revision ID: d4a1f7c93e62
Revises: a1f4c7d92e36
Create Date: 2026-09-11 18:40:00.000000

文件公开分享标识（与 collection 的 public_id 同构）：
- fileindb 新增 public_id：base62 随机串 16 位（62^16 ≈ 2^95），唯一索引
- 用作分享页 URL 标识，替代可枚举的自增 file_id 对外暴露

注意（fileindb 为存量表，不能一步加 NOT NULL 列）：
- 随机值无法用 server_default 表达，故分三步：加可空列 → 逐行回填 → 收窄 NOT NULL
- 生成逻辑内联在迁移内，不 import tools.generate_public_id——迁移应自包含，
  否则将来生成逻辑演进会改变这条旧迁移重放的结果
- 回填使用 batch_alter_table 收窄列宽以兼容 SQLite（空库迁移测试）与 MariaDB

下载链路不变：仍按 file_id 调用 /files/{file_id}/download_raw_noauth，
public_id 仅用于分享页入口（详见 routers/file.py 的公开端路由）。
"""
from collections.abc import Sequence

import secrets

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'd4a1f7c93e62'
down_revision: str | Sequence[str] | None = 'a1f4c7d92e36'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# 与 tools/id_tools.py 一致的生成参数（此处内联，见模块 docstring）
PUBLIC_ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
PUBLIC_ID_LENGTH = 16


def _generate_public_id() -> str:
    """生成 16 位 base62 随机串（回填存量行用）"""
    return "".join(secrets.choice(PUBLIC_ID_ALPHABET) for _ in range(PUBLIC_ID_LENGTH))


def upgrade() -> None:
    """Upgrade schema: fileindb 新增 public_id 并回填存量行。"""
    # ① 先加可空列（存量表上 NOT NULL 且无默认值无法直接添加）
    op.add_column(
        'fileindb',
        sa.Column('public_id', sqlmodel.sql.sqltypes.AutoString(length=16), nullable=True),
    )

    # ② 逐行回填：随机串无法由 server_default 生成
    conn = op.get_bind()
    existing_ids = conn.execute(sa.text("SELECT file_id FROM fileindb")).fetchall()
    for (file_id,) in existing_ids:
        conn.execute(
            sa.text("UPDATE fileindb SET public_id = :public_id WHERE file_id = :file_id"),
            {"public_id": _generate_public_id(), "file_id": file_id},
        )

    # ③ 收窄为 NOT NULL（batch 兼容 SQLite 的表重建）
    with op.batch_alter_table('fileindb') as batch_op:
        batch_op.alter_column(
            'public_id',
            existing_type=sqlmodel.sql.sqltypes.AutoString(length=16),
            nullable=False,
        )

    # 唯一索引与模型 FileInDB.public_id 的 index=True/unique=True 对齐
    op.create_index(op.f('ix_fileindb_public_id'), 'fileindb', ['public_id'], unique=True)


def downgrade() -> None:
    """Downgrade schema: 移除 public_id 列。"""
    op.drop_index(op.f('ix_fileindb_public_id'), table_name='fileindb')
    with op.batch_alter_table('fileindb') as batch_op:
        batch_op.drop_column('public_id')
