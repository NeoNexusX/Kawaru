"""add unique constraint on userindb.username

Revision ID: 3840a26d8fa5
Revises: cbf76b958145
Create Date: 2026-09-15 12:30:00.000000

userindb.username 原先没有唯一约束，而登录与 JWT sub 都按用户名定位用户（get_user 取 first）：
并发注册出同名用户后，token 可能解析到另一个账号。改为由数据库唯一约束兜底。

注意（升级前人工确认）：
- 存量数据存在重复用户名时拒绝升级并列出重复项，需人工改名/合并后重试（无法自动判断保留哪个账号）
- 重复判断沿用列的排序规则（MariaDB 默认 utf8mb4_unicode_ci，大小写不敏感），与唯一约束口径一致
- 用 batch_alter_table 兼容 SQLite（空库迁移测试）与 MariaDB（生产为普通 ALTER）
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3840a26d8fa5'
down_revision: str | Sequence[str] | None = 'cbf76b958145'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT_NAME = "uq_userindb_username"


def upgrade() -> None:
    """Upgrade schema."""
    duplicates = op.get_bind().execute(sa.text(
        "SELECT username, COUNT(*) FROM userindb GROUP BY username HAVING COUNT(*) > 1"
    )).fetchall()
    if duplicates:
        listed = ", ".join(f"{username!r}×{count}" for username, count in duplicates[:20])
        raise RuntimeError(
            f"userindb has {len(duplicates)} duplicated username(s): {listed}. "
            "Rename or merge these accounts before adding the unique constraint."
        )

    with op.batch_alter_table("userindb") as batch_op:
        batch_op.create_unique_constraint(CONSTRAINT_NAME, ["username"])


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("userindb") as batch_op:
        batch_op.drop_constraint(CONSTRAINT_NAME, type_="unique")
