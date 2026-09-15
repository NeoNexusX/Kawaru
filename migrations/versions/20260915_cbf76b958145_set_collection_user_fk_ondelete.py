"""set ondelete on collection foreign keys referencing userindb

Revision ID: cbf76b958145
Revises: a1f4c7d92e36
Create Date: 2026-09-15 12:00:00.000000

datacollection.owner_id、collectionmember.added_by 两个指向 userindb 的外键未设置 ondelete
（默认 RESTRICT），删除建过集合或给集合添加过成员的用户会因外键约束失败：
- datacollection.owner_id → ON DELETE CASCADE：用户的集合随用户删除（成员行随集合级联删除）
- collectionmember.added_by → ON DELETE SET NULL：保留成员关系，仅清空添加人

原外键建表时未命名：MariaDB 自动命名（如 datacollection_ibfk_1），SQLite 反射不出名字。
按反射结果定位原约束名，SQLite 的无名外键按 batch 模式的 naming_convention 推导名称；
重建后的外键显式命名，便于 downgrade 定位。纯约束变更，不涉及数据。
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cbf76b958145'
down_revision: str | Sequence[str] | None = 'a1f4c7d92e36'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# SQLite 反射出的外键没有名字，batch 模式按此约定命名后才能按名删除
_NAMING_CONVENTION = {"fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s"}

# (表, 外键列, upgrade 后的 ondelete, 重建后的约束名)
_USER_FOREIGN_KEYS = (
    ("datacollection", "owner_id", "CASCADE", "fk_datacollection_owner_id_userindb"),
    ("collectionmember", "added_by", "SET NULL", "fk_collectionmember_added_by_userindb"),
)


def _user_foreign_key_name(table: str, column: str) -> str:
    """定位 table.column -> userindb.id 外键的当前约束名"""
    for fk in sa.inspect(op.get_bind()).get_foreign_keys(table):
        if fk["constrained_columns"] == [column] and fk["referred_table"] == "userindb":
            return fk["name"] or _NAMING_CONVENTION["fk"] % {
                "table_name": table,
                "column_0_name": column,
                "referred_table_name": "userindb",
            }
    raise RuntimeError(f"foreign key {table}.{column} -> userindb not found")


def _replace_user_foreign_key(table: str, column: str, ondelete: str | None, new_name: str) -> None:
    old_name = _user_foreign_key_name(table, column)
    with op.batch_alter_table(table, naming_convention=_NAMING_CONVENTION) as batch_op:
        batch_op.drop_constraint(old_name, type_="foreignkey")
        batch_op.create_foreign_key(new_name, "userindb", [column], ["id"], ondelete=ondelete)


def upgrade() -> None:
    """Upgrade schema."""
    for table, column, ondelete, name in _USER_FOREIGN_KEYS:
        _replace_user_foreign_key(table, column, ondelete, name)


def downgrade() -> None:
    """Downgrade schema: 恢复为未设置 ondelete（RESTRICT）"""
    for table, column, _ondelete, name in _USER_FOREIGN_KEYS:
        _replace_user_foreign_key(table, column, None, name)
