"""
原子数据库操作辅助模块

提供基于 SQLAlchemy update() 的原子计数器增减操作，
避免 ORM read-modify-write 模式在高并发下的丢失更新（Lost Update）问题。

使用方式：
    from database.atomic_ops import atomic_increment, atomic_decrement

    await atomic_increment(session, UserInDB, user.id, "file_count", 1)
    await atomic_decrement(session, UserInDB, user.id, "total_file_size", file_size)

设计原则：
    - 所有操作在调用方事务内执行，不自动 commit
    - 更新后调用方需要 refresh 以获取 ORM 对象的最新值
    - decrement 时自动使用 GREATEST 保护，值不会低于 0
"""

from __future__ import annotations

from typing import Any
from sqlmodel import update, func
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel.main import SQLModel


async def _atomic_update(
    session: AsyncSession,
    model: type[SQLModel],
    pk_column: str,
    pk_value: Any,
    field: str,
    expression: Any,
) -> None:
    """执行原子字段更新（内部函数），使用 session.exec 执行 UPDATE 语句"""
    pk_col = getattr(model, pk_column)
    stmt = (
        update(model)
        .where(pk_col == pk_value)
        .values(**{field: expression})
    )
    await session.exec(stmt)


async def atomic_increment(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    amount: int | float = 1,
    *,
    pk_column: str = "id",
) -> None:
    """
    原子地将指定记录的字段值增加 amount。

    等价 SQL：UPDATE <table> SET <field> = <field> + <amount> WHERE <pk_column> = <pk_value>

    Example:
        await atomic_increment(session, FileInDB, file_id, "download_count")
        await atomic_increment(session, UserInDB, user.id, "download_used", 5)
    """
    col = getattr(model, field)
    await _atomic_update(session, model, pk_column, pk_value, field, col + amount)


async def atomic_decrement(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    amount: int | float = 1,
    *,
    pk_column: str = "id",
) -> None:
    """
    原子地将指定记录的字段值减少 amount，结果不低于 0。

    等价 SQL：UPDATE <table> SET <field> = GREATEST(<field> - <amount>, 0) WHERE <pk_column> = <pk_value>
    """
    col = getattr(model, field)
    await _atomic_update(
        session, model, pk_column, pk_value, field,
        func.greatest(col - amount, 0),
    )


async def atomic_set(
    session: AsyncSession,
    model: type[SQLModel],
    pk_value: Any,
    field: str,
    value: Any,
    *,
    pk_column: str = "id",
) -> None:
    """
    原子地设置指定记录的字段值。

    等价 SQL：UPDATE <table> SET <field> = <value> WHERE <pk_column> = <pk_value>
    """
    await _atomic_update(session, model, pk_column, pk_value, field, value)
