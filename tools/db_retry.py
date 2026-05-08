"""数据库死锁/锁等待超时重试工具"""
import asyncio
from typing import Awaitable, Callable, TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError
from sqlmodel.ext.asyncio.session import AsyncSession

from .logger import get_logger


T = TypeVar("T")

_DEADLOCK_HINTS = ("deadlock", "lock wait timeout")


def _is_deadlock_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(hint in msg for hint in _DEADLOCK_HINTS)


async def retry_on_deadlock(
    operation: Callable[[], Awaitable[T]],
    async_session: AsyncSession,
    *,
    retries: int = 3,
    base_delay: float = 0.05,
) -> T:
    """
    在 MySQL 死锁或锁等待超时发生时重试一个 async 操作。

    operation 必须是无参可重复调用的工厂函数，每次调用返回一个新的 coroutine
    （coroutine 只能被 await 一次，所以不能直接传 coroutine 实例）。
    重试前会自动 rollback session，避免事务残留。

    用法:
        async def _do_work() -> FilePublic:
            ... # 包含 commit
        result = await retry_on_deadlock(_do_work, async_session)
    """
    logger = get_logger("tools.db_retry")

    last_exc: BaseException | None = None
    for attempt in range(retries):
        try:
            return await operation()
        except (OperationalError, DBAPIError) as e:
            last_exc = e
            if _is_deadlock_error(e) and attempt < retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "deadlock detected, retrying attempt=%s/%s delay=%.3fs error=%s",
                    attempt + 1, retries, delay, e,
                )
                await async_session.rollback()
                await asyncio.sleep(delay)
                continue
            raise
    assert last_exc is not None
    raise last_exc
