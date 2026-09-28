"""
访问统计 - Redis 原子计数操作
"""
import asyncio
from collections.abc import Coroutine
from typing import Any

from tools.logger import get_logger

from database.redis_client import redis_client
from constants.stats_constants import VISIT_STATS_KEY_PREFIX, VISIT_DAILY_TTL, VISIT_MONTHLY_TTL
from tools.time_tools import utc_time

logger = get_logger("database")

# 事件循环对 task 只持弱引用，官方文档要求调用方自行保存强引用，
# 否则未引用的 task 可能在执行中途被 GC 回收（计数静默丢失）
_pending_tasks: set[asyncio.Task] = set()


def fire_and_forget(coro: Coroutine[Any, Any, Any]) -> None:
    """
    发射后不管地执行协程：保存 task 强引用防 GC 回收，结束（含异常）后自动移除引用。

    用于访问计数这类允许失败、不阻塞响应的后台任务；异常在此统一记日志，
    避免 Redis 抖动时以 "Task exception was never retrieved" 的形式延迟出现。
    """
    task = asyncio.create_task(coro)
    _pending_tasks.add(task)
    task.add_done_callback(_discard_and_log)


def _discard_and_log(task: asyncio.Task) -> None:
    """done 回调：移除强引用；有异常则记录（cancelled 的 task 调 exception() 会抛错，先排除）"""
    _pending_tasks.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("background task failed: %s", exc, exc_info=exc)


async def incr_visit() -> None:
    """原子递增全站访问计数（当日 + 当月 + 总计，共 3 次 INCR）"""
    now = utc_time()
    daily_key = f"{VISIT_STATS_KEY_PREFIX}:all:daily:{now.strftime('%Y-%m-%d')}"
    monthly_key = f"{VISIT_STATS_KEY_PREFIX}:all:monthly:{now.strftime('%Y-%m')}"
    total_key = f"{VISIT_STATS_KEY_PREFIX}:all:total"

    pipe = redis_client.pipeline()
    pipe.incr(daily_key)
    pipe.expire(daily_key, VISIT_DAILY_TTL)
    pipe.incr(monthly_key)
    pipe.expire(monthly_key, VISIT_MONTHLY_TTL)
    pipe.incr(total_key)
    await pipe.execute()


async def get_visit_stats() -> dict[str, int]:
    """获取全站访问统计（当日、当月、总计）"""
    now = utc_time()
    daily_key = f"{VISIT_STATS_KEY_PREFIX}:all:daily:{now.strftime('%Y-%m-%d')}"
    monthly_key = f"{VISIT_STATS_KEY_PREFIX}:all:monthly:{now.strftime('%Y-%m')}"
    total_key = f"{VISIT_STATS_KEY_PREFIX}:all:total"

    daily_val, monthly_val, total_val = await redis_client.mget(
        daily_key, monthly_key, total_key
    )
    return {
        "daily": int(daily_val or 0),
        "monthly": int(monthly_val or 0),
        "total": int(total_val or 0),
    }
