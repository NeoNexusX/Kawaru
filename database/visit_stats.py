"""
访问统计 - Redis 原子计数操作
"""
from fastapi import BackgroundTasks

from tools.logger import get_logger

from database.redis_client import redis_client
from constants.stats_constants import VISIT_STATS_KEY_PREFIX, VISIT_DAILY_TTL, VISIT_MONTHLY_TTL
from tools.time_tools import utc_time

logger = get_logger("database")


async def incr_visit() -> None:
    """原子递增全站访问计数（当日 + 当月 + 总计，共 3 次 INCR）

    路由不直接调用，统一通过 count_visit 依赖在响应发出后执行。
    计数属旁路统计——Redis 异常在此吞掉并记日志，绝不让业务接口失败，
    也不要靠它判断 Redis 健康状况。
    """
    try:
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
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("incr_visit failed: %s", exc)


async def count_visit(background_tasks: BackgroundTasks) -> None:
    """路由依赖：把一次访问计数登记为后台任务，响应发出后再写 Redis。

    用法：路由装饰器加 dependencies=[Depends(count_visit)]。
    - 不占用接口响应时间；任务随响应由 Starlette 持有并执行，不会被 GC 回收
    - 只统计成功请求：依赖或接口抛异常时返回的是错误响应，后台任务不会执行
    """
    background_tasks.add_task(incr_visit)


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
