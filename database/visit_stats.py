"""
访问统计 - Redis 原子计数操作
"""
from database.redis_client import redis_client
from constants.stats_constants import VISIT_STATS_KEY_PREFIX, VISIT_DAILY_TTL, VISIT_MONTHLY_TTL
from tools.time_tools import cst_time


async def incr_visit() -> None:
    """原子递增全站访问计数（当日 + 当月 + 总计，共 3 次 INCR）"""
    now = cst_time()
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
    now = cst_time()
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
