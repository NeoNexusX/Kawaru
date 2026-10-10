import os
from redis.asyncio.connection import ConnectionPool
from redis.asyncio import Redis
from boot.config import get_config
from tools.logger import get_logger

logger = get_logger("database.redis")

redis_config = get_config().redis

# 从配置中读取 Redis 连接参数
REDIS_HOST = redis_config.host
REDIS_PORT = redis_config.port
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_USERNAME = redis_config.username or None
REDIS_DEFAULT_DB = redis_config.default_db_index

# 连接池（db 必须配在池上：传入 connection_pool 后，Redis() 上的其余连接参数会被 redis-py 忽略）
# 超时必配：默认 None 表示永不超时，Redis 半死不活（TCP 黑洞）时调用方会永久挂起，
# 而 redis_client 同时服务请求链路（缓存、访问计数），没有超时就没有兜底。
pool_default = ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    db=REDIS_DEFAULT_DB,
    decode_responses=True,
    max_connections=redis_config.max_connection,
    socket_timeout=2,
    socket_connect_timeout=2,
)

# 建立redis连接
redis_client = Redis(connection_pool=pool_default) # 默认redis数据库，用于一般用户数据存储

def connect_new_redis(db_index: int) -> Redis:
    """用于创建指定数据库的独立Redis连接，适用于需要独立连接的场景(如Celery任务队列)"""
    return Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USERNAME,
        password=REDIS_PASSWORD,
        db=db_index,
        decode_responses=True,
    )

async def clean_up_stale_packup_data():
    """清理 oss-packup 存储的打包任务数据，防止过期任务积累。"""
    _redis_client = None

    try:
        redis_cfg = get_config().redis
        _redis_client = connect_new_redis(db_index=redis_cfg.default_db_index)
        prefix = 'oss_packup'
        pattern = f"{prefix}*"
        delete_count = 0

        async for key in _redis_client.scan_iter(match=pattern):
            await _redis_client.delete(key)
            delete_count += 1
        logger.info("Cleaned up %d stale packup data", delete_count)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to clean up packup data: %s", exc)
    finally:
        if _redis_client is not None:
            try:
                await _redis_client.aclose()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to close Redis client after stale packup data cleanup: %s", exc)
