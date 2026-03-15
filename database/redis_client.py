import os
from redis.asyncio.connection import ConnectionPool
from redis.asyncio import Redis
from boot.config import get_config
from tools.logger import get_logger

logger = get_logger("main.redis")

redis_config = get_config().redis

# 从配置中读取 Redis 连接参数
REDIS_HOST = redis_config.host
REDIS_PORT = redis_config.port
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_USERNAME = redis_config.username or None
REDIS_DEFAULT_DB = redis_config.default_db_index

# 连接池
pool_default = ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    decode_responses=True,
    max_connections=redis_config.max_connection,
)

# 建立redis连接
redis_client = Redis(connection_pool=pool_default, db=REDIS_DEFAULT_DB) # 默认redis数据库，用于一般用户数据存储

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