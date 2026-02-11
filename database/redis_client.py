import os
from redis.asyncio.connection import ConnectionPool
from redis.asyncio import Redis
from envset.config import get_config
from logger import get_logger

logger = get_logger("main.redis")

redis_config = get_config().redis

# 连接池
pool = ConnectionPool(
    host=redis_config.host,
    port=redis_config.port,
    password=os.getenv('REDIS_PASSWORD'),
    db=0,
    decode_responses=True,
    max_connections=redis_config.max_connection # redis最大同时连接数，高并发状态下适当把值调高
)

# 建立redis连接
redis_client = Redis(connection_pool=pool)
