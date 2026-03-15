## Celery 配置文件，通过 celery_app.config_from_object('boot.celeryconfig') 加载

# pylint: disable=invalid-name
import os
from boot.config import get_config
from boot.bootstrap import init_config, init_logging

# 由于celery为独立子进程，在celeryconfig阶段无法直接访问主进程的变量，因此需要在这里单独读取配置
init_config()  # 显式初始化配置
init_logging() # 显式初始化日志系统
_config = get_config()


redis_config = _config.redis
celery_config = _config.celery

# List of modules to import when celery starts.
imports = ["background.file_background"] # TODO: 可以考虑自动扫描 background 目录下的模块来动态生成这个列表，避免手动维护

# beat scheduler settings
beat_scheduler = "redbeat.schedulers.RedBeatScheduler"

# 从配置中读取 Redis 连接参数
REDIS_HOST = redis_config.host
REDIS_PORT = redis_config.port
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_USERNAME = redis_config.username or "default"
REDIS_DEFAULT_DB = redis_config.default_db_index
REDIS_BROKER_DB = redis_config.celery_broker_db_index
REDIS_BACKEND_DB = redis_config.celery_backend_db_index
REDIS_REDBEAT_DB = redis_config.celery_redbeat_db_index

# build auth part: include username if present
if REDIS_PASSWORD:
    if REDIS_USERNAME:
        auth = f"{REDIS_USERNAME}:{REDIS_PASSWORD}@"
    else:
        auth = f":{REDIS_PASSWORD}@"
else:
    auth = ""

# Broker and backend URLs (use redis scheme)
DEFAULT_REDIS_URL = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_DEFAULT_DB}"
BROKER_URL = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_BROKER_DB}"
RESULT_BACKEND = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_BACKEND_DB}"
REDBEAT_REDIS_URL = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_REDBEAT_DB}"

# Broker and backend URLs (use redis scheme)
broker_url = BROKER_URL
result_backend = RESULT_BACKEND
redbeat_redis_url = REDBEAT_REDIS_URL

## Worker settings
worker_disable_rate_limits = celery_config.worker_disable_rate_limits,
celery_worker_config = {
    ## If you’re doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    "worker_concurrency": celery_config.worker_concurrency,
    "worker_log_level": celery_config.worker_log_level,
    "worker_log_file": celery_config.worker_log_file,
}


## Celery Beat Settings (for periodic tasks)
time_zone = 'UTC'
enable_utc = True
beat_schedule = {
    "heartbeat": {
        "task": "background.file_background.heartbeat",
        "schedule": 10,  # 每10秒执行一次
    }
}
