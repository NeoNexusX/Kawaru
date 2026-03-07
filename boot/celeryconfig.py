import os
from boot.config import get_config

redis_config = get_config().redis

# List of modules to import when celery starts.
CELERY_IMPORTS = ("tasks.user_tasks", )
## Result store settings.
CELERY_RESULT_BACKEND = "redis"
REDIS_HOST = redis_config.host
REDIS_PORT = redis_config.port
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_DB = redis_config.celery_backend_db_index
REDIS_CONNECT_RETRY = True

## Broker settings.
BROKER_HOST = redis_config.host
BROKER_PORT = redis_config.port
BROKER_VHOST = "/"
BROKER_USER = redis_config.username
BROKER_PASSWORD = os.getenv('REDIS_PASSWORD')

## Worker settings
## If you’re doing mostly I/O you can have more processes,
## but if mostly spending CPU, try to keep it close to the
## number of CPUs on your machine. If not set, the number of CPUs/cores
## available will be used.
CELERYD_CONCURRENCY = 10
CELERY_DISABLE_RATE_LIMITS = True
CELERYD_LOG_LEVEL = "INFO"
# CELERYD_LOG_FILE = "celeryd.log"