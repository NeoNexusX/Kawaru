# pylint: disable=invalid-name
import os
from boot.config import get_config

redis_config = get_config().redis
celery_config = get_config().celery

# List of modules to import when celery starts.
imports = celery_config.imports
## Result store settings.
REDIS_HOST = redis_config.host
REDIS_PORT = redis_config.port
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_USERNAME = redis_config.username or "default"
REDIS_BROKER_DB = redis_config.celery_broker_db_index
REDIS_BACKEND_DB = redis_config.celery_backend_db_index
REDIS_CONNECT_RETRY = True

# build auth part: include username if present
if REDIS_PASSWORD:
    if REDIS_USERNAME:
        auth = f"{REDIS_USERNAME}:{REDIS_PASSWORD}@"
    else:
        auth = f":{REDIS_PASSWORD}@"
else:
    auth = ""

# Broker and backend URLs (use redis scheme)
broker_url = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_BROKER_DB}"
result_backend = f"redis://{auth}{REDIS_HOST}:{REDIS_PORT}/{REDIS_BACKEND_DB}"

## Worker settings
## If you’re doing mostly I/O you can have more processes,
## but if mostly spending CPU, try to keep it close to the
## number of CPUs on your machine. If not set, the number of CPUs/cores
## available will be used.
worker_disable_rate_limits = celery_config.worker_disable_rate_limits,
celery_worker_config = {
    "worker_concurrency": celery_config.worker_concurrency,
    "worker_log_level": celery_config.worker_log_level,
    "worker_log_file": celery_config.worker_log_file,
}
