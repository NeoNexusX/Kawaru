import os
from logging import Logger
from celery import Celery

from boot.bootstrap import init_config, init_logging, load_constants
from boot.config import get_config
from background.celery_app import celery_create
from database.redis_client import connect_new_redis
from tools.logger import get_logger

RUNTIME_ENV_VAR = "CELERY_RUNTIME_ENABLED"


def is_celery_runtime_enabled() -> bool:
    """是否启用独立 Celery runtime 初始化。"""
    return os.getenv(RUNTIME_ENV_VAR, "").lower() in {"1", "true", "yes", "on"}


def build_celery_runtime() -> Celery | None:
    """初始化并返回 Celery app，供 worker/beat 独立入口使用。"""
    init_config()
    init_logging()
    constants = load_constants()
    logger = get_logger("celery_runtime")

    try:
        app = celery_create(get_config(), constants, _logger=logger)
        logger.info("Celery runtime initialized")
        return app
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to initialize celery runtime: %s", exc)
        return None


def celery_summary(app: Celery, logger: Logger):
    """输出 Celery 配置摘要。"""
    broker = app.conf.get('broker_url', 'N/A')
    backend = app.conf.get('result_backend', 'N/A')
    redbeat_backend = app.conf.get('redbeat_redis_url', 'N/A')
    timezone = app.conf.get('timezone', 'UTC')

    tasks = list(app.tasks.keys())
    user_tasks = [t for t in tasks if not t.startswith('celery.')]
    beat_schedule = app.conf.get('beat_schedule', {})

    logger.info("=" * 50)
    logger.info("Celery Configuration Summary")
    logger.info("-" * 50)
    logger.info("Broker:     %s", broker)
    logger.info("Backend:    %s", backend)
    logger.info("Redbeat Backend: %s", redbeat_backend)
    logger.info("Timezone:   %s", timezone)
    logger.info("Tasks:      %s registered", len(user_tasks))
    for task in sorted(user_tasks):
        logger.info("            - %s", task)

    if beat_schedule:
        logger.info("Beat Jobs:  %s scheduled", len(beat_schedule))
        for name, conf in beat_schedule.items():
            schedule = conf.get('schedule', 'N/A')
            task_name = conf.get('task', 'N/A')
            logger.info("            - %s: %s (%s)", name, task_name, schedule)
    else:
        logger.info("Beat Jobs:  None")

    logger.info("=" * 50)


async def clean_up_stale_task():
    """清理 RedBeat 存储的定时任务数据，防止过期任务积累。"""
    logger = get_logger("celery_runtime.redbeat_cleanup")
    redis_client = None

    try:
        redis_cfg = get_config().redis
        redis_client = connect_new_redis(db_index=redis_cfg.celery_redbeat_db_index)
        prefix = 'redbeat:'
        pattern = f"{prefix}*"
        delete_count = 0

        async for key in redis_client.scan_iter(match=pattern):
            await redis_client.delete(key)
            delete_count += 1
        logger.info("Cleaned up %d stale RedBeat scheduler entries", delete_count)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to clean up RedBeat storage: %s", exc)
    finally:
        if redis_client is not None:
            try:
                await redis_client.aclose()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to close Redis client after stale task cleanup: %s", exc)
