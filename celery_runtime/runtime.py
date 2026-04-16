from celery import Celery

from boot.bootstrap import init_config, init_env, init_logging, load_constants
from boot.config import get_config
from celery_runtime.app import create_celery_app
from celery_runtime.state import set_celery_app
from tools.logger import get_logger


def build_celery_runtime() -> Celery | None:
    """初始化并返回 Celery app，供 worker/beat 独立入口使用。"""
    init_env()
    init_config()
    init_logging()
    constants = load_constants()
    logger = get_logger("celery_runtime")

    try:
        app = create_celery_app(get_config(), constants, _logger=logger)
        set_celery_app(app)
        logger.info("Celery runtime initialized")
        return app
    except Exception as exc:  # pylint: disable=broad-exception-caught
        set_celery_app(None)
        logger.error("Failed to initialize celery runtime: %s", exc)
        return None


def celery_summary(app: Celery, config):
    """生成 Celery 配置摘要字符串。"""

    if config.celery.enable_embedded_celery:
        broker = app.conf.get('broker_url', 'N/A')
        backend = app.conf.get('result_backend', 'N/A')
        redbeat_backend = app.conf.get('redbeat_redis_url', 'N/A')
        timezone = app.conf.get('timezone', 'UTC')

        tasks = list(app.tasks.keys())
        user_tasks = [t for t in tasks if not t.startswith('celery.')]
        beat_schedule = app.conf.get('beat_schedule', {})

        lines = [
            "=" * 50,
            "Celery Configuration Summary",
            "-" * 50,
            f"Broker:          {broker}",
            f"Backend:         {backend}",
            f"Redbeat Backend: {redbeat_backend}",
            f"Timezone:        {timezone}",
            f"Tasks:           {len(user_tasks)} registered",
        ]
        for task in sorted(user_tasks):
            lines.append(f"                 - {task}")

        if beat_schedule:
            lines.append(f"Beat Jobs:       {len(beat_schedule)} scheduled")
            for name, conf in beat_schedule.items():
                schedule = conf.get('schedule', 'N/A')
                task_name = conf.get('task', 'N/A')
                lines.append(f"                 - {name}: {task_name} ({schedule})")
        else:
            lines.append("Beat Jobs:       None")

        lines.append("=" * 50)
        get_logger('bootstrap').info("\n".join(lines))
    else:
        get_logger('bootstrap').info("Embedded Celery is disabled, skipping Celery summary.")


async def clean_up_stale_task():
    """清理 RedBeat 存储的定时任务数据，防止过期任务积累。"""
    from database.redis_client import connect_new_redis  # pylint: disable=import-outside-toplevel

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
