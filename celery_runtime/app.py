import asyncio
import os
from logging import Logger

from celery import Celery
from celery import Task as CeleryTask

from models.config_models import Config


CELERY_MODULE_IMPORTS = [
    "background.file_background",
    "background.api_background",
    "background.file_background_helper",
    "background.experiment_background",
    "background.experiment_background_helper",
    "background.processing_background",
    "storage_backend.oss_log_helper",
]


class CeleryAsyncTask(CeleryTask):  # pylint: disable=abstract-method
    """Run coroutine task results inside an event loop."""

    def __call__(self, *args, **kwargs):
        result = self.run(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result


def build_redis_urls(cfg: Config) -> dict:
    """构造 broker/backend/redbeat 三个 Redis URL（worker 与 beat 共用）。"""
    redis = cfg.redis
    password = os.getenv("REDIS_PASSWORD")
    username = redis.username or "default"
    auth = f"{username}:{password}@" if password else ""
    return {
        "broker_url": f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_broker_db_index}",
        "result_backend": f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_backend_db_index}",
        "redbeat_redis_url": f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_redbeat_db_index}",
    }


def build_beat_schedule(const_kwargs: dict) -> dict:
    """构造 beat 定时任务表（worker summary 与 beat 入口共用，避免漂移）。"""
    return {
        "heartbeat": {
            "task": "background.file_background.heartbeat",
            "schedule": 67.0,
        },
        "cleanup_old_download_zips": {
            "task": "background.file_background.cleanup_old_download_zips",
            "schedule": const_kwargs["cleanup_download_zip_interval_seconds"],
            "args": (),
            "kwargs": {},
        },
        "cleanup_stale_preflights_all_users": {
            "task": "background.file_background.cleanup_stale_preflights_all_users",
            "schedule": const_kwargs["cleanup_stale_preflights_interval_seconds"],
            "args": (),
            "kwargs": {},
        },
        "cleanup_failed_experiments": {
            "task": "background.file_background.cleanup_failed_experiments",
            "schedule": const_kwargs["cleanup_failed_experiments_interval_seconds"],
            "args": (),
            "kwargs": {},
        },
    }


def create_celery_app(
    cfg: Config,
    const_kwargs: dict,
    _logger: Logger | None = None,
) -> Celery:
    """Create a Celery app without triggering bootstrap side effects."""
    if _logger is None:
        from tools.logger import get_logger  # pylint: disable=import-outside-toplevel

        _logger = get_logger("background.taskmanager")

    config_dict = {
        **build_redis_urls(cfg),
        "imports": CELERY_MODULE_IMPORTS,
        "beat_scheduler": "redbeat.schedulers.RedBeatScheduler",
        "worker_pool_restarts": False,
        "task_async_backend": "asyncio",
        "task_default_queue": "default",
        "task_routes": {
        },
        "timezone": "UTC",
        "enable_utc": True,
        "worker_disable_rate_limits": cfg.celery.worker_disable_rate_limits,
        "beat_schedule": build_beat_schedule(const_kwargs),
    }

    app = Celery("kawaru_celery")
    app.config_from_object(config_dict)
    app.Task = CeleryAsyncTask
    return app
