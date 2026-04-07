# Celery app instance definition and task manager
import os
import asyncio
from logging import Logger
from celery import Celery
from celery import Task as CeleryTask
from celery.result import AsyncResult
from celery.schedules import schedule
from redbeat.schedulers import RedBeatSchedulerEntry as Entry
from models.config_models import Config


logger: "Logger | None" = None


# celery 任务涉及到的模块都要在这里导入
CELERY_MODULE_IMPORTS = [
    "background.file_background",
    "background.api_background",
    "background.file_background_helper",
]


# Task base that runs coroutine results in an event loop
class CeleryAsyncTask(CeleryTask): # pylint: disable=abstract-method
    """If task.run returns a coroutine, run it with asyncio.run and return result."""
    def __call__(self, *args, **kwargs):
        result = self.run(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result


def _get_logger():
    """对logger进行封装，避免logger还没初始化即调用"""
    global logger # pylint: disable=global-statement
    if logger is None:
        from tools.logger import get_logger  # pylint: disable=import-outside-toplevel
        logger = get_logger("background.taskmanager")
    return logger


def celery_create(
    cfg: Config,
    const_kwargs: dict,
    _logger: Logger | None = None,
) -> Celery:
    """Create a Celery app and return it."""
    if _logger is None:
        _logger = _get_logger()

    redis = cfg.redis
    password = os.getenv('REDIS_PASSWORD')
    username = redis.username or "default"

    auth = f"{username}:{password}@" if password else ""

    config_dict = {
        'broker_url': f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_broker_db_index}",
        'result_backend': f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_backend_db_index}",
        'redbeat_redis_url': f"redis://{auth}{redis.host}:{redis.port}/{redis.celery_redbeat_db_index}",
        'imports': CELERY_MODULE_IMPORTS,
        'beat_scheduler': "redbeat.schedulers.RedBeatScheduler",
        'timezone': 'UTC',
        'enable_utc': True,
        'worker_disable_rate_limits': cfg.celery.worker_disable_rate_limits,
        'beat_schedule': {
            "heartbeat": {
                "task": "background.file_background.heartbeat",
                "schedule": 10.0,
            },
            "cleanup_old_download_zips": {
                "task": "background.file_background.cleanup_old_download_zips",
                "schedule": const_kwargs['cleanup_download_zip_interval_seconds'],
                "args": (),
                "kwargs": {},
            },
            "cleanup_stale_preflights_all_users": {
                "task": "background.file_background.cleanup_stale_preflights_all_users",
                "schedule": const_kwargs['cleanup_stale_preflights_interval_seconds'],
                "args": (),
                "kwargs": {},
            },
        },
    }

    app = Celery('kawaru_celery')
    app.config_from_object(config_dict)
    app.Task = CeleryAsyncTask # Register as default Task base so @celery_app.task() uses it

    return app


# 如果子进程通过 env 指示需要在 import 时创建 celery app（用于 celery CLI 子进程）
def init_celery_in_subprocess() -> Celery | None:
    """
    本函数用于在启动celery worker时，
    在celery worker子进程内初始化celery配置，创建celery_app
    """
    # pylint:disable = C0415
    from boot.bootstrap import init_config
    from boot.bootstrap import init_logging
    from boot.bootstrap import load_constants
    from boot.config import get_config
    from tools.logger import get_logger
    # pylint:enable = C0415
    init_config()
    init_logging()
    _constants = load_constants()
    _logger = get_logger("bootstrap.celery_boot")
    _celery_app = None

    try:
        # 延迟导入 config，避免在主进程重复初始化冲突
        _celery_app = celery_create(
            get_config(),
            _constants,
            _logger=_logger,
        )
        _logger.info("Celery app created for worker subprocess")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # 不要抛异常阻止 celery CLI 启动，记录日志即可
        _logger.error("Failed to auto-create celery app on import: %s", exc)

    return _celery_app


if os.getenv("CREATE_CELERY_SUB"):
    celery_app  = init_celery_in_subprocess()
else:
    celery_app: "Celery | None" = None


class TaskManager:
    """Celery 任务管理器，提供动态添加/修改定时任务的接口"""
    @staticmethod
    async def add_interval_task(task_name: str,
                          task_path: str,
                          interval_seconds: int,
                          task_args: list | None = None,
                          task_kwargs: dict | None = None):
        """动态添加 Celery 定时间隔任务"""
        _logger = _get_logger()
        try:
            entry = Entry(
                name = task_name,
                task = task_path,
                schedule = schedule(run_every=interval_seconds),
                args = task_args or [],
                kwargs = task_kwargs or {},
                app = celery_app
            )
            _loop = asyncio.get_running_loop()
            await _loop.run_in_executor(None, entry.save)
            _logger.info(f"Added interval task '{task_name}' with path '{task_path}' to run every {interval_seconds} seconds.")
            return entry
        except Exception as exc: # pylint:disable = broad-exception-caught
            _logger.error(f"Cannot add interval task due to error: {exc}")
            return None

    @staticmethod
    async def remove_task(task_name: str):
        """动态移除 Celery 定时任务"""
        _logger = _get_logger()
        key = f"redbeat:{task_name}"
        _loop = asyncio.get_running_loop()

        try:
            def _load_entry() -> Entry | None:
                return Entry.from_key(key, app=celery_app)

            entry = await _loop.run_in_executor(None, _load_entry)

            if entry:
                await _loop.run_in_executor(None, entry.delete)
                _logger.info(f"Removed task '{task_name}' from schedule.")
                return True
        except KeyError:
            _logger.warning(f"Task '{task_name}' not found in schedule.")
        except Exception as exc: # pylint:disable = broad-exception-caught
            _logger.error(f"Cannot remove task due to error: {exc}")

        return False


async def wait_for_result(result: AsyncResult, poll_interval: float, timeout: float|None = 20): #TODO 压缩任务时间要求会长一点
    """异步轮询等待 Celery 结果

    :param result: Celery AsyncResult 对象
    :param poll_interval: 轮询间隔（秒）
    :param timeout: 最长等待时间（秒），为 None 时表示不限制
    """
    async def _poll_until_ready():
        while not result.ready():
            await asyncio.sleep(poll_interval)

        if result.failed():
            raise result.result

        return result.result

    if timeout is None:
        return await _poll_until_ready()
    try:
        return await asyncio.wait_for(_poll_until_ready(), timeout=timeout)
    except asyncio.TimeoutError:
        raise asyncio.TimeoutError("Celery task timeout") # pylint:disable = raise-missing-from
