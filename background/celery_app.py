# Celery app instance definition and task manager
import asyncio
from celery import Celery
from celery import Task as CeleryTask
from celery.schedules import schedule
from redbeat.schedulers import RedBeatSchedulerEntry as Entry

from boot.bootstrap import init_logging
from tools.logger import get_logger
init_logging() # ensure logging system initialized before creating Celery instance
logger = get_logger("background.taskmanager")


# create Celery app instance and load config from celeryconfig.py
celery_app = Celery('kawaru_celery')
celery_app.config_from_object('background.celeryconfig')



# Task base that runs coroutine results in an event loop
class AsyncTask(CeleryTask): # pylint: disable=abstract-method
    """If task.run returns a coroutine, run it with asyncio.run and return result."""
    def __call__(self, *args, **kwargs):
        result = self.run(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result


# Register as default Task base so @celery_app.task() uses it
celery_app.Task = AsyncTask


class TaskManager:
    """Celery 任务管理器，提供动态添加/修改定时任务的接口"""
    # TODO：目前为同步版本任务管理器，redis服务器本身延迟会影响redbeat entry执行效率，后续考虑封装成异步操作
    @staticmethod
    def add_interval_task(task_name: str,
                          task_path: str,
                          interval_seconds: int,
                          task_args: list | None = None,
                          task_kwargs: dict | None = None):
        """动态添加 Celery 定时间隔任务"""
        entry = Entry(
            name = task_name,
            task = task_path,
            schedule = schedule(run_every=interval_seconds),
            args = task_args or [],
            kwargs = task_kwargs or {},
            app = celery_app
        )
        entry.save()
        logger.info(f"Added interval task '{task_name}' with path '{task_path}' to run every {interval_seconds} seconds.")
        return entry

    @staticmethod
    def remove_task(task_name: str):
        """动态移除 Celery 定时任务"""
        key = f"redbeat:{task_name}"
        try:
            entry = Entry.from_key(key, app=celery_app)
            if entry:
                entry.delete()
                logger.info(f"Removed task '{task_name}' from schedule.")
                return True
        except KeyError:
            logger.warning(f"Task '{task_name}' not found in schedule.")

        return False
