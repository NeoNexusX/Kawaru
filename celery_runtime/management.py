import asyncio

from celery.schedules import schedule
from redbeat.schedulers import RedBeatSchedulerEntry as Entry

from celery_runtime.state import get_celery_app
from tools.logger import get_logger


class TaskManager:
    """Celery 任务管理器，提供动态添加/修改定时任务的接口"""

    @staticmethod
    async def add_interval_task(
        task_name: str,
        task_path: str,
        interval_seconds: int,
        task_args: list | None = None,
        task_kwargs: dict | None = None,
    ):
        """动态添加 Celery 定时间隔任务"""
        logger = get_logger("background.taskmanager")
        try:
            entry = Entry(
                name=task_name,
                task=task_path,
                schedule=schedule(run_every=interval_seconds),
                args=task_args or [],
                kwargs=task_kwargs or {},
                app=get_celery_app(),
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, entry.save)
            logger.info(
                "Added interval task '%s' with path '%s' to run every %s seconds.",
                task_name,
                task_path,
                interval_seconds,
            )
            return entry
        except Exception as exc:  # pylint:disable = broad-exception-caught
            logger.error("Cannot add interval task due to error: %s", exc)
            return None

    @staticmethod
    async def remove_task(task_name: str):
        """动态移除 Celery 定时任务"""
        logger = get_logger("background.taskmanager")
        key = f"redbeat:{task_name}"
        loop = asyncio.get_running_loop()

        try:
            def _load_entry() -> Entry | None:
                return Entry.from_key(key, app=get_celery_app())

            entry = await loop.run_in_executor(None, _load_entry)

            if entry:
                await loop.run_in_executor(None, entry.delete)
                logger.info("Removed task '%s' from schedule.", task_name)
                return True
        except KeyError:
            logger.warning("Task '%s' not found in schedule.", task_name)
        except Exception as exc:  # pylint:disable = broad-exception-caught
            logger.error("Cannot remove task due to error: %s", exc)

        return False
