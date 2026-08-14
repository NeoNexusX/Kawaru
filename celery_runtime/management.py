#########################
# Celery 动态定时任务管理器
#
# 【定位】
#   本模块用于「运行时动态增删周期任务」，与 celery_runtime/app.py 中
#   build_beat_schedule() 的「静态全局任务」互补：
#     - 静态任务（heartbeat / 各类 cleanup）：全局共享、间隔写死、启动时注册，
#       走 beat_schedule，**不要**用本模块管理。
#     - 动态任务：由用户操作或数据驱动、运行时才会产生/销毁的周期任务，
#       才用本模块通过 redbeat 写入 Redis。
#
# 【当前状态】
#   全仓暂无调用方，属于为后续扩展预留的能力。仅在出现以下需求时才需要：
#     1) 用户自定义周期任务（如「每隔 N 分钟自动重跑某实验的聚类分析」）
#     2) 多租户各自的独立调度规则
#     3) 不重启服务即可热更新任务频率
#
# 【使用注意】
#   - 任务名需全局唯一且幂等（如 f"auto_analysis_user{uid}_exp{eid}"），重复 add 会覆盖同名 entry。
#   - redbeat 的 entry 存于 Redis；注意 runtime.clean_up_stale_task() 会在启动时清空
#     所有 redbeat:* 键。若要持久化动态任务，需收窄该清理逻辑，或在启动时从 DB 重建。
#   - 示例见文末 add_interval_task 的 docstring。
#########################
import asyncio

from celery.schedules import schedule
from redbeat.schedulers import RedBeatSchedulerEntry as Entry

from celery_runtime.state import get_celery_app
from tools.logger import get_logger


class TaskManager:
    """Celery 任务管理器，提供动态添加/移除定时任务的接口（基于 redbeat 持久化到 Redis）。"""

    @staticmethod
    async def add_interval_task(
        task_name: str,
        task_path: str,
        interval_seconds: int,
        task_args: list | None = None,
        task_kwargs: dict | None = None,
    ):
        """动态添加（或覆盖同名）Celery 定时间隔任务。

        Args:
            task_name: 任务名，需全局唯一（redbeat 以 `redbeat:{task_name}` 为 key 存储）。
            task_path: 任务的可导入路径，如 "background.processing_background.auto_clustering_task"。
            interval_seconds: 执行间隔（秒）。
            task_args: 传给任务的位置参数。
            task_kwargs: 传给任务的关键字参数。

        Returns:
            成功返回 redbeat Entry；失败返回 None（异常已记录日志，不向上抛）。

        示例（用户开启「每隔 N 分钟自动重跑聚类」）：
            await TaskManager.add_interval_task(
                task_name=f"auto_analysis_user{user.id}_exp{exp_id}",
                task_path="background.processing_background.auto_clustering_task",
                interval_seconds=interval_minutes * 60,
                task_kwargs={"user_id": user.id, "exp_id": exp_id},
            )
        """
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
            # redbeat 的 save 是同步阻塞 IO，丢到线程池避免卡住事件循环
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
        """动态移除 Celery 定时任务。

        Args:
            task_name: 要移除的任务名（与 add_interval_task 时使用的名字一致）。

        Returns:
            移除成功返回 True；任务不存在或出错返回 False（不向上抛异常）。
        """
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
