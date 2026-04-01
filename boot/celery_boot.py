import sys
import os
import asyncio
from logging import Logger

from celery import Celery

from database.redis_client import connect_new_redis
from models.auth import SECRET_KEY
from boot.config import get_config

from tools.logger import get_logger


async def start_celery_worker():
    """启动 Celery worker 作为子进程，并返回进程对象"""
    await clean_up_stale_task() # 启动worker前先清理过期任务数据
    logger = get_logger("bootstrap.celery_boot")
    celery_proc = None

    ## If you're doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    celery_worker_config = get_config().celery
    worker_concurrency = celery_worker_config.worker_concurrency
    worker_log_level = celery_worker_config.worker_log_level
    worker_log_file = celery_worker_config.worker_log_file

    try:
        # 拷贝主进程运行环境，确保worker子进程运行环境与主进程一致
        env = os.environ.copy()
        env["SECRET_KEY"] = SECRET_KEY  # 确保 Celery worker 子进程也能访问 SECRET_KEY 环境变量
        env["CREATE_CELERY_SUB"] = "True" # 告知子进程在 import 时基于 config 创建子进程的 celery app
        celery_args = ["-A", "background.celery_app.celery_app", "worker",
                       "--beat",
                       "--loglevel", worker_log_level,
                       "--logfile", worker_log_file,
                       "--without-gossip",
                       "--concurrency", str(worker_concurrency),
                       #"--task-events"
                       ]

        celery_proc = await asyncio.create_subprocess_exec(sys.executable,
                                                           "-m", "celery", *celery_args,
                                                           stdout=None, stderr=None, env=env)

        logger.info("Started celery worker subprocess (pid=%s)", getattr(celery_proc, 'pid', None))

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to start celery subprocess: %s", exc)

    # return process object so other parts of app can inspect/terminate if needed
    return celery_proc


async def stop_celery_worker(celery_proc):
    """FastAPI程序退出时安全地终止 Celery worker 子进程"""
    logger = get_logger("bootstrap.celery_stop")
    try:
        # try graceful termination
        celery_proc.terminate()
        try:
            await asyncio.wait_for(celery_proc.wait(), timeout=10)
            logger.info("Celery subprocess exited cleanly")
        except asyncio.TimeoutError:
            logger.warning("Celery subprocess did not exit in time; killing")
            celery_proc.kill()
            await celery_proc.wait()
        finally:
            await clean_up_stale_task() # 终止worker后清理过期任务数据
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("Error while terminating celery subprocess: %s", exc)


def celery_summary(app: Celery, logger: Logger):
    """输出 Celery 配置摘要"""

    # 关键配置信息
    broker = app.conf.get('broker_url', 'N/A')
    backend = app.conf.get('result_backend', 'N/A')
    redbeat_backend = app.conf.get('redbeat_redis_url', 'N/A')
    timezone = app.conf.get('timezone', 'UTC')

    # 任务模块
    tasks = list(app.tasks.keys())
    # 过滤掉 Celery 内部任务
    user_tasks = [t for t in tasks if not t.startswith('celery.')]

    # Beat 调度任务
    beat_schedule = app.conf.get('beat_schedule', {})

    logger.info("=" * 50)
    logger.info("Celery Configuration Summary")
    logger.info("-" * 50)
    logger.info(f"Broker:     {broker}")
    logger.info(f"Backend:    {backend}")
    logger.info(f"Redbeat Backend: {redbeat_backend}")
    logger.info(f"Timezone:   {timezone}")
    logger.info(f"Tasks:      {len(user_tasks)} registered")
    for task in sorted(user_tasks):
        logger.info(f"            - {task}")

    if beat_schedule:
        logger.info(f"Beat Jobs:  {len(beat_schedule)} scheduled")
        for name, conf in beat_schedule.items():
            schedule = conf.get('schedule', 'N/A')
            task_name = conf.get('task', 'N/A')
            logger.info(f"            - {name}: {task_name} ({schedule})")
    else:
        logger.info("Beat Jobs:  None")

    logger.info("=" * 50)


async def clean_up_stale_task():
    """清理 RedBeat 存储的定时任务数据，防止过期任务积累"""
    logger = get_logger("bootstrap.redbeat_cleanup")
    _redis_client = None

    try:
        # 获取所有 RedBeat 任务条目并删除
        redis_cfg = get_config().redis
        _redis_client = connect_new_redis(db_index=redis_cfg.celery_redbeat_db_index)
        prefix = 'redbeat:'
        pattern = f"{prefix}*"
        delete_count = 0

        async for key in _redis_client.scan_iter(match=pattern):
            await _redis_client.delete(key)
            delete_count += 1
        logger.info("Cleaned up %d stale RedBeat scheduler entries", delete_count)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to clean up RedBeat storage: %s", exc)

    finally:
        if _redis_client is not None:
            try:
                await _redis_client.aclose()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to close Redis client after stale task cleanup: %s", exc)
