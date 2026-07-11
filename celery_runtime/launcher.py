import asyncio
import os
import sys

from models.auth import SECRET_KEY
from tools.logger import get_logger
from boot.config import get_config
from celery_runtime.runtime import clean_up_stale_task
from database.redis_client import clean_up_stale_packup_data

APP_PATH = "celery_runtime.entrypoint:celery_app"
BEAT_APP_PATH = "celery_runtime.beat_entrypoint:beat_app"


async def _start_worker(*, queues: str, concurrency: int, log_level: str,
                        log_file: str, pool: str, env: dict) -> asyncio.subprocess.Process: # pylint: disable=no-member
    """启动一个 Celery worker 子进程。"""
    worker_args = [
        "-A", APP_PATH, "worker",
        "--pool", pool,
        "--loglevel", log_level,
        "--logfile", log_file,
        "-Q", queues,
        "--without-gossip",
        "--concurrency", str(concurrency),
    ]
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "celery", *worker_args,
        stdout=None, stderr=None, env=env,
    )
    return proc


async def start_embedded_celery():
    """由 API 进程托管 worker/beat 子进程。

    启动 default worker（消费 default 队列）和 celery beat。
    """
    await clean_up_stale_task()
    logger = get_logger("celery_runtime.launcher")
    cfg = get_config()
    env = os.environ.copy()
    env["SECRET_KEY"] = SECRET_KEY

    workers = {}
    beat_proc = None

    try:
        # --- default worker ---
        celery_cfg = cfg.celery
        pool = celery_cfg.worker_pool
        if pool not in ("prefork", "gevent", "eventlet", "solo", "threads"):
            logger.warning("Invalid worker pool %s, defaulting to prefork", pool)
            pool = "prefork"

        default_worker = await _start_worker(
            queues="default",
            concurrency=celery_cfg.worker_concurrency,
            log_level=celery_cfg.worker_log_level,
            log_file=celery_cfg.worker_log_file,
            pool=pool,
            env=env,
        )
        workers["default"] = default_worker
        logger.info("Started default celery worker (pid=%s, queues=default, pool=%s, concurrency=%s)",
                    default_worker.pid, pool, celery_cfg.worker_concurrency)

        # --- beat ---
        # beat 使用独立轻量入口（不加载 task 模块），显著降低内存占用
        beat_args = [
            "-A", BEAT_APP_PATH, "beat",
            "--loglevel", "info",
            "--logfile", celery_cfg.worker_log_file,
        ]
        beat_proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "celery", *beat_args,
            stdout=None, stderr=None, env=env,
        )
        logger.info("Started embedded celery beat subprocess (pid=%s)", beat_proc.pid)

    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("Failed to start embedded celery subprocesses: %s", exc)

    return workers, beat_proc


async def stop_embedded_celery(workers: dict, celery_beat_proc):
    """停止由 API 托管的所有 worker/beat 子进程。"""
    logger = get_logger("celery_runtime.stop")

    async def _stop_proc(proc, name):
        if proc is None or proc.returncode is not None:
            return
        try:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning("Celery %s subprocess did not exit in time; killing", name)
            proc.kill()
            await proc.wait()

    try:
        for name, proc in workers.items():
            await _stop_proc(proc, f"worker/{name}")

        await _stop_proc(celery_beat_proc, "beat")

    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.exception("Error while terminating embedded celery subprocesses: %s", exc)
    finally:
        await clean_up_stale_packup_data()
        await clean_up_stale_task()


def enable_embedded_celery() -> bool:
    """控制是否由 API 进程托管 Celery 子进程。"""
    return get_config().celery.enable_embedded_celery
