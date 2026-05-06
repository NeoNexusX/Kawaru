import asyncio
import os
import sys

from models.auth import SECRET_KEY
from tools.logger import get_logger
from boot.config import get_config
from celery_runtime.runtime import clean_up_stale_task

APP_PATH = "celery_runtime.entrypoint:celery_app"


async def _start_worker(*, queues: str, concurrency: int, log_level: str,
                        log_file: str, pool: str, env: dict) -> asyncio.subprocess.Process: # pylint: disable=no-member
    """启动一个 Celery worker 子进程。新增函数将重复逻辑抽离出来，便于同时启动 default 和 compute 两个 worker。"""
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

    启动两个 worker：
      - default worker：消费 default 队列，沿用原有配置
      - compute worker：消费 compute 队列，使用 compute_celery 配置
    """
    await clean_up_stale_task()
    logger = get_logger("celery_runtime.launcher")
    cfg = get_config()
    env = os.environ.copy()
    env["SECRET_KEY"] = SECRET_KEY

    workers = {}
    beat_proc = None

    try:
        # --- default worker（原 I/O 任务）---
        celery_cfg = cfg.celery
        pool = celery_cfg.worker_pool
        if pool not in ("prefork", "gevent", "eventlet"):
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
        logger.info("Started default celery worker (pid=%s, queues=default, concurrency=%s)",
                    default_worker.pid, celery_cfg.worker_concurrency)

        # --- compute worker（重计算任务）---
        compute_cfg = cfg.compute_celery
        if compute_cfg.enable_embedded_celery:
            compute_pool = compute_cfg.worker_pool
            if compute_pool not in ("prefork", "gevent", "eventlet"):
                logger.warning("Invalid compute worker pool %s, defaulting to prefork", compute_pool)
                compute_pool = "prefork"

            compute_worker = await _start_worker(
                queues="compute",
                concurrency=compute_cfg.worker_concurrency,
                log_level=compute_cfg.worker_log_level,
                log_file=compute_cfg.worker_log_file,
                pool=compute_pool,
                env=env,
            )
            workers["compute"] = compute_worker
            logger.info("Started compute celery worker (pid=%s, queues=compute, concurrency=%s)",
                        compute_worker.pid, compute_cfg.worker_concurrency)

        # --- beat ---
        beat_args = [
            "-A", APP_PATH, "beat",
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
        await clean_up_stale_task()


def enable_embedded_celery() -> bool:
    """控制是否由 API 进程托管 Celery 子进程。"""
    return get_config().celery.enable_embedded_celery
