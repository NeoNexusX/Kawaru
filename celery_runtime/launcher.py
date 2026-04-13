import asyncio
import os
import sys

from models.auth import SECRET_KEY
from tools.logger import get_logger
from boot.config import get_config
from celery_runtime.runtime import clean_up_stale_task

RUNTIME_ENV_VAR = "CELERY_RUNTIME_ENABLED"
APP_PATH = "celery_runtime.entrypoint:celery_app"


async def start_embedded_celery():
    """由 API 进程托管 worker/beat 子进程。"""
    await clean_up_stale_task()
    logger = get_logger("celery_runtime.launcher")
    ## If you're doing mostly I/O you can have more processes,
    ## but if mostly spending CPU, try to keep it close to the
    ## number of CPUs on your machine. If not set, the number of CPUs/cores
    ## available will be used.
    celery_worker_config = get_config().celery
    worker_pool = celery_worker_config.worker_pool
    if worker_pool not in ["prefork", "gevent", "eventlet"]:
        logger.warning("Invalid worker pool %s, defaulting to prefork", worker_pool)
        worker_pool = "prefork"

    env = os.environ.copy()
    env["SECRET_KEY"] = SECRET_KEY
    env[RUNTIME_ENV_VAR] = "true"

    worker_args = [
        "-A", APP_PATH, "worker",
        "--pool", worker_pool,
        "--loglevel", celery_worker_config.worker_log_level,
        "--logfile", celery_worker_config.worker_log_file,
        "--without-gossip",
        "--concurrency", str(celery_worker_config.worker_concurrency),
    ]
    beat_args = [
        "-A", APP_PATH,
        "beat",
        "--loglevel", "info",
        "--logfile", celery_worker_config.worker_log_file,
    ]

    worker_proc = None
    beat_proc = None
    try:
        worker_proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "celery", *worker_args, stdout=None, stderr=None, env=env
        )
        beat_proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "celery", *beat_args, stdout=None, stderr=None, env=env
        )
        logger.info("Started embedded celery worker subprocess (pid=%s)", getattr(worker_proc, "pid", None))
        logger.info("Started embedded celery beat subprocess (pid=%s)", getattr(beat_proc, "pid", None))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to start embedded celery subprocesses: %s", exc)
    return worker_proc, beat_proc


async def stop_embedded_celery(celery_worker_proc, celery_beat_proc):
    """停止由 API 托管的 worker/beat 子进程。"""
    logger = get_logger("celery_runtime.stop")
    try:
        if celery_worker_proc and celery_worker_proc.returncode is None:
            celery_worker_proc.terminate()
            try:
                await asyncio.wait_for(celery_worker_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Celery worker subprocess did not exit in time; killing")
                celery_worker_proc.kill()
                await celery_worker_proc.wait()

        if celery_beat_proc and celery_beat_proc.returncode is None:
            celery_beat_proc.terminate()
            try:
                await asyncio.wait_for(celery_beat_proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Celery beat subprocess did not exit in time; killing")
                celery_beat_proc.kill()
                await celery_beat_proc.wait()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.exception("Error while terminating embedded celery subprocess: %s", exc)
    finally:
        await clean_up_stale_task()

def enable_embedded_celery() -> bool:
    """控制是否由 API 进程托管 Celery 子进程。"""
    return get_config().celery.enable_embedded_celery
