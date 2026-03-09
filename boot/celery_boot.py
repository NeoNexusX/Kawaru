import os
import sys
import shutil
import asyncio
from tools.logger import get_logger
from boot.celeryconfig import celery_worker_config

async def start_celery_worker():
    """启动 Celery worker 作为子进程，并返回进程对象"""
    logger = get_logger("celery_boot")
    celery_proc = None

    CELERYD_CONCURRENCY = celery_worker_config.get("worker_concurrency", 10)
    CELERY_DISABLE_RATE_LIMITS = celery_worker_config.get("worker_disable_rate_limits", True)
    CELERYD_LOG_LEVEL = celery_worker_config.get("worker_log_level", "INFO")
    CELERYD_LOG_FILE = celery_worker_config.get("worker_log_file", "logs/celeryd.log")

    try:
        celery_cmd = shutil.which("celery")
        celery_args = ["-A", "main.celery_app", "worker", "-l", CELERYD_LOG_LEVEL, "--concurrency", str(CELERYD_CONCURRENCY)]
        if celery_cmd:
            # use system celery executable
            celery_proc = await asyncio.create_subprocess_exec(celery_cmd, *celery_args, stdout=None, stderr=None)
        else:
            # fallback to `python -m celery` if celery command not available
            celery_proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "celery", *celery_args, stdout=None, stderr=None)
        logger.info("Started celery worker subprocess (pid=%s)", getattr(celery_proc, 'pid', None))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Failed to start celery subprocess: %s", exc)

    # return process object so other parts of app can inspect/terminate if needed
    return celery_proc

async def stop_celery_worker(celery_proc):
    logger = get_logger("celery_stop")
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
    except Exception as exc:  # pragma: no cover - best-effort cleanup
        logger.exception("Error while terminating celery subprocess: %s", exc)