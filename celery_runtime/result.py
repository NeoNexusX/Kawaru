import asyncio

from celery.result import AsyncResult

from tools.logger import get_logger


async def wait_for_result(result: AsyncResult, poll_interval: float, timeout: float | None = 20):
    """异步轮询等待 Celery 结果。"""

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
        logger = get_logger("background.taskmanager")
        logger.error(
            "Celery task timeout, task_id=%s, task_name=%s, timeout=%s",
            result.id,
            result.name,
            timeout,
        )
        return None
