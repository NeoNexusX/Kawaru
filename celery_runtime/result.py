from typing import Any, Sequence
import asyncio

from celery.result import AsyncResult, GroupResult

from tools.logger import get_logger


async def wait_for_result(result: AsyncResult | GroupResult, poll_interval: float, timeout: float | None = 20) -> Any:
    """异步轮询等待 Celery 结果。"""

    async def _poll_until_ready():
        while not result.ready():
            await asyncio.sleep(poll_interval)

        if isinstance(result, GroupResult):
            results: Sequence[AsyncResult] = result.results # type: ignore
            result_list = []
            for sub_result in results:
                if sub_result.failed():
                    raise sub_result.result
                result_list.append(sub_result.result)
            return result_list
        else:
            if result.failed():
                raise result.result
            return result.result

    if timeout is None:
        return await _poll_until_ready()

    try:
        return await asyncio.wait_for(_poll_until_ready(), timeout=timeout)

    except asyncio.TimeoutError:
        logger = get_logger("background.taskmanager")
        task_name = result.name if isinstance(result, AsyncResult) else "GroupTask"
        logger.error(
            "Celery task timeout, task_id=%s, task_name=%s, timeout=%s",
            result.id,
            task_name,
            timeout,
        )
        return None
