"""
重计算 Celery 任务模块。

此模块的任务由独立的 compute worker 消费（队列名: compute），
适用于 CPU 密集型、同步（非协程）的数据处理任务。
"""

from celery_runtime.state import get_celery_app
from tools.logger import get_logger

logger = get_logger("background.compute_background")
celery_app = get_celery_app()


@celery_app.task()  # type: ignore
def sample_compute_task(data: dict) -> dict:
    """重计算任务示例。"""
    logger.info("Received compute task with data keys: %s", list(data.keys()))
    # TODO: 替换为实际的重计算逻辑
    result = {"status": "completed", "input_keys": list(data.keys())}
    return result
