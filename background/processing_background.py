"""
FC 算法处理触发模块。

通过 HTTP 触发器统一触发 FC 函数，通过 task_type 字段区分任务类型：
- task_type=algorithm: 算法管线处理
- task_type=tic: TIC 图像生成

FC 函数处理完成后通过回调接口返回结果。
"""
import os
from httpx import AsyncClient

from fastapi import status

from celery_runtime.state import get_celery_app
from constants.file_constants import FC_ALGORITHM_ENDPOINT, FC_ALGORITHM_TIMEOUT_SECONDS
from tools.logger import get_logger


logger = get_logger("background.processing_background")
celery_app = get_celery_app()

# 共用同一个端点、token、超时
FC_URL = FC_ALGORITHM_ENDPOINT
FC_TIMEOUT = FC_ALGORITHM_TIMEOUT_SECONDS
FC_TOKEN = os.getenv("OSS_FC_ALGORITHM_TOKEN")


async def _trigger(event: dict) -> bool:
    """发送 HTTP 请求到 FC，返回是否成功触发。"""
    if not FC_URL or not FC_TOKEN:
        logger.error("fc_algorithm_endpoint or OSS_FC_ALGORITHM_TOKEN not found")
        return False

    async with AsyncClient(timeout=FC_TIMEOUT) as client:
        res = await client.post(
            FC_URL,
            json=event,
            headers={
                "Authorization": f"Bearer {FC_TOKEN}",
                "Content-Type": "application/json",
            },
        )
    ok = res.status_code == status.HTTP_200_OK
    if not ok:
        logger.error("trigger fc failed: status=%s content=%s", res.status_code, res.text)
    return ok


@celery_app.task()
async def trigger_fc_algorithm_task(
    run_id: int,
    imzml_path: str,
    config_json: str,
    output_path: str,
) -> None:
    """触发 FC 执行算法管线处理"""
    event = {
        "task_type": "algorithm",
        "run_id": run_id,
        "imzml_path": imzml_path,
        "config_json": config_json,
        "output_path": output_path,
    }
    logger.info("trigger fc algorithm run_id=%s imzml_path=%s", run_id, imzml_path)
    if await _trigger(event):
        logger.info("trigger fc algorithm success run_id=%s", run_id)
    else:
        logger.error("trigger fc algorithm failed run_id=%s", run_id)


@celery_app.task()
async def trigger_fc_tic_task(
    file_id: int,
    imzml_path: str,
) -> None:
    """触发 FC 生成 TIC 图像"""
    event = {
        "task_type": "tic",
        "file_id": file_id,
        "imzml_path": imzml_path,
        "tic_dest": f"tic/file_{file_id}.jpeg",
    }
    logger.info("trigger fc tic file_id=%s imzml_path=%s", file_id, imzml_path)
    if await _trigger(event):
        logger.info("trigger fc tic success file_id=%s", file_id)
    else:
        logger.error("trigger fc tic failed file_id=%s", file_id)
