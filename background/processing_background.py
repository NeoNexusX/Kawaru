"""
FC 算法处理触发模块。

通过 HTTP 触发阿里云 FC 函数执行算法管线，
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


@celery_app.task()
async def trigger_fc_algorithm_task(
    run_id: int,
    imzml_path: str,
    config_json: str,
    output_path: str,
) -> None:
    """
    通过 HTTP 触发 FC 函数执行算法处理

    FC 函数处理完成后会通过自身环境变量中配置的回调地址通知主服务，
    本任务只负责触发，不等待结果。
    """
    logger.info(
        "trigger fc algorithm run_id=%s imzml_path=%s output_path=%s",
        run_id, imzml_path, output_path,
    )

    async with AsyncClient(timeout=FC_ALGORITHM_TIMEOUT_SECONDS) as client:
        event = {
            "run_id": run_id,
            "imzml_path": imzml_path,
            "config_json": config_json,
            "output_path": output_path,
        }

        fc_token = os.getenv("OSS_FC_ALGORITHM_TOKEN")
        fc_url = FC_ALGORITHM_ENDPOINT
        if not fc_url or not fc_token:
            logger.error(
                "fc_algorithm_endpoint or OSS_FC_ALGORITHM_TOKEN not found, run_id=%s",
                run_id,
            )
            return

        res = await client.post(
            fc_url,
            json=event,
            headers={
                "Authorization": f"Bearer {fc_token}",
                "Content-Type": "application/json",
            },
        )

        if res.status_code != status.HTTP_200_OK:
            logger.error(
                "trigger fc algorithm failed run_id=%s status=%s content=%s",
                run_id, res.status_code, res.text,
            )
        else:
            logger.info("trigger fc algorithm success run_id=%s", run_id)
