import os
from httpx import AsyncClient

from fastapi import status

from celery_runtime.state import get_celery_app
from storage_backend.storage import OssStorage
from constants.file_constants import OSS_BUCKET_NAME, FC_PACKUP_ENDPOINT
from tools.logger import get_logger


logger = get_logger("background.experiment_background_helper")
celery_app = get_celery_app()


@celery_app.task()
async def packup_oss_file_task(filename: str, experiment_folder: str, dest_zip_path: str) -> str | None:
    """
    异步触发打包 OSS 中的实验文件夹
    """
    logger.info(f"start to packup_oss_file, experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")

    async with AsyncClient(timeout=20) as client: # 20秒超时，这个超时是发送异步请求的超时，不是等待FC打包完成的超时
        # 构造event json
        event = {
            "bucket": OSS_BUCKET_NAME,
            "filename": filename,
            "source-dir": experiment_folder,
            "dest-file": dest_zip_path,
        }

        # 获取FC打包函数的token和url配置
        fc_token = os.getenv("OSS_FC_PACKUP_TOKEN")
        fc_url = FC_PACKUP_ENDPOINT
        if not fc_url or not fc_token:
            logger.error("fc_packup_endpoint or fc_packup_token not found")
            return None

        # 调用FC打包函数
        res = await client.post(
            fc_url,
            json=event,
            headers={
                "Authorization": f"Bearer {fc_token}",
                "Content-Type": "application/json",
                "X-Fc-Invocation-Type": "Async",
            },
        )

        # 触发异步打包任务，应返回状态码202 Accepted，包含request_id
        if res.status_code != status.HTTP_202_ACCEPTED:
            logger.error(f"packup_oss_file trigger failed, status_code={res.status_code}, content={res.text}")
            return None

        request_id = res.headers["X-FC-Request-ID"]
        logger.info(f"packup_oss_file task triggered, request_id={request_id},"
                    f"experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")

    return request_id
