import os
import asyncio
from httpx import AsyncClient

from fastapi import status

from celery_runtime.state import get_celery_app
from storage_backend.storage import OssStorage
from constants.file_constants import OSS_BUCKET_NAME, FC_PACKUP_ENDPOINT
from tools.logger import get_logger


logger = get_logger("background.experiment_background_helper")
celery_app = get_celery_app()


@celery_app.task()
async def packup_oss_file_task(experiment_folder: str, dest_zip_path: str) -> tuple[str | None, str | None, str | None]:
    """
    打包 OSS 中的实验文件夹
    """
    logger.info(f"start to packup_oss_file, experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")

    async with AsyncClient() as client:
        # 获取STS Token 并构造credentials
        sts_token = await OssStorage.get_packup_sts_token(experiment_folder, dest_zip_path)
        if not sts_token:
            logger.error(f"get_packup_sts_token failed, experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")
            return (None, None, None)

        credentials = {}
        credentials["accessKeyId"] = sts_token.AccessKeyId
        credentials["accessKeySecret"] = sts_token.AccessKeySecret
        credentials["securityToken"] = sts_token.SecurityToken

        # 构造event json
        event = {
            "bucket": OSS_BUCKET_NAME,
            "source-dir": experiment_folder,
            "dest-file": dest_zip_path,
            "credentials": credentials,
        }

        # 获取FC打包函数的token和url配置
        fc_token = os.getenv("OSS_FC_PACKUP_TOKEN")
        fc_url = FC_PACKUP_ENDPOINT
        if not fc_url or not fc_token:
            logger.error("fc_packup_endpoint or fc_packup_token not found")
            return (None, None, None)

        # 调用FC打包函数
        res = await client.post(
            fc_url,
            json=event,
            headers={
                "Authorization": f"Bearer {fc_token}",
                "Content-Type": "application/json",
            },
        )
        request_id = res.headers["X-FC-Request-ID"]

        # 返回签名后的下载URL
        if res.status_code != status.HTTP_302_FOUND:
            logger.error(f"packup_oss_file failed, status_code={res.status_code}, content={res.text}")
            return (None, None, request_id)

        zip_download_url = res.headers["Location"]
        logger.info(f"packup_oss_file success, zip_download_url={zip_download_url}")

    return (dest_zip_path, zip_download_url, request_id)