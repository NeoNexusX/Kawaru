from contextlib import contextmanager
from typing import Generator
import os
import asyncio
import alibabacloud_oss_v2 as oss
import alibabacloud_oss_v2.aio as oss_aio
from alibabacloud_oss_v2 import GetBucketLocationRequest

from boot.config import get_config
from exceptions.file_exceptions import oss_client_create_failed_exception
from models.config_models import OSSSettings

from tools.logger import get_logger


def create_oss_client_direct() -> oss_aio.AsyncClient | oss.client.Client | None:
    """
    创建OSS客户端，不通过上下文管理器

    :param sync_client: 是否创建同步客户端
    """
    _oss_client = None
    logger = get_logger("oss_storage")
    try:
        oss_config = get_config().oss
        # Read credentials from environment variables to authenticate
        access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
        access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        if not access_key_id or not access_key_secret:
            raise ValueError("oss access_key_id or access_key_secret is empty")
        credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()
        cfg = oss.config.load_default()
        cfg.credentials_provider = credentials_provider
        if not oss_config.region_id:
            raise ValueError("oss region_id is empty")
        cfg.region = oss_config.region_id
        _oss_client = oss_aio.AsyncClient(cfg)
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("create oss client failed: %s", exc)
    return _oss_client


@contextmanager
def create_oss_client() -> Generator[oss_aio.AsyncClient | None, None, None]:
    """Create and return an OSS client"""
    _oss_client = None
    _oss_client: oss_aio.AsyncClient | None = create_oss_client_direct()

    yield _oss_client

    if _oss_client is not None:
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, _oss_client.close)
        _oss_client = None


if os.getenv("CREATE_CELERY_SUB"):
    oss_client = create_oss_client()
else:
    oss_client: "oss_aio.AsyncClient | None" = None


async def oss_sumary(client: oss_aio.AsyncClient | None):
    """oss_client 摘要"""
    logger = get_logger("bootstrap.oss_sumary")
    if client is None:
        return
    try:
        # 检查 oss 配置是否完整
        oss_config = get_config().oss
        check_oss_config(oss_config)
        # 构造请求对象，检查 bucket 是否正常
        req = GetBucketLocationRequest(oss_config.bucket_name) # type: ignore
        res = await client.get_bucket_location(req) # type: ignore
        if res.status_code == 200:
            logger.info("init oss client success, bucket_name: %s, bucket_location: %s", oss_config.bucket_name, res.location)
        else:
            raise ConnectionAbortedError(f"get oss bucket status failed, status_code: {res.status_code}")
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("oss initialize failed: %s", exc)
        raise oss_client_create_failed_exception from exc

def check_oss_config(config: OSSSettings):
    """检查 oss 配置"""
    if config.bucket_name is None:
        raise ValueError("oss bucket_name is empty")
    if config.fc_logstore_name is None:
        raise ValueError("oss fc_logstore_name is empty")
    if config.region_id is None:
        raise ValueError("oss region_id is empty")
    if config.oss_zip_storage_root is None:
        raise ValueError("oss oss_zip_storage_root is empty")
    if config.oss_experiment_storage_root is None:
        raise ValueError("oss oss_experiment_storage_root is empty")
    if config.oss_upload_sts_token_expire_minutes is None:
        raise ValueError("oss oss_upload_sts_token_expire_minutes is empty")
    if config.oss_download_sts_token_expire_minutes is None:
        raise ValueError("oss oss_download_sts_token_expire_minutes is empty")
    if config.fc_log_project_name is None:
        raise ValueError("oss fc_log_project_name is empty")
