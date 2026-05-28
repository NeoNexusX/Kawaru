import os
from contextlib import contextmanager
import asyncio
import alibabacloud_oss_v2 as oss
import alibabacloud_oss_v2.aio as oss_aio
from alibabacloud_oss_v2 import GetBucketLocationRequest
from boot.config import get_config
from exceptions.file_exceptions import oss_client_create_failed_exception
from models.config_models import OSSSettings

from tools.logger import get_logger


@contextmanager
def create_oss_client_context():
    """create OSS client with context manager"""
    client = create_oss_client()
    if client is None:
        raise oss_client_create_failed_exception
    try:
        yield client
    finally:
        try:
            asyncio.run(client.close())
        except RuntimeError:
            # 当已有循环运行时，用 create_task 安全关闭
            loop = asyncio.get_event_loop()
            loop.create_task(client.close())


def create_oss_client() -> oss_aio.AsyncClient | None:
    """
    Create OSS client without context manager
    Note: The caller is responsible for closing the client to release resources.
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


def create_oss_sync_client() -> oss.Client | None:
    """
    Create OSS sync client without context manager

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
        _oss_client = oss.Client(cfg)
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("create oss client failed: %s", exc)
    return _oss_client


async def oss_summary(client: oss_aio.AsyncClient | None = None):
    """oss_client 摘要"""
    logger = get_logger("bootstrap.oss_sumary")
    if client is None:
        logger.warning("OSS client is not initialized, skipping OSS summary.")
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
    finally:
        if client is not None:
            await client.close()  # 确保客户端连接被关闭，释放资源


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
    if config.oss_origin_storage_root is None:
        raise ValueError("oss oss_origin_storage_root is empty")
    if config.oss_upload_sts_token_expire_minutes is None:
        raise ValueError("oss oss_upload_sts_token_expire_minutes is empty")
    if config.oss_download_sts_token_expire_minutes is None:
        raise ValueError("oss oss_download_sts_token_expire_minutes is empty")
    if config.fc_log_project_name is None:
        raise ValueError("oss fc_log_project_name is empty")


def init_oss_summary(client: oss_aio.AsyncClient | None):
    """同步包装异步验证"""
    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(oss_summary(client), loop)
