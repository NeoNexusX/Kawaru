import asyncio
import alibabacloud_oss_v2 as oss
import alibabacloud_oss_v2.aio as oss_aio
from alibabacloud_oss_v2 import GetBucketLocationRequest
from boot.config import get_config
from exceptions.file_exceptions import oss_client_create_failed_exception

from tools.logger import get_logger


def create_oss_client() -> oss_aio.AsyncClient | None:
    """Create and return an OSS client"""
    _oss_client = None
    logger = get_logger("oss_storage")
    try:
        oss_config = get_config().oss
        # Read credentials from environment variables to authenticate
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


async def oss_summary(client: oss_aio.AsyncClient | None = None):
    """oss_client 摘要"""
    logger = get_logger("bootstrap.oss_sumary")
    if client is None:
        logger.warning("OSS client is not initialized, skipping OSS summary.")
        return
    try:
        oss_config = get_config().oss
        if oss_config.bucket_name is None:
            raise ValueError("oss bucket_name is empty")
        req = GetBucketLocationRequest(oss_config.bucket_name)
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

def init_oss_summary(client: oss_aio.AsyncClient | None):
    """同步包装异步验证"""
    asyncio.run(oss_summary(client))
