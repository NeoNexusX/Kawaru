import os
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


if os.getenv("CREATE_CELERY_SUB"):
    oss_client = create_oss_client()
else:
    oss_client: "oss_aio.AsyncClient | None" = None


async def oss_sumary(client: oss_aio.AsyncClient):
    """oss_client 摘要"""
    logger = get_logger("bootstrap.oss_sumary")
    try:
        oss_config = get_config().oss
        assert oss_config.bucket_name is not None
        req = GetBucketLocationRequest(oss_config.bucket_name)
        res = await client.get_bucket_location(req) # type: ignore
        if res.status_code == 200:
            logger.info("init oss client success, bucket_name: %s, bucket_location: %s", oss_config.bucket_name, res.location)
        else:
            raise ConnectionAbortedError(f"get oss bucket status failed, status_code: {res.status_code}")
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("oss initialize failed: %s", exc)
        raise oss_client_create_failed_exception from exc
