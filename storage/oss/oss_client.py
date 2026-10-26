import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any

import alibabacloud_oss_v2 as oss
import alibabacloud_oss_v2.aio as oss_aio
from alibabacloud_oss_v2 import GetBucketLocationRequest
from alibabacloud_oss_v2.aio.transport import AioHttpClient
from alibabacloud_oss_v2.types import HttpRequest

from boot.config import get_config
from constants.file_constants import RELAY_IDLE_TIMEOUT
from exceptions.oss_exceptions import oss_client_create_failed_exception
from models.config_models import OSSSettings
from tools.logger import get_logger

# 中转下载流式透传的 chunk 大小：SDK 默认 16KB 过碎，逐 chunk 循环开销大，调大减少循环次数
RELAY_STREAM_BLOCK_SIZE = 512 * 1024


class _StreamingAioHttpClient(AioHttpClient):
    """AioHttpClient that forces streaming responses.

    SDK v1.2.4 异步客户端从不向 transport 传 stream=True（aio/_aioclient.py 中
    该传参被注释掉），导致 GetObject 的响应体被整体读进内存后 get_object 才返回，
    大文件中转会先占满服务器内存、浏览器长时间无响应。强制 stream=True 让
    GetObject 走真正的逐 chunk 流式。本客户端仅供中转下载使用（GetObject /
    HeadObject 无 body），非 2xx 时 SDK 会自行 read() 解析错误体，不受影响。
    """

    async def send(self, request: HttpRequest, **kwargs: Any):
        kwargs["stream"] = True
        return await super().send(request, **kwargs)


@asynccontextmanager
async def create_oss_client_context():
    """Create an async OSS client with an async context manager.

    Must be used inside a coroutine (``async with``) so the client is
    created, used and closed on the same running event loop.
    """
    client = create_oss_client()
    if client is None:
        raise oss_client_create_failed_exception
    try:
        yield client
    finally:
        await client.close()


def create_oss_client() -> oss_aio.AsyncClient | None:
    """Create an OSS client; callers are responsible for closing it."""
    oss_client = None
    logger = get_logger("oss_storage")
    try:
        oss_config = get_config().oss
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
        oss_client = oss_aio.AsyncClient(cfg)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("create oss client failed: %s", exc)
    return oss_client


def create_oss_sync_client() -> oss.Client | None:
    """Create a synchronous OSS client."""
    oss_client = None
    logger = get_logger("oss_storage")
    try:
        oss_config = get_config().oss
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
        oss_client = oss.Client(cfg)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("create oss client failed: %s", exc)
    return oss_client


def create_internal_oss_client() -> oss_aio.AsyncClient | None:
    """Create an async OSS client bound to the internal endpoint (relay download only).

    中转下载专用：走内网 endpoint，OSS→服务器 这跳不计公网流量费；仅同地域服务器可达。
    返回 None 表示中转不可用（未配置 oss_internal_endpoint 或凭证缺失）。
    """
    oss_client = None
    logger = get_logger("oss_storage")
    try:
        oss_config = get_config().oss
        access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
        access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        if not access_key_id or not access_key_secret:
            raise ValueError("oss access_key_id or access_key_secret is empty")
        if not oss_config.oss_internal_endpoint:
            raise ValueError("oss_internal_endpoint is not configured")
        credentials_provider = oss.credentials.EnvironmentVariableCredentialsProvider()
        cfg = oss.config.load_default()
        cfg.credentials_provider = credentials_provider
        if not oss_config.region_id:
            raise ValueError("oss region_id is empty")
        cfg.region = oss_config.region_id
        cfg.endpoint = oss_config.oss_internal_endpoint
        # 注入强制流式的 HTTP 客户端，超时与 block_size 走构造参数直传：
        # 注入 http_client 后 SDK 不再消费 cfg.readwrite_timeout（_resolve_httpclient 直接返回）。
        # SDK 默认 readwrite_timeout=20s，慢客户端场景下会抢在 relay 空闲超时前抛错；
        # 调大让"是否判卡死"由 RELAY_IDLE_TIMEOUT 说了算。
        cfg.http_client = _StreamingAioHttpClient( # type: ignore
            readwrite_timeout=max(60.0, RELAY_IDLE_TIMEOUT * 2),
            block_size=RELAY_STREAM_BLOCK_SIZE,
        )
        oss_client = oss_aio.AsyncClient(cfg)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("create internal oss client failed: %s", exc)
    return oss_client


async def oss_summary(client: oss_aio.AsyncClient | None = None):
    """输出 OSS 客户端摘要。"""
    logger = get_logger("bootstrap.oss_sumary")
    if client is None:
        logger.warning("OSS client is not initialized, skipping OSS summary.")
        return
    try:
        oss_config = get_config().oss
        check_oss_config(oss_config)
        req = GetBucketLocationRequest(oss_config.bucket_name)  # type: ignore
        res = await client.get_bucket_location(req)  # type: ignore
        if res.status_code == 200:
            logger.info(
                "init oss client success, bucket_name: %s, bucket_location: %s",
                oss_config.bucket_name,
                res.location,
            )
        else:
            raise ConnectionAbortedError(
                "get oss bucket status failed, status_code: " f"{res.status_code}"
            )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("oss initialize failed: %s", exc)
        raise oss_client_create_failed_exception from exc
    finally:
        if client is not None:
            await client.close()


def check_oss_config(config: OSSSettings):
    """检查 OSS 配置。"""
    if config.bucket_name is None:
        raise ValueError("oss bucket_name is empty")
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


def init_oss_summary(client: oss_aio.AsyncClient | None):
    """同步包装异步验证，兼容已有事件循环的场景。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        asyncio.run_coroutine_threadsafe(oss_summary(client), loop)
    else:
        asyncio.run(oss_summary(client))
