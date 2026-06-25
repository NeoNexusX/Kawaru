"""
OSS 直传共用工具

抽取 test_oss.py 中「用 STS 临时凭证创建 OSS client → 上传本地文件」的逻辑，
避免多处维护同一段代码。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from boot.config import get_config


async def upload_file_to_oss(
    *,
    oss_sts_token: dict,
    oss_bucket: str,
    oss_path: str,
    filepath: Path,
    checkpoint_dir: Path,
) -> oss.UploadResult:
    """用 /files/upload 返回的 STS 临时凭证，将本地文件直传到 OSS。

    OSS SDK 是同步阻塞的，放入线程池执行以不阻塞事件循环。
    """
    credentials_provider = oss.credentials.StaticCredentialsProvider(
        access_key_id=oss_sts_token["AccessKeyId"],
        access_key_secret=oss_sts_token["AccessKeySecret"],
        security_token=oss_sts_token["SecurityToken"],
    )
    cfg = oss.config.load_default()
    cfg.credentials_provider = credentials_provider
    cfg.region = get_config().oss.region_id or ""   # 与服务端同源：均取自 test-config
    oss_client = oss.Client(cfg)

    def _put_object() -> oss.UploadResult:
        uploader = oss_client.uploader(enable_checkpoint=True, checkpoint_dir=str(checkpoint_dir))
        return uploader.upload_file(
            PutObjectRequest(bucket=oss_bucket, key=oss_path),
            filepath=str(filepath),
        )

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _put_object)
    assert result.status_code == 200, f"oss put failed: status={result.status}"
    return result
