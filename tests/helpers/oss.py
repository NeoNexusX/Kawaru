"""
OSS 直传共用工具

抽取 test_oss.py 中「用 STS 临时凭证创建 OSS client → 上传本地文件」的逻辑，
避免多处维护同一段代码。
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path, PurePosixPath

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from boot.config import get_config
from constants.file_constants import OSS_BUCKET_NAME


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


def parse_file_id_from_oss_path(oss_path: str) -> int:
    """从上传对象键 {oss_zip_storage_root}/{file_verify_code}/{file_id}.zip 解析内部 file_id。

    对外接口不再返回 file_id；模拟 FC 回调、拼 origin 路径仍需内部 file_id，
    与真实 FC 一样从对象键解析（file_id 在文件名中）。
    """
    return int(PurePosixPath(oss_path).stem)


async def put_oss_object(*, oss_path: str, data: bytes) -> None:
    """以服务端同源的环境变量凭证（长期 AK/SK）向任意对象键直传真实对象。

    上传接口发放的 STS 只能写预检路径；删除流程测试需要往 origin/、processed/
    等任意前缀放真实对象供 worker 真实清理，走 create_oss_client 同源的环境
    变量凭证（CI 由 compose 注入、本地从 .env 读取，见 conftest）。
    OSS SDK 是同步阻塞的，放入线程池执行以不阻塞事件循环。
    """
    access_key_id = os.environ.get("OSS_ACCESS_KEY_ID")
    access_key_secret = os.environ.get("OSS_ACCESS_KEY_SECRET")
    assert access_key_id and access_key_secret, (
        "OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET not set; "
        "put_oss_object needs server-identical credentials"
    )

    def _put() -> None:
        credentials_provider = oss.credentials.StaticCredentialsProvider(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
        )
        cfg = oss.config.load_default()
        cfg.credentials_provider = credentials_provider
        cfg.region = get_config().oss.region_id or ""  # 与服务端同源：均取自 config
        client = oss.Client(cfg)
        result = client.put_object(
            PutObjectRequest(bucket=OSS_BUCKET_NAME, key=oss_path, body=data)
        )
        assert result.status_code == 200, (
            f"oss put failed: key={oss_path} status={result.status_code}"
        )

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _put)
