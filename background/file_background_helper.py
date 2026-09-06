#########################
# File Background Helper - 后台任务辅助函数
# 职责：低级后台任务函数，避免循环导入
#########################
from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config as CredentialConfig
from alibabacloud_sts20150401.client import Client as StsClient
from alibabacloud_sts20150401.models import AssumeRoleRequest
from alibabacloud_oss_v2 import (Client as OssSyncClient, HeadObjectRequest,
                                  DeleteObjectRequest, ListObjectsV2Request, Tag,
                                  PutObjectTaggingRequest, Tagging, TagSet, GetObjectRequest,
                                  DeleteMultipleObjectsRequest, DeleteObject, credentials)
from alibabacloud_oss_v2.config import load_default
from alibabacloud_tea_openapi import models as open_api_models

from celery_runtime.state import get_celery_app

from storage.oss.oss_client import create_oss_client_context, create_oss_sync_client

from constants.file_constants import (
    OSS_REGION_ID, OSS_BUCKET_NAME
)

from tools.logger import get_logger

logger = get_logger("background.file_background")
celery_app = get_celery_app()


@celery_app.task()
async def tag_oss_file_task(file_path: str, tags: dict[str, str]) -> bool:
    """为OSS文件添加标签"""
    async with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                logger.error("oss client create failed")
                return False

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                logger.error("oss bucket name not set")
                return False

            # 构造tagging
            tag_set: list[Tag] = []
            for key, value in tags.items():
                tag_set.append(Tag(key=key, value=value))
            tagging = Tagging(tag_set=TagSet(tag_set))

            # 发送请求
            req = PutObjectTaggingRequest(bucket=bucket_name, key=file_path, tagging=tagging) # type: ignore
            res = await oss_client.put_object_tagging(req)
            if res.status_code != 200:
                logger.warning(f"oss file {file_path} tag failed, status code {res.status_code}")
                return False
            return True
        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to tag oss file: {e}")
            return False


@celery_app.task()
async def check_oss_file_exist_task(file_path: str) -> bool:
    """验证OSS文件或目录是否存在，存在返回True，不存在则返回False"""
    async with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                logger.error("oss client create failed")
                return False

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                logger.error("oss bucket name not set")
                return False

            if file_path.endswith("/"): # 目录
                # 发送请求
                req = ListObjectsV2Request(bucket=bucket_name, prefix=file_path) # type: ignore

                # 检查目录是否存在
                resp = await oss_client.list_objects_v2(req)
                key_count = resp.key_count
                is_dir_exist = key_count is not None and key_count > 0
                if not is_dir_exist:
                    logger.warning(f"oss directory {file_path} not exist")
                    return False

            else: # 文件
                # 发送请求
                req = HeadObjectRequest(bucket=bucket_name, key=file_path) # type: ignore
                meta = await oss_client.head_object(req)

                # 检查文件是否存在
                if meta.status_code != 200:
                    logger.warning(f"oss file {file_path} not exist")
                    return False

            return True

        except Exception as e: # pylint:disable = broad-exception-caught
            if "NoSuchKey" in str(e):
                logger.warning(f"oss file {file_path} not exist")
            else:
                logger.error(f"failed to check oss file: {e}")

            return False


@celery_app.task()
async def get_oss_file_size_task(file_path: str) -> int | None:
    """
    获取OSS文件大小

    :param file_path: OSS文件路径（如"folder/file.zip")
    :return: 文件大小
    """
    async with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                logger.error("oss client create failed")
                return None

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                logger.error("oss bucket name not set")
                return None

            # 发送请求
            req = HeadObjectRequest(bucket=bucket_name, key=file_path) # type: ignore
            meta = await oss_client.head_object(req)

            # 检查文件是否存在
            if meta.status_code != 200:
                logger.warning(f"oss file {file_path} not found")
                return None

            # 返回文件大小
            return meta.content_length

        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to get oss file size: {e}")
            return None


@celery_app.task()
def get_oss_upload_sts_token_task(file_path: str) -> dict:
    """
    获取OSS上传STS Token

    :param file_path: 文件路径（如"folder/file.zip")
    """
    file_path = file_path.rstrip('/').lstrip('/')

    # 构建STS策略
    policy = {
        "Version": "1",
        "Statement": [
            {
            "Effect": "Allow",
            "Action": ["oss:PutObject", "oss:GetObject", "oss:AbortMultipartUpload", "oss:ListMultipartUploadParts"],
            "Resource": [
               f"acs:oss:*:*:{OSS_BUCKET_NAME}/{file_path}"
            ]
            }
        ]
    }

    # 构建role_session_name
    code = hashlib.md5(str(Path(file_path).stem).encode()).hexdigest()
    role_session_name = f"upload-{code}-{datetime.now().timestamp()}"

    # 调用STS API获取上传STS Token
    result = _get_oss_sts_token(policy, role_session_name)

    return result


def _get_oss_sts_token(policy: dict, role_session_name: str,
                        duration_seconds: int = 3600) -> dict:
    # 从环境变量获取OSS角色权限相关配置
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
    role_arn = os.getenv("OSS_ROLE_ARN")
    region_id = OSS_REGION_ID

    if not access_key_id or not access_key_secret or not role_arn or not region_id:
        raise ValueError("oss access_key_id or access_key_secret or role_arn or region_id not set")

    # 创建配置
    credential_config = CredentialConfig(
        type='access_key',
        access_key_id=access_key_id,
        access_key_secret=access_key_secret
    )

    credential = CredentialClient(credential_config)

    client_config = open_api_models.Config(
        credential=credential,
        region_id=region_id,
    )

    # 创建STS客户端
    sts_client = StsClient(client_config)

    # 创建请求
    request = AssumeRoleRequest(
        role_arn=role_arn,
        role_session_name=role_session_name,
        policy=json.dumps(policy),
        duration_seconds=duration_seconds
    )

    # 发送请求并获取响应
    response = sts_client.assume_role(request)
    result = response.body.credentials.to_map()

    return result


@celery_app.task()
async def delete_oss_file_task(file_path: str) -> bool | None:
    """
    删除OSS文件（celery 任务壳，供 OssStorage.delete_file 的存量调用方过渡期使用；
    删除主流程统一走 delete_objects_task + delete_oss_objects 原语）

    :param file_path: OSS文件路径（如"folder/file.zip")

    :return: bool 是否删除成功
    """
    return await delete_oss_objects(file_path)


async def delete_oss_objects(file_paths: str | list[str]) -> bool:
    """
    删除一个或多个 OSS 对象/目录前缀（纯 async 原语，进程内直接调用，不投递 celery 任务）。

    幂等：删不存在的 key 返回 204 同样算成功，可无限重试——这是删除状态位
    方案「DELETING 卡死可重投」的基石。目录前缀（尾带 /）走 list + 批量删除，
    并清理目录占位对象。

    :param file_paths: 单个 OSS 路径或路径列表

    :return: True 全部删除成功（含输入为空）；False 任一对象删除失败
    """
    paths = [file_paths] if isinstance(file_paths, str) else [p for p in file_paths if p]
    if not paths:
        return True

    async with create_oss_client_context() as oss_client:
        try:
            # 判断oss客户端是否能正常使用
            if oss_client is None:
                logger.error("oss client create failed")
                return False

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                logger.error("oss bucket name not set")
                return False

            err_count = 0
            for file_path in paths:
                if file_path.endswith("/"):
                    err_count += await _delete_oss_prefix(oss_client, bucket_name, file_path)
                else:
                    req = DeleteObjectRequest(bucket=bucket_name, key=file_path)  # type: ignore
                    res = await oss_client.delete_object(req)
                    if res.status_code != 204:
                        logger.error("oss file %s deleted failed", file_path)
                        err_count += 1
                        continue
                    logger.info("oss file %s deleted", file_path)

            return err_count == 0
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"failed to delete oss objects: {e}")
            return False


async def cleanup_oss_objects_best_effort(out_path: str | None, context: str = "") -> None:
    """
    best-effort 删除单个 OSS 对象/目录前缀，失败仅记日志、绝不抛异常。

    用于回调丢弃后的善后（FC 迟到写入的输出对象无人认领，防孤儿）等
    「删除失败可接受」的场景：底层 delete_oss_objects 幂等（204 算成功），
    未删净的对象由 bucket 生命周期规则兜底。

    :param out_path: OSS 路径；None/空字符串直接返回
    :param context: 日志用途的场景标注（如 "late algorithm callback output"）
    """
    if not out_path:
        return
    try:
        ok = await delete_oss_objects(out_path)
        if ok:
            logger.info("cleaned up %s path=%s", context or "oss objects", out_path)
        else:
            logger.warning("failed to clean up %s path=%s", context or "oss objects", out_path)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("cleanup %s failed path=%s error=%s", context or "oss objects", out_path, exc)


async def _delete_oss_prefix(oss_client, bucket_name: str, prefix: str) -> int:
    """
    删除一个目录前缀（尾带 /）下的全部对象及目录占位对象。

    :return: 失败计数
    """
    keys: list[str] = []
    continuation_token = None
    while True:
        req = ListObjectsV2Request(
            bucket=bucket_name,
            prefix=prefix,
            continuation_token=continuation_token,
        )  # type: ignore
        res = await oss_client.list_objects_v2(req)
        if res.contents:
            keys.extend(obj.key for obj in res.contents)
        if not res.is_truncated:
            break
        continuation_token = res.next_continuation_token

    err_count = 0
    if len(keys) > 1:
        # 批量删除：每批最多 1000 个对象，一次 HTTP 请求
        batch_size = 1000
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            req = DeleteMultipleObjectsRequest(
                bucket=bucket_name,
                objects=[DeleteObject(key=k) for k in batch],
                quiet=True,
            )  # type: ignore
            res = await oss_client.delete_multiple_objects(req)
            if res.status_code != 200:
                logger.error(
                    "oss batch delete failed batch=%d-%d status=%s",
                    i, i + len(batch), res.status_code,
                )
                err_count += 1
            else:
                logger.info("oss batch deleted %d objects", len(batch))
    else:
        for key in keys:
            req = DeleteObjectRequest(bucket=bucket_name, key=key)  # type: ignore
            res = await oss_client.delete_object(req)
            if res.status_code != 204:
                logger.error(f"oss file {key} deleted failed")
                err_count += 1
                continue
            logger.info(f"oss file {key} deleted")

    # 清理目录占位对象
    req = DeleteObjectRequest(bucket=bucket_name, key=prefix)  # type: ignore
    res = await oss_client.delete_object(req)
    if res.status_code != 204:
        logger.error(f"oss folder {prefix} deleted failed")
        err_count += 1

    return err_count


@celery_app.task()
def sign_url_task(file_path: str, expires: int) -> str | None: #TODO：这个函数后续可以考虑和下面这个函数合并，减少重复代码
    """
    签名OSS文件下载URL

    :param file_path: OSS文件路径（如"folder/file.zip")
    :param expires: 过期时间（秒），由调用方传入

    :return: str 签名后的URL
    """
    # presign 为纯本地签名计算（SDK 内部使用 _nopHttpClient，不发网络请求），
    # 因此本任务保持同步执行，不占用事件循环
    oss_client = create_oss_sync_client()
    try:
        # 创建OSS客户端
        if oss_client is None:
            logger.error("oss client create failed")
            return None

        # 获取bucket名称
        bucket_name = OSS_BUCKET_NAME
        if not bucket_name:
            logger.error("oss bucket name not set")
            return None

        # 生成预签名的URL
        pre_result = oss_client.presign(
            GetObjectRequest(
                bucket=bucket_name,  # 指定存储空间名称
                key=file_path,        # 指定对象键名
                response_content_disposition="attachment",# 设置为强制下载
            ),
            expires=timedelta(seconds=expires),
        )

        sign_url = pre_result.url
        return sign_url
    except Exception as e: # pylint:disable = broad-exception-caught
        logger.error(f"failed to sign oss file: {e}")
        return None

@celery_app.task()
def get_oss_presigned_url_task(file_path: str, expires: int = 300) -> str | None:
    """
    生成 OSS 文件预签名下载 URL。
    """
    try:
        access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
        access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        if not access_key_id or not access_key_secret:
            logger.error("oss credentials not set")
            return None

        bucket_name = OSS_BUCKET_NAME
        if not bucket_name:
            logger.error("oss bucket name not set")
            return None

        creds_provider = credentials.EnvironmentVariableCredentialsProvider()
        cfg = load_default()
        cfg.credentials_provider = creds_provider
        cfg.region = OSS_REGION_ID # type: ignore

        sync_client = OssSyncClient(cfg)

        req = GetObjectRequest(
            bucket=bucket_name,
            key=file_path,
        )
        result = sync_client.presign(req, expires=timedelta(seconds=expires)) # type: ignore
        return result.url
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("failed to generate presigned url: %s", e)
        return None


@celery_app.task()
def get_zarr_read_sts_token_task(folder_path: str, expires: int) -> dict:
    """
    获取 zarr 文件夹的只读 STS Token。

    :param folder_path: OSS 文件夹路径，如 "processed/run_42.zarr"
    :param expires: Token 有效期（秒）
    """
    folder_path = folder_path.rstrip("/")

    policy = {
        "Version": "1",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["oss:GetObject"],
                "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}/{folder_path}/*"]
            },
            {
                "Effect": "Allow",
                "Action": ["oss:ListObjects"],
                "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}"],
                "Condition": {
                    "StringLike": {
                        "oss:Prefix": [f"{folder_path}/*"]
                    }
                }
            }
        ]
    }

    code = hashlib.md5(folder_path.encode()).hexdigest()[:8]
    role_session_name = f"zarr-read-{code}-{datetime.now().timestamp()}"

    return _get_oss_sts_token(policy, role_session_name, expires)
