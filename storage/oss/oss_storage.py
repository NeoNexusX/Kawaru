#########################
# OSS Storage - OSS 操作封装
# 职责：上传/读取 STS、存在性检查、取大小、打标签、签名、删除
# 全部在调用方进程内直接执行，不投递 Celery 任务、不轮询结果：
# - 异步 OSS 客户端按调用创建、用完关闭（create_oss_client_context），
#   API 进程与 Celery 任务（每个任务一个新事件循环）都能安全调用
# - STS 走 SDK 原生异步接口 assume_role_async；签名为本地计算，不发网络请求
#########################
import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta
from functools import cache
from pathlib import Path
from typing import Awaitable, TypeVar

import alibabacloud_oss_v2 as oss
from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config as CredentialConfig
from alibabacloud_oss_v2 import (
    DeleteMultipleObjectsRequest,
    DeleteObject,
    DeleteObjectRequest,
    GetObjectRequest,
    HeadObjectRequest,
    ListObjectsV2Request,
    PutObjectTaggingRequest,
    Tag,
    Tagging,
    TagSet,
)
from alibabacloud_sts20150401.client import Client as StsClient
from alibabacloud_sts20150401.models import AssumeRoleRequest
from alibabacloud_tea_openapi import models as open_api_models

from constants.experiment_constants import OSS_PROCESSED_STORAGE_ROOT
from constants.file_constants import (
    OSS_BUCKET_NAME,
    OSS_CDN_DOMAIN,
    OSS_DEFAULT_TIMEOUT_SECONDS,
    OSS_DELETE_OBJECT_TIMEOUT_SECONDS,
    OSS_DELETE_PREFIX_TIMEOUT_SECONDS,
    OSS_REGION_ID,
    OSS_UPLOAD_STS_TOKEN_EXPIRE_SECONDS,
    OSS_ZIP_STORAGE_ROOT,
)
from exceptions.oss_exceptions import oss_client_create_failed_exception, oss_timeout_exception
from models.auth import OssStsToken
from storage.oss.oss_client import create_oss_client_context, create_oss_sync_client
from tools.logger import get_logger

logger = get_logger("storage")

_T = TypeVar("_T")
_D = TypeVar("_D")


class OssStorage:
    """OSS 存储相关操作（进程内直接执行，不经过 Celery）。"""

    @staticmethod
    def prepare_storage_path(file_id: int, file_verify_code: str, file_type: str) -> str:
        """为 OSS 存储准备最终保存路径。

        对象键使用唯一标识而非文件名：data/{file_verify_code}/{file_id}.{file_type}
        verify_code 前置将键打散到随机前缀，避免自增 file_id 造成 OSS 分区热点；
        file_id 随对象键经 OSS 事件传给解压 FC，FC 从文件名解析后回调后端。
        """
        return f"{OSS_ZIP_STORAGE_ROOT.rstrip('/')}/{file_verify_code}/{file_id}.{file_type}"  # type: ignore

    @staticmethod
    def prepare_run_output_path(run_id: int) -> str:
        """为算法 run 的 zarr 输出准备最终保存路径。

        对象键打散到随机前缀，避免自增 run_id 形成 OSS 分区热点：
        run_{md5后6位}_{run_id}.zarr/。md5(run_id) 后 6 位为确定性派生前缀
        （同 id 恒同值，重触发幂等）；run_id 保留在键中保证唯一（6 位哈希
        空间有限会碰撞，仅负责打散）。
        """
        hash6 = hashlib.md5(str(run_id).encode()).hexdigest()[-6:]
        return f"{OSS_PROCESSED_STORAGE_ROOT.rstrip('/')}/run_{hash6}_{run_id}.zarr/"  # type: ignore

    @staticmethod
    async def get_upload_sts_token(file_path: str) -> OssStsToken | None:
        """获取 OSS 上传 STS Token（权限限定到单个对象键）；超时返回 None。"""
        object_key = file_path.rstrip('/').lstrip('/')
        policy = {
            "Version": "1",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["oss:PutObject", "oss:GetObject", "oss:AbortMultipartUpload", "oss:ListMultipartUploadParts"],
                    "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}/{object_key}"],
                }
            ],
        }
        code = hashlib.md5(str(Path(object_key).stem).encode()).hexdigest()
        role_session_name = f"upload-{code}-{datetime.now().timestamp()}"

        # 有效期由配置文件控制
        result = await _wait_bounded(
            _assume_role(policy, role_session_name, OSS_UPLOAD_STS_TOKEN_EXPIRE_SECONDS),
            None, "get oss upload sts token", file_path,
        )
        return OssStsToken(**result) if result is not None else None

    @staticmethod
    async def check_file_exist(file_path: str) -> bool:
        """检查 OSS 文件或文件夹（尾带 /）是否存在；出错或超时按不存在处理。"""
        return await _wait_bounded(_object_exists(file_path), False, "check oss file existence", file_path)

    @staticmethod
    async def get_file_size(file_path: str) -> int:
        """获取 OSS 文件大小；出错或超时返回 0。"""
        size = await _wait_bounded(_head_object_size(file_path), None, "get oss file size", file_path)
        return size or 0

    @staticmethod
    async def delete_file(file_path: str | list[str]) -> bool | None:
        """
        删除 OSS 文件，支持单个 key 或 key 列表。

        单对象与 chunk 目录（以 '/' 结尾，如 zarr 输出目录）由 delete_oss_objects
        内部分支自动区分：目录走 list + 批量删除，单对象直接 delete。

        Returns:
            None: 路径为空（跳过删除）
            True/False: 全部删除成功 / 存在失败

        Raises:
            oss_timeout_exception: 超过删除超时（目录前缀与单对象分别配置）
        """
        paths = [file_path] if isinstance(file_path, str) else list(file_path)
        paths = [p for p in paths if p]
        if not paths:
            logger.warning("empty path for deletion, skipping")
            return None

        timeout = (
            OSS_DELETE_PREFIX_TIMEOUT_SECONDS
            if any(p.endswith("/") for p in paths)
            else OSS_DELETE_OBJECT_TIMEOUT_SECONDS
        )
        try:
            return await asyncio.wait_for(delete_oss_objects(paths), timeout=timeout)
        except asyncio.TimeoutError as exc:
            logger.error("delete oss file timed out after %ss: %s", timeout, paths)
            raise oss_timeout_exception from exc

    @staticmethod
    async def tag_oss_file(file_path: str, tag: dict[str, str]) -> bool:
        """给 OSS 文件打标签；出错或超时返回 False。"""
        return await _wait_bounded(_put_object_tagging(file_path, tag), False, "tag oss file", file_path)

    @staticmethod
    async def sign_download_url(file_path: str, expires: int) -> str | None:
        """签名 OSS 文件下载 URL；失败返回 None。

        presign 为纯本地签名计算，直接在当前协程内完成；保留 async 签名以兼容调用方的 await。
        """
        return _presign_get_url(file_path, expires)

    @staticmethod
    async def get_zarr_read_sts_token(
        folder_path: str,
        expires: int = 3600,
    ) -> OssStsToken | None:
        """获取 zarr 文件夹只读 STS Token（如 "processed/run_a1b2c3_42.zarr"）；超时返回 None。"""
        folder = folder_path.rstrip("/")
        policy = {
            "Version": "1",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["oss:GetObject"],
                    "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}/{folder}/*"]
                },
                {
                    "Effect": "Allow",
                    "Action": ["oss:ListObjects"],
                    "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}"],
                    "Condition": {
                        "StringLike": {
                            "oss:Prefix": [f"{folder}/*"]
                        }
                    }
                }
            ]
        }
        code = hashlib.md5(folder.encode()).hexdigest()[:8]
        role_session_name = f"zarr-read-{code}-{datetime.now().timestamp()}"

        result = await _wait_bounded(
            _assume_role(policy, role_session_name, expires),
            None, "get zarr read sts token", folder_path,
        )
        return OssStsToken(**result) if result is not None else None


#########################
# 进程内执行的 OSS 原语
#########################

async def _wait_bounded(op: Awaitable[_T], default: _D, action: str, target: str) -> _T | _D:
    """
    在 OSS_DEFAULT_TIMEOUT_SECONDS 内等待一次 OSS 调用，超时返回 default、不抛异常。

    原生协程超时会被真正取消（客户端随 async with 关闭），不会在后台继续执行。
    """
    try:
        return await asyncio.wait_for(op, timeout=OSS_DEFAULT_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.error("%s timed out after %ss: %s", action, OSS_DEFAULT_TIMEOUT_SECONDS, target)
        return default


async def _put_object_tagging(file_path: str, tags: dict[str, str]) -> bool:
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


async def _object_exists(file_path: str) -> bool:
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


async def _head_object_size(file_path: str) -> int | None:
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


@cache
def _presign_client() -> oss.Client:
    """presign 专用同步客户端（进程内复用）：presign 只做本地签名，不使用 HTTP 传输层。

    创建失败抛异常，不进缓存，下次调用重试。
    """
    client = create_oss_sync_client()
    if client is None:
        raise oss_client_create_failed_exception
    return client


def _presign_get_url(file_path: str, expires: int) -> str | None:
    """
    签名OSS文件下载URL

    presign 为纯本地签名计算（SDK 内部使用 _nopHttpClient，不发网络请求），
    在事件循环内同步执行即可，不需要线程或 Celery。

    :param file_path: OSS文件路径（如"folder/file.zip")
    :param expires: 过期时间（秒），由调用方传入

    :return: str 签名后的URL
    """
    try:
        # 获取bucket名称
        bucket_name = OSS_BUCKET_NAME
        if not bucket_name:
            logger.error("oss bucket name not set")
            return None

        # 生成预签名的URL
        pre_result = _presign_client().presign(
            GetObjectRequest(
                bucket=bucket_name,  # 指定存储空间名称
                key=file_path,        # 指定对象键名
                response_content_disposition="attachment",# 设置为强制下载
            ),
            expires=timedelta(seconds=expires),
        )
        # 配置 CDN 域名（正式服）时仅替换域名：V1 签名不含 Host，不影响 Signature；
        # 未配置（测试服）则原样返回 OSS 域名。OSS_CDN_DOMAIN 需带协议（如 https://cdn.neonexus.top）
        if OSS_CDN_DOMAIN:
            return f"{OSS_CDN_DOMAIN.rstrip('/')}/{pre_result.url.split('/', 3)[3]}"
        return pre_result.url
    except Exception as e: # pylint:disable = broad-exception-caught
        logger.error(f"failed to sign oss file: {e}")
        return None


async def _assume_role(policy: dict, role_session_name: str,
                       duration_seconds: int = 3600) -> dict:
    """调用 STS AssumeRole 签发临时凭证（SDK 原生异步接口）。"""
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
    response = await sts_client.assume_role_async(request)
    return response.body.credentials.to_map()


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
