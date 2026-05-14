#########################
# File Background Helper - 后台任务辅助函数
# 职责：低级后台任务函数，避免循环导入
#########################
from __future__ import annotations

import os
import json
import datetime
import zipfile
import hashlib
import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import HTTPException, status
import aiofiles

from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config as CredentialConfig
from alibabacloud_sts20150401.client import Client as StsClient
from alibabacloud_sts20150401.models import AssumeRoleRequest
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_oss_v2 import (HeadObjectRequest, DeleteObjectRequest, ListObjectsV2Request,
                                  Tag, PutObjectTaggingRequest, Tagging, TagSet)

from celery_runtime.state import get_celery_app

from storage_backend.oss_client import create_oss_client_context

from constants.file_constants import (
    CHUNK_BUFFER_SIZE, OSS_REGION_ID, OSS_BUCKET_NAME,
    OSS_DEFAULT_TIMEOUT_SECONDS
)

from tools.logger import get_logger

logger = get_logger("background.file_background")
celery_app = get_celery_app()


@celery_app.task()  # type: ignore
async def build_zip_file_background(
    files: list[tuple[str, str]],
    output_path: str,
    compression: int | None = None,
) -> int:
    """
    后台创建ZIP文件任务

    参数已转换为可序列化类型：
      - files: list[tuple[file_path_str, arc_name]]
      - output_path: str
    """
    if compression is None:
        compression = zipfile.ZIP_DEFLATED

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    # 从可序列化参数转换为 Path
    files_path = [(Path(fp), arc) for fp, arc in files]
    output_str = str(output)

    with zipfile.ZipFile(output_str, "w", compression) as zf:
        for file_path, arc_name in files_path:
            if not file_path.exists():
                raise FileNotFoundError(f"Source file not found: {file_path}")
            zf.write(file_path, arc_name)

    logger.info(f"ZIP file created at {output_str}")
    return output.stat().st_size


@celery_app.task # type: ignore
async def compute_upload_local_file_hash_task(file_path: str) -> tuple[str, int]:
    """
    计算上传文件元数据的本地版本(md值和大小)
    分片读取指定本地路径的文件内容,避免一次性加载到内存
    """
    md5 = hashlib.md5()  # 使用MD5算法计算哈希值
    size = 0
    async with aiofiles.open(file=file_path, mode='rb') as file:
        await file.seek(0)
        while True:
            chunk = await file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            md5.update(chunk)
            size += len(chunk)
    logger.info(f"computed hash={md5.hexdigest()} size={size} for file={file_path}")
    return md5.hexdigest(), size


@celery_app.task()
async def validate_oss_file_task(file_path: str, size: int, file_verify_code: str) -> bool:
    """验证OSS文件，存在且信息匹配返回True，不存在或信息不匹配则返回False"""
    with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                raise HTTPException(status_code=500, detail="oss client create failed")

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                raise HTTPException(status_code=500, detail="oss bucket name not set")

            # 发送请求
            req = HeadObjectRequest(bucket=bucket_name, key=file_path) # type: ignore
            meta = await oss_client.head_object(req)

            # 检查文件是否存在
            if meta.status_code != status.HTTP_200_OK:
                logger.warning(f"oss file {file_path} not exist")
                return False

            # 检查文件大小是否一致
            actual_size = meta.content_length
            if actual_size != size:
                logger.warning(f"oss file {file_path} size not match, expect {size}, but actual {actual_size}")
                return False

            # 轮询获取oss端计算的file-verify-code
            meta_dict = meta.headers
            timeout_seconds = OSS_DEFAULT_TIMEOUT_SECONDS
            assert timeout_seconds is not None
            start_time = datetime.now()
            while True:
                await asyncio.sleep(1)
                meta = await oss_client.head_object(req)
                meta_dict = meta.headers
                if "x-oss-meta-file-verify-code" in meta_dict:
                    break
                now_time = datetime.now()
                if (now_time - start_time).total_seconds() > float(timeout_seconds):
                    logger.warning(f"verification for oss {file_path} file verify code timeout after {timeout_seconds} seconds")
                    return False

            # 检查file-verify-code是否匹配
            actual_verify_code = meta_dict["x-oss-meta-file-verify-code"]
            if actual_verify_code != file_verify_code:
                logger.warning(f"oss file {file_path} verify code not match, expect {file_verify_code}, but actual {actual_verify_code}")
                return False

            return True

        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to validate oss file: {e}")
            return False


@celery_app.task()
async def tag_oss_file_task(file_path: str, tags: dict[str, str]) -> bool:
    """为OSS文件添加标签"""
    with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                raise HTTPException(status_code=500, detail="oss client create failed")

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                raise HTTPException(status_code=500, detail="oss bucket name not set")

            # 构造tagging
            tag_set: list[Tag] = []
            for key, value in tags.items():
                tag_set.append(Tag(key=key, value=value))
            tagging = Tagging(tag_set=TagSet(tag_set))

            # 发送请求
            req = PutObjectTaggingRequest(bucket=bucket_name, key=file_path, tagging=tagging) # type: ignore
            res = await oss_client.put_object_tagging(req)
            if res.status_code != status.HTTP_200_OK:
                logger.warning(f"oss file {file_path} tag failed, status code {res.status_code}")
                return False
            return True
        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to tag oss file: {e}")
            return False


@celery_app.task()
async def check_oss_file_exist_task(file_path: str) -> bool:
    """验证OSS文件或目录是否存在，存在返回True，不存在则返回False"""
    with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                raise HTTPException(status_code=500, detail="oss client create failed")

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                raise HTTPException(status_code=500, detail="oss bucket name not set")

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
                if meta.status_code != status.HTTP_200_OK:
                    logger.warning(f"oss file {file_path} not exist")
                    return False

            return True

        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to check oss file: {e}")
            return False


@celery_app.task()
async def get_oss_file_size_task(file_path: str) -> int | None:
    """
    获取OSS文件大小

    :param file_path: OSS文件路径（如"folder/file.zip")
    :return: 文件大小
    """
    with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                raise HTTPException(status_code=500, detail="oss client create failed")

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                raise HTTPException(status_code=500, detail="oss bucket name not set")

            # 发送请求
            req = HeadObjectRequest(bucket=bucket_name, key=file_path) # type: ignore
            meta = await oss_client.head_object(req)

            # 检查文件是否存在
            if meta.status_code != status.HTTP_200_OK:
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
    role_session_name = f"upload-{str(Path(file_path).stem)}-{datetime.now().timestamp()}"

    # 调用STS API获取上传STS Token
    result = _get_oss_sts_token(policy, role_session_name)

    return result


@celery_app.task()
def get_oss_packup_sts_token_task(packup_folder: str, zip_file_path: str) -> dict:
    """
    获取OSS打包STS Token

    :param experiment_folder: 要打包的oss文件夹路径（如"experiment/aaa/")
    :param zip_file_path: 打包后的zip文件路径（如"data/aaa.zip")
    """

    # 构建STS策略
    policy = {
        "Version": "1",
        "Statement": [
            {
                # 针对实验文件夹下的所有文件读取权限
                "Effect": "Allow",
                "Action": ["oss:GetObject"],
                "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}/{packup_folder.strip('/')}/*"]
            },
            {
                # 针对Bucket的文件列举权限
                "Effect": "Allow",
                "Action": ["oss:ListObjects"],
                "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}"]
            },
            {
                # 针对zip文件写入权限
                "Effect": "Allow",
                "Action": ["oss:PutObject", "oss:GetObject"],
                "Resource": [f"acs:oss:*:*:{OSS_BUCKET_NAME}/{zip_file_path.rstrip('/')}"]
            }
        ]
    }

    # 构建role_session_name
    role_session_name = f"packup-{str(Path(packup_folder).stem)}-{datetime.now().timestamp()}"

    # 调用STS API获取打包STS Token
    result = _get_oss_sts_token(policy, role_session_name)

    return result


def _get_oss_sts_token(policy: dict, role_session_name: str) -> dict:
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
        duration_seconds=3600
    )

    # 发送请求并获取响应
    response = sts_client.assume_role(request)
    result = response.body.credentials.to_map()

    return result


@celery_app.task()
async def delete_oss_file_task(file_path: str) -> bool | None:
    """
    删除OSS文件

    :param file_path: OSS文件路径（如"folder/file.zip")

    :return: bool 是否删除成功
    """
    with create_oss_client_context() as oss_client:
        try:
            # 创建OSS客户端
            if oss_client is None:
                raise HTTPException(status_code=500, detail="oss client create failed")

            # 获取bucket名称
            bucket_name = OSS_BUCKET_NAME
            if not bucket_name:
                raise HTTPException(status_code=500, detail="oss bucket name not set")

            keys = []
            err_count = 0
            if file_path.endswith('/'): # 针对文件夹进行处理
                req = ListObjectsV2Request(bucket=bucket_name, prefix=file_path) # type: ignore
                res = await oss_client.list_objects_v2(req)
                 # 提取所有匹配对象的 key
                keys = [obj.key for obj in res.contents] if res.contents else []

            if not keys:
                keys = [file_path]
            for key in keys:
                req = DeleteObjectRequest(bucket=bucket_name, key=key) # type: ignore
                res = await oss_client.delete_object(req)
                if res.status_code != status.HTTP_204_NO_CONTENT:
                    logger.error(f"oss file {key} deleted failed")
                    err_count += 1
                    continue
                logger.info(f"oss file {key} deleted")
            if err_count > 0:
                return False

            return True
        except Exception as e: # pylint:disable = broad-exception-caught
            logger.error(f"failed to delete oss file: {e}")
            return False
