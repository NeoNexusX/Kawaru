# -*- coding: utf-8 -*-
"""
声明：
这个函数针对文件和文件夹命名编码是如下格式：
1. mac/linux 系统， 默认是utf-8
2. windows 系统， 默认是gb2312， 也可以是utf-8

对于其他编码，我们这里尝试使用chardet这个库进行编码判断，但是这个并不能保证100% 正确，
建议用户先调试函数，如果有必要改写这个函数，并保证调试通过

Statement:
This function names and encodes files and folders as follows:
1. MAC/Linux system, default is utf-8
2. For Windows, the default is gb2312 or utf-8

For other encodings, we try to use the chardet library for coding judgment here, 
but this is not guaranteed to be 100% correct. 
If necessary to rewrite this function, and ensure that the debugging pass
"""

import json
import os
import asyncio
import zipfile
from pathlib import Path
import chardet
import oss2

from helper import (
    LOGGER,
    extract_zip_and_compute_hash,
    get_expected_hash,
    oss_upload_callback,
    PROCESSED_DIR,
)

# 小于此阈值的 zip 文件会下载到 FC 本地硬盘 /tmp/，再解压 + 计算哈希
FC_LOCAL_STORAGE_THRESHOLD_MB = int(os.environ.get("FC_LOCAL_STORAGE_THRESHOLD_MB", "8192"))
# 超过此阈值的 zip 文件 FC 不处理，直接跳过
FC_MAX_FILE_SIZE_MB = int(os.environ.get("FC_MAX_FILE_SIZE_MB", "20480"))
# NAS 挂载路径，用于存放中等大小的 zip 文件（超过 /tmp/ 阈值但未超过最大限制）
NAS_STORAGE_PATH = os.environ.get("NAS_STORAGE_PATH", "/home/app")
    

# pylint: disable=invalid-name

def parse_object_key(object_name: str) -> tuple[int, str, str]:
    """从 OSS 对象键解析业务标识。

    对象键约定（由后端签发 STS 时生成）：
        data/{file_verify_code}/{file_id}.zip
    verify_code 前置将键打散到随机前缀，避免自增 file_id 造成 OSS 分区热点；
    file_id 在文件名中，据此解析。

    Returns:
        (file_id, file_verify_code, zip_name)

    Raises:
        RuntimeError: 对象键格式不符合约定或 file_id 非数字
    """
    parts = object_name.split("/")
    if len(parts) != 3 or not parts[2].endswith(".zip"):
        raise RuntimeError(f"invalid object key format, expect data/{{file_verify_code}}/{{file_id}}.zip: {object_name}")
    try:
        file_id = int(Path(parts[2]).stem)
    except ValueError as e:
        raise RuntimeError(f"invalid file_id in object key: {object_name}") from e
    return file_id, parts[1], parts[2]

def get_zipfile_name(origin_name):
    """解决中文乱码问题"""
    name = origin_name
    try:
        name_bytes = origin_name.encode(encoding="cp437")
    except Exception: # pylint: disable=W0718
        name_bytes = origin_name.encode(encoding="utf-8")

    # the string to be detect is long enough, the detection result accuracy is higher
    detect = chardet.detect(name_bytes)
    confidence = detect["confidence"]
    detect_encoding = detect["encoding"]
    if confidence > 0.75 and (
        detect_encoding.lower() in ["gb2312", "gbk", "gb18030", "ascii", "utf-8"]
    ):
        try:
            if detect_encoding.lower() in ["gb2312", "gbk", "gb18030"]:
                detect_encoding = "gb18030"
            name = name_bytes.decode(detect_encoding)
        except Exception: # pylint: disable=W0718
            name = name_bytes.decode(encoding="gb18030")
    else:
        try:
            name = name_bytes.decode(encoding="gb18030")
        except Exception: # pylint: disable=W0718
            name = name_bytes.decode(encoding="utf-8")
    # fix windows \\ as dir segment
    name = name.replace("\\", "/")
    return name

def handler(event, context):
    asyncio.run(_async_handler(event, context))

async def _async_handler(event, context):
    """
    The object from OSS will be decompressed automatically .
    param: event:   The OSS event json string. Including oss object uri and other information.
        For detail info, please refer https://help.aliyun.com/document_detail/70140.html?spm=a2c4g.11186623.6.578.5eb8cc74AJCA9p#OSS

    param: context: The function context, including credential and runtime info.

        For detail info, please refer to https://help.aliyun.com/document_detail/56316.html#using-context
    """
    # 获取配置信息

    
    evt_lst = json.loads(event)
    # 新版运行时（Python 3.12 / FC 3.0）不再提供 context.credentials，
    # 临时凭证由函数配置的服务角色自动注入为以下环境变量
    access_key_id = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    security_token = os.environ.get("ALIBABA_CLOUD_SECURITY_TOKEN")
    if not (access_key_id and access_key_secret and security_token):
        raise RuntimeError(
            "cannot get STS credentials from environment: configure a service role "
            "for the FC function so that ALIBABA_CLOUD_ACCESS_KEY_ID / "
            "ALIBABA_CLOUD_ACCESS_KEY_SECRET / ALIBABA_CLOUD_SECURITY_TOKEN are injected"
        )
    auth = oss2.StsAuth(access_key_id, access_key_secret, security_token)

    evt = evt_lst["events"][0]
    bucket_name = evt["oss"]["bucket"]["name"]
    endpoint = "oss-" + evt["region"] + "-internal.aliyuncs.com"
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    object_name = evt["oss"]["object"]["key"]
    object_sizeMB = evt["oss"]["object"]["size"] / 1024 / 1024 # pylint: disable=C0103
    LOGGER.info(f"{object_name,} size is = {object_sizeMB}MB") # pylint: disable=W1203


    file_type = os.path.splitext(object_name)[1]
    if file_type != ".zip":
        raise RuntimeError(f"{object_name} filetype is not zip")

    if "ObjectCreated:PutSymlink" == evt["eventName"]:
        object_name = bucket.get_symlink(object_name).target_key
        if object_name == "":
            raise RuntimeError(f'{evt["oss"]["object"]["key"]} is invalid symlink file')

    file_type = os.path.splitext(object_name)[1]
    if file_type != ".zip":
        raise RuntimeError(f"{object_name} filetype is not zip")

    # 获取实际文件大小（处理 symlink 后 object_name 可能已变，需重新获取大小）
    zip_file_size_mb = bucket.head_object(object_name).content_length / 1024 / 1024
    LOGGER.info(f"{object_name} size = {zip_file_size_mb:.2f}MB")

    # 对象键约定为 data/{file_verify_code}/{file_id}.zip，从中解析业务标识；
    # STS 上传凭证限定了对象键只能是后端签发的格式
    file_id, expected_verify_code, zip_name = parse_object_key(object_name)

    # 解压结果通过 OSS SDK 分片直传到：
    # origin/{file_verify_code}/{file_id}/...
    # 注意：不写入 ossfs 挂载盘（大文件经 FUSE write-back 写入会静默损坏）
    newKeyPrefix = os.path.join(PROCESSED_DIR, expected_verify_code, str(file_id))

    # 根据文件大小决定 zip 文件的读取位置
    tmp_zip_path = None  # 用于追踪 /tmp/ 或 NAS 下的临时文件，以便后续清理
    file_path = object_name

    if zip_file_size_mb > FC_MAX_FILE_SIZE_MB:
        # 超大文件：FC 不处理，回调通知后端后跳过
        LOGGER.warning(
            f"zip file size {zip_file_size_mb:.2f}MB exceeds max limit {FC_MAX_FILE_SIZE_MB}MB, "
            f"skipping: {object_name}"
        )
        await oss_upload_callback(
            file_id, None, False, None, None, None, None, None, "FileTooLarge"
        )
        return
    elif zip_file_size_mb <= FC_LOCAL_STORAGE_THRESHOLD_MB:
        # 小文件：下载到 FC 本地硬盘 /tmp/，本地 SSD 读取更快
        tmp_zip_path = f"/tmp/{zip_name}"
        LOGGER.info(f"zip file under local threshold, downloading to local: {tmp_zip_path}")
    else:
        # 中等文件：下载到 NAS 挂载盘
        os.makedirs(NAS_STORAGE_PATH, exist_ok=True)
        tmp_zip_path = f"{NAS_STORAGE_PATH}/{zip_name}"
        LOGGER.info(f"zip file between local threshold and max limit, downloading to NAS: {tmp_zip_path}")

    # 构建回调信息
    file_verify_code = None
    is_success = False
    error_type = None
    imzml_key, ibd_key, imzml_size, ibd_size = None, None, None, None

    try:
        # 下载 zip 文件到本地或 NAS（放在 try 块内，确保失败时能清理和回调）
        bucket.get_object_to_file(object_name, tmp_zip_path)
        zip_file_path = tmp_zip_path

        # 解压并同时计算哈希，通过 OSS SDK 分片直传（数据只读取一遍，不落盘）
        LOGGER.info(f"start to decompress, compute hash and upload to OSS, zip = {object_name}") # pylint: disable=W1203
        imzml_key, ibd_key, file_verify_code, imzml_size, ibd_size = \
            extract_zip_and_compute_hash(bucket, Path(zip_file_path), newKeyPrefix)
        LOGGER.info(f"decompress, hash and upload done, verifying against expected hash")

        expected_hash = await get_expected_hash(file_id)

        if file_verify_code != expected_hash:
            LOGGER.error(f"file verify code not equal to expected,\n"
                         f"file_verify_code={file_verify_code}, expected_hash={expected_hash}")
            raise ValueError("file verify code is not equal to expected hash")

        is_success = True
    except (zipfile.BadZipFile, FileNotFoundError) as e:
        is_success = False
        if isinstance(e, zipfile.BadZipFile):
            LOGGER.error("invalid zip file: %s", file_path)
            error_type = "BadZipFile"
        elif isinstance(e, FileNotFoundError):
            LOGGER.error("invalid imzml file format: %s", file_path)
            error_type = "FileNotFoundError"

        imzml_key, ibd_key, imzml_size, ibd_size = None, None, None, None
    except Exception as e:
        LOGGER.error("catch other error: %s", e)
        error_type = str(e)
        is_success = False

        imzml_key, ibd_key, imzml_size, ibd_size = None, None, None, None

    finally:
        # 清理 /tmp/ 或 NAS 下的临时 zip 文件
        if tmp_zip_path and os.path.exists(tmp_zip_path):
            os.remove(tmp_zip_path)
            LOGGER.info(f"cleaned up tmp zip: {tmp_zip_path}")

        LOGGER.info("start to callback oss, params are:")
        LOGGER.info(
            f"file_id: {file_id}, file_path: {file_path}, file_verify_code: {file_verify_code}, is_success: {is_success},\n"
            f"imzml_key: {imzml_key}, ibd_key: {ibd_key}, imzml_size: {imzml_size}, "
            f"ibd_size: {ibd_size}, error_type: {error_type}"
        )
        await oss_upload_callback(
            file_id, file_path, is_success, imzml_key,
            ibd_key, imzml_size, ibd_size, file_verify_code, error_type
        )
