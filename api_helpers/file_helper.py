#########################
# File Helper - 底层工具函数
# 职责：纯工具函数/存储IO,无业务逻辑,无数据库会话依赖
# 包含：文件校验、OSS 删除、OSS 回调签名校验
#########################
from __future__ import annotations

import base64
import hashlib
from pathlib import Path
from urllib.parse import unquote

from M2Crypto import RSA, BIO
from httpx import AsyncClient

from storage.oss.oss_storage import OssStorage
from background.file_background_helper import delete_oss_file_task
from models.user_models import UserInDB
from constants.file_constants import (
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
    ALLOWED_STORAGE_TYPES,
    MAX_PARTS_PER_UPLOAD,
)

from tools.logger import get_logger
logger = get_logger("api_helpers.file_helper")

# OSS回调公钥常量
OSS_CALLBACK_PUBLIC_KEY = None


########
# file 删
########
async def delete_multiple_files_by_storage(file_paths: list[str | Path], storage_type: str) -> int:
    """
    异步删除多个文件

    Args:
        file_paths: 文件路径列表

    Returns:
        成功删除的文件数量
    """

    deleted_count = 0
    for file_path in file_paths:
        if storage_type != "oss":
            raise NotImplementedError(f"Unsupported storage type: {storage_type}")
        delete_oss_file_task.delay(str(file_path)) # type: ignore # 直接调用任务队列删除避免阻塞
        deleted_count += 1
    return deleted_count


async def delete_file_by_storage(
    path: str | Path,
    storage_type: str,
    *,
    include_packup: bool = False,
) -> None:
    """
    删除 OSS 文件。保留 storage_type 参数以维持既有 service 调用契约。

    Args:
        include_packup: 是否连带删除同名的临时打包 zip 文件。
            仅删除用户上传的原始文件时需要——打包 zip 由下载流程产生、无独立
            DB 记录，不连带删除会残留为 OSS 孤儿对象；
            删除算法输出/图像目录/下载 zip 等场景不需要。

    异常处理：不捕获,让异常传播到调用方

    Raises:
        NotImplementedError: 不支持的存储类型
        OSError: 文件删除失败时
    """
    if storage_type != "oss":
        raise NotImplementedError(f"Unsupported storage type: {storage_type}")

    if include_packup:
        res = await OssStorage.delete_file_and_packup(str(path))
    else:
        res = await OssStorage.delete_file(str(path))
    if res is None:
        return
    elif res is False:
        raise OSError(f"Failed to delete file: {path}")

########
# file 查
########
def check_user_quota(user: UserInDB, file_size: int) -> bool:  #TODO 风险，后面加锁
    """
    检查用户配额是否足够

    综合检查：
    1. 文件数量是否超过限制
    2. 单个文件大小是否超过总限额
    3. 总存储空间是否超过限制

    Returns:
        True 表示所有配额检查通过,False 表示至少有一项超出限制
    """
    # 检查文件数量,单个文件大小,总存储空间（均从用户 DB 记录读取）
    # 注意：max_total_file_size 以 GB 存储，需转换为字节后与 file_size / total_file_size 比较
    BYTES_IN_GB = 1024**3 #type: ignore
    max_total_bytes = user.max_total_file_size * BYTES_IN_GB

    current_count = user.file_count
    projected_total_size = user.total_file_size + file_size
    if (current_count >= user.max_file_count
        or file_size > max_total_bytes
        or projected_total_size > max_total_bytes):
        logger.error(f"user quota exceeded user={user.id}:\n"
                       f"current_count={current_count}: limit={user.max_file_count}\n"
                       f"file_size={file_size}: limit={max_total_bytes}\n"
                       f"projected={projected_total_size}: limit={max_total_bytes}"
                       )
        return False

    return True


def validate_filename(filename: str) -> bool:
    """
    校验文件名安全性

    检查项：
    - 文件名非空
    - 长度不超过限制
    - 不包含路径遍历攻击字符(../, ..\\ 等)
    - 不包含危险字符(<, >, :, ", |, ?, *, 空字符)
    """
    if not filename or not filename.strip():
        return False

    if len(filename) > MAX_FILENAME_LENGTH:
        return False

    # 检查路径遍历攻击
    unsafe_patterns = ["../", "..\\", "/", "\\", ".."]
    for pattern in unsafe_patterns:
        if pattern in filename:
            return False

    # 检查危险字符
    dangerous_chars = ["<", ">", ":", '"', "|", "?", "*", "\x00"]
    for char in dangerous_chars:
        if char in filename:
            return False

    return True


def validate_file_type(file_type: str) -> bool:
    """
    校验文件类型是否在允许列表中

    直接检查 file_type 是否在 ALLOWED_FILE_TYPES 中
    """
    if not file_type:
        return False

    if file_type not in ALLOWED_FILE_TYPES:
        return False

    return True


def validate_total_parts(total_parts: int) -> bool:
    """
    验证分片数量是否合法

    Returns:
        True 表示分片数量合法,False 表示不合法
    """
    return 0 < total_parts <= MAX_PARTS_PER_UPLOAD


def validate_file_metadata(filename: str, file_type: str) -> bool:
    """
    验证文件元数据(文件名和类型)

    Args:
        filename: 文件名（无后缀）
        file_type: 文件类型

    Returns:
        True 表示验证通过,False 表示验证失败
    """
    return validate_filename(filename) and validate_file_type(file_type)


def validate_upload_params(
    storage_type: str,
    filename: str,
    expected_storage_type: str | None = None,
    expected_filename: str | None = None,
) -> bool:
    """
    验证上传参数

    Returns:
        True 表示验证通过,False 表示验证失败
    """
    # 1. 存储类型验证
    if storage_type not in ALLOWED_STORAGE_TYPES:
        return False

    # 2. 提供了预检的存储类型,进行比较
    if expected_storage_type and storage_type != expected_storage_type:
        return False

    # 3. 文件名验证
    if not filename:
        return False

    # 4. 提供了预检的文件名,进行比较
    if expected_filename and filename != expected_filename:
        return False

    # 5. 文件名验证（类型验证需要 file_type 参数，此处跳过）
    if not validate_filename(filename):
        return False

    return True


async def check_oss_callback_signature(
        authorization: str,
        oss_pub_key_url: str,
        raw_body: str,
        callback_path: str,
        query: str = "",
    ) -> bool:
    """
    检查OSS回调签名是否有效

    Args:
        authorization: OSS回调签名
        oss_pub_key_url: OSS公钥URL
        raw_body: OSS回调原始体
        callback_path: OSS回调路径
        query: OSS回调查询参数

    Returns:
        True 表示签名有效,False 表示无效
    """

    global OSS_CALLBACK_PUBLIC_KEY # pylint: disable=W0603
    if not OSS_CALLBACK_PUBLIC_KEY:
        pub_key_url = base64.b64decode(oss_pub_key_url).decode()
        if (not pub_key_url.startswith("http://gosspublic.alicdn.com/")
            and not pub_key_url.startswith("https://gosspublic.alicdn.com/")):
            logger.error("oss_pub_key_url not valid")
            return False
        async with AsyncClient() as client:
            res = await client.get(pub_key_url, timeout=5)
            res.raise_for_status()
            OSS_CALLBACK_PUBLIC_KEY = res.read()

    authorization_bytes = base64.b64decode(authorization)
    sign_str = unquote(callback_path) + (f"?{query}" if query else "") + '\n' + raw_body
    sign_str_md5 = hashlib.md5(sign_str.encode()).digest()

    bio = BIO.MemoryBuffer(OSS_CALLBACK_PUBLIC_KEY)
    rsa_pub = RSA.load_pub_key_bio(bio)
    try:
        result = rsa_pub.verify(sign_str_md5, authorization_bytes, 'md5')
    except Exception as e: # pylint: disable=W0718
        logger.error(
            "oss_callback_signature verify failed,\nerror: %s," \
            "sign_str: %s, pub_key: %s, authorization: %s, raw_body: %s",
            e, sign_str, OSS_CALLBACK_PUBLIC_KEY, authorization, raw_body
        )
        return False

    if not result:
        logger.error(
            "oss_callback_signature verify failed,\n" \
            "sign_str: %s, pub_key: %s, authorization: %s, raw_body: %s",
            sign_str, OSS_CALLBACK_PUBLIC_KEY, authorization, raw_body
        )
        return False

    return True
