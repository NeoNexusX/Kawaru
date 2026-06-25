#########################
# File Helper - 底层工具函数
# 职责：纯工具函数/存储IO,无业务逻辑,无数据库会话依赖
# 包含：文件校验、路径准备、文件IO操作、按存储类型删除、OSS 回调签名校验
#########################
from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path
from urllib.parse import unquote

import aiofiles
from M2Crypto import RSA, BIO
from httpx import AsyncClient

from storage_backend.storage import LocalStorage, OssStorage
from background.file_background_helper import delete_oss_file_task
from models.user_models import UserInDB
from constants.file_constants import (
    TEMP_PARTS_ROOT,
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
    CHUNK_BUFFER_SIZE,
    ALLOWED_STORAGE_TYPES,
    MAX_PARTS_PER_UPLOAD,
)

from tools.logger import get_logger
logger = get_logger("api_helpers.file_helper")

# OSS回调公钥常量
OSS_CALLBACK_PUBLIC_KEY = None


def prepare_part_temp_path(owner_id: int, file_hash: str, part_number: int) -> Path:
    """
    为分片上传准备临时存储路径

    使用 user_{owner_id}_{file_hash}_part_{part_number} 格式命名
    避免不同用户、不同文件的分片冲突
    """
    TEMP_PARTS_ROOT.mkdir(parents=True, exist_ok=True)
    part_filename = f"user_{owner_id}_{file_hash}_part_{part_number:06d}"
    return TEMP_PARTS_ROOT / part_filename


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
        if storage_type == "local":
            await asyncio.to_thread(lambda p=file_path: LocalStorage.delete_file(p))
        elif storage_type == "oss":
            delete_oss_file_task.delay(str(file_path)) # type: ignore # 直接调用任务队列删除避免阻塞
        else:
            raise NotImplementedError(f"Unsupported storage type: {storage_type}")
        deleted_count += 1
    return deleted_count


async def delete_file_by_storage(path: str | Path, storage_type: str) -> None:
    """
    根据存储类型删除文件

    目前仅支持 local 和 oss 存储类型

    异常处理：不捕获,让异常传播到调用方

    Raises:
        NotImplementedError: 不支持的存储类型
        OSError: 文件删除失败时
    """
    if storage_type == "local":
        LocalStorage.delete_file(path)

    elif storage_type == "oss":
        res = await OssStorage.delete_file(str(path))
        if res is None:
            return
        if not res:
            raise OSError(f"Failed to delete oss file: {str(path)}")

    else:
        raise NotImplementedError(f"Unsupported storage type: {storage_type}")


########
# file 查
########
async def merge_files_and_get_hash(
    source_files: list[Path],
    destination: Path,
    expected_hash: str | None = None,
    buffer_size: int = CHUNK_BUFFER_SIZE,
) -> str:

    """
    合并多个文件并计算哈希

    Args:
        source_files: 源文件列表(按顺序)
        destination: 目标文件路径
        expected_hash: 期望的哈希值(可选),如果提供则验证
        buffer_size: 读取缓冲区大小

    Returns:
        合并后文件的哈希值

    Raises:
        FileNotFoundError: 源文件不存在
        ValueError: 哈希不匹配(如果提供了 expected_hash)
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(destination, "wb") as dest_file:
        for source_file in source_files:
            if not source_file.exists():
                raise FileNotFoundError(f"Source file not found: {source_file}")

            async with aiofiles.open(source_file, "rb") as src_file:
                while True:
                    data = await src_file.read(buffer_size)
                    if not data:
                        break
                    await dest_file.write(data)

    final_hash, _ = await LocalStorage.compute_upload_file_hash(str(destination))

    if expected_hash and final_hash != expected_hash:
        destination.unlink(missing_ok=True)
        raise ValueError(f"Hash mismatch: expected={expected_hash}, actual={final_hash}")

    return final_hash


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
