#########################
# File API Helper - 底层工具函数
# 职责：纯工具函数,无业务逻辑,无数据库依赖
# 包含：文件校验、路径准备、文件IO操作
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
from fastapi import UploadFile
from sqlalchemy.orm import selectinload
from sqlmodel import and_, exists, select
from sqlmodel.ext.asyncio.session import AsyncSession

from storage_backend.storage import LocalStorage, OssStorage
from background.file_background_helper import delete_oss_file_task
from models.file_models import FileInDB, UserFileLink
from models.user_models import UserInDB
from exceptions import file_not_found_exception
from constants.file_constants import (
    TEMP_PARTS_ROOT,
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
    MAX_FILES_PER_USER,
    MAX_TOTAL_STORAGE_PER_USER,
    CHUNK_BUFFER_SIZE,
    MAX_FILE_SIZE,
    ALLOWED_STORAGE_TYPES,
    MAX_PARTS_PER_UPLOAD,
)

from tools.logger import get_logger
logger = get_logger("api.file_api_helper")

# OSS回调公钥常量
OSS_CALLBACK_PUBLIC_KEY = None

def prepare_part_temp_path(owner_id: int, file_hash: str, part_number: int) -> Path:
    """
    为分片上传准备临时存储路径

    使用 user_{owner_id}_{file_hash}_part_{part_number} 格式命名
    避免不同用户、不同文件的分片冲突
    2026.3.3 Neo checked
    """
    TEMP_PARTS_ROOT.mkdir(parents=True, exist_ok=True)
    part_filename = f"user_{owner_id}_{file_hash}_part_{part_number:06d}"
    return TEMP_PARTS_ROOT / part_filename


########
# file 增
########
async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    将上传文件保存到指定路径
    使用 aiofiles 异步写入,分片读取避免内存占用过大 写入完成后关闭上传文件句柄

        2026.3.3 Neo checked
    """
    await upload_file.seek(0)
    async with aiofiles.open(destination, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            await buffer.write(chunk)
    await upload_file.close()


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

    Args:
        file_record: 文件记录对象(FileInDB)

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
    2. 单个文件大小是否超过限制
    3. 总存储空间是否超过限制

    Returns:
        True 表示所有配额检查通过,False 表示至少有一项超出限制
    """
    # 检查文件数量,单个文件大小,总存储空间
    current_count = user.file_count
    projected_total_size = user.total_file_size + file_size
    if current_count >= MAX_FILES_PER_USER or file_size > MAX_FILE_SIZE or projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        logger.error(f"user quota exceeded user={user.id}:\n"
                       f"current={current_count}: limit={MAX_FILES_PER_USER},{current_count >= MAX_FILES_PER_USER}\n"
                       f"current={file_size}: limit={MAX_FILE_SIZE},{file_size > MAX_FILE_SIZE}\n"
                       f"current={projected_total_size}: limit={MAX_TOTAL_STORAGE_PER_USER},{projected_total_size > MAX_TOTAL_STORAGE_PER_USER}"
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

    Args:
        storage_type: 存储类型
        filename: 文件名
        expected_storage_type: 期望的存储类型
        expected_filename: 期望的文件名

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


async def valid_duplicate_filename(
    async_session: AsyncSession, user: UserInDB, filename: str
) -> bool:
    """
    检查用户是否已有同名文件

    事务管理：只读查询,无需 commit
    异常处理：不抛出异常,返回检查结果

    Returns:
        True 表示存在重复文件名,False 表示不重复
    """

    # 通过 SQL 的 exists() 直接判断数据库是否存在重复文件名文件
    stmt = select(
        exists().where(
            and_(
                UserFileLink.user_id == user.id,
                FileInDB.file_id == UserFileLink.file_id,
                FileInDB.filename == filename,
            )
        )
    )

    has_duplicate_file = (await async_session.exec(stmt)).one()

    if has_duplicate_file:
        logger.warning(
            "duplicate filename found user=%s filename=%s",
            user.id,
            filename,
        )
        return True

    return False


async def valid_user_duplicate_file_hash(
    session: AsyncSession, user: UserInDB, hash_md5: str
) -> bool:
    """
    检查用户是否已拥有该哈希的文件(只针对本用户)

    Returns:
        True 表示用户已拥有该文件,False 表示未拥有
    """

    # 通过预加载获取当前用户文件列表中和传入哈希相同的文件列表
    # 直接 exist 子句高效获取布尔值
    has_same_hash_file = (await session.exec(
        select(
            exists().where(
                and_(
                    (UserFileLink.user_id == user.id),
                    (UserFileLink.file_id == FileInDB.file_id),
                    (FileInDB.file_verify_code == hash_md5)
                )
            )
        )
    )).one()

    if has_same_hash_file:
        logger.warning(
            "duplicate file hash found user=%s hash=%s",
            user.id,
            hash_md5,
        )
        return True

    return False


async def get_file_record_by_id(
    file_id: int,
    async_session: AsyncSession,
    load_parts: bool = False
) -> FileInDB:
    """
    根据文件ID查询文件记录

    可选参数:
    load_parts: bool 是否同时加载分片信息

    Returns:
        文件记录对象,不存在则返回 None
    Raises:
        file_not_found_exception: 文件记录不存在

    2026.3.3 Neo checked
    """
    statement = select(FileInDB).where(FileInDB.file_id == file_id)

    if load_parts:
        statement = statement.options(selectinload(FileInDB.parts)) # type: ignore
    file_record = (await async_session.exec(statement)).first()

    if not file_record:
        logger.warning("file record not found file_id=%s", file_id)
        raise file_not_found_exception

    return file_record


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