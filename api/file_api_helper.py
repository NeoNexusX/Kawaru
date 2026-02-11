#########################
# File API Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：文件校验、路径准备、文件IO操作
#########################
from __future__ import annotations

import zipfile
import asyncio
import hashlib
from pathlib import Path

import aiofiles
from fastapi import UploadFile

from models.user_models import UserInDB
from constants.file_constants import (
    STORAGE_ROOT,
    LOCAL_STORAGE_ROOT,
    TEMP_CHUNKS_ROOT,
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
    ALLOWED_EXPERIMENT_TYPES,
    MAX_FILES_PER_USER,
    MAX_TOTAL_STORAGE_PER_USER,
    CHUNK_BUFFER_SIZE,
    MAX_FILE_SIZE,
    ALLOWED_STORAGE_TYPES,
    MAX_CHUNKS_PER_UPLOAD,
)

from logger import get_logger

logger = get_logger("main.file_api_helper")


def validate_file_metadata(filename: str) -> bool:
    """
    验证文件元数据（文件名和类型）

    Args:
        filename: 文件名

    Returns:
        True 表示验证通过，False 表示验证失败
    """
    return validate_filename(filename) and validate_file_type(filename)


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
        True 表示验证通过，False 表示验证失败
    """
    # 1. 存储类型验证
    if storage_type not in ALLOWED_STORAGE_TYPES:
        return False

    # 2. 提供了预检的存储类型，进行比较
    if expected_storage_type and storage_type != expected_storage_type:
        return False

    # 3. 文件名验证
    if not filename:
        return False

    # 4. 提供了预检的文件名，进行比较
    if expected_filename and filename != expected_filename:
        return False

    # 5. 文件名和类型验证
    if not validate_file_metadata(filename):
        return False

    return True


async def build_zip_file(#todotodo
    files: list[tuple[Path, str]],
    output_path: Path,
    compression: int | None = None,
) -> Path:
    """
    创建ZIP文件（通用工具函数）

    Args:
        files: 文件列表，每个元素是 (文件路径, ZIP中的文件名) 元组
        output_path: 输出ZIP文件路径
        compression: 压缩级别，默认为 ZIP_DEFLATED

    Returns:
        创建的ZIP文件路径

    Raises:
        FileNotFoundError: 源文件不存在
    """

    if compression is None:
        compression = zipfile.ZIP_DEFLATED

    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _create_zip() -> None:
        with zipfile.ZipFile(output_path, "w", compression) as zf:
            for file_path, arc_name in files:
                if not file_path.exists():
                    raise FileNotFoundError(f"Source file not found: {file_path}")
                zf.write(file_path, arc_name)

    await asyncio.to_thread(_create_zip)
    return output_path


async def merge_files_with_hash(
    source_files: list[Path],
    destination: Path,
    expected_hash: str | None = None,
    buffer_size: int = CHUNK_BUFFER_SIZE,
) -> str:
    """
    合并多个文件并计算哈希

    Args:
        source_files: 源文件列表（按顺序）
        destination: 目标文件路径
        expected_hash: 期望的哈希值（可选），如果提供则验证
        buffer_size: 读取缓冲区大小

    Returns:
        合并后文件的哈希值

    Raises:
        FileNotFoundError: 源文件不存在
        ValueError: 哈希不匹配（如果提供了 expected_hash）
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    total_hash = hashlib.sha256()
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
                    total_hash.update(data)

    final_hash = total_hash.hexdigest()

    if expected_hash and final_hash != expected_hash:
        destination.unlink(missing_ok=True)
        raise ValueError(f"Hash mismatch: expected={expected_hash}, actual={final_hash}")

    return final_hash


async def delete_files_async(file_paths: list[str | Path]) -> int:
    """
    异步删除多个文件

    Args:
        file_paths: 文件路径列表

    Returns:
        成功删除的文件数量
    """

    deleted_count = 0
    for file_path in file_paths:
        await asyncio.to_thread(lambda p=file_path: delete_local_file(p))
        deleted_count += 1
    return deleted_count


def ensure_storage_dirs() -> None:
    """
    确保存储目录存在
    在上传操作前调用，创建必要的存储目录结构
    """
    for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_CHUNKS_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def validate_filename(filename: str) -> bool:
    """
    校验文件名安全性

    检查项：
    - 文件名非空
    - 长度不超过限制
    - 不包含路径遍历攻击字符（../, ..\\ 等）
    - 不包含危险字符（<, >, :, ", |, ?, *, 空字符）
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


def validate_file_type(filename: str) -> bool:
    """
    校验文件类型是否在允许列表中

    通过文件扩展名判断，必须有扩展名且在 ALLOWED_FILE_TYPES 中
    """
    if not filename:
        return False

    file_ext = Path(filename).suffix.lstrip(".")

    if not file_ext:
        return False

    if file_ext not in ALLOWED_FILE_TYPES:
        return False

    return True


def validate_experiment_type(experiment_type: str) -> bool:
    """
    校验实验类型是否在允许列表中

    Args:
        experiment_type: 实验类型（如 imzML、raw 等）

    Returns:
        True 表示验证通过，False 表示验证失败
    """
    if not experiment_type:
        return False

    if experiment_type not in ALLOWED_EXPERIMENT_TYPES:
        return False

    return True

def prepare_local_storage_path(filename: str) -> Path:
    """
    为简单上传准备最终保存路径

    文件直接保存在 LOCAL_STORAGE_ROOT 下，不按用户ID分文件夹
    """
    LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    return LOCAL_STORAGE_ROOT / filename


def prepare_temp_chunk_path(owner_id: int, file_hash: str, part_number: int) -> Path:
    """
    为分片上传准备临时存储路径

    使用 user_{owner_id}_{file_hash}_part_{part_number} 格式命名
    避免不同用户、不同文件的分片冲突

    """
    TEMP_CHUNKS_ROOT.mkdir(parents=True, exist_ok=True)
    chunk_filename = f"user_{owner_id}_{file_hash}_part_{part_number:06d}"
    return TEMP_CHUNKS_ROOT / chunk_filename


async def compute_upload_file_metadata(upload_file: UploadFile) -> tuple[str, int]:
    """
    计算上传文件的元数据（SHA256哈希和大小）

    分片读取文件内容，避免一次性加载到内存
    读取完成后重置文件指针到开头，供后续写入使用

    TODO: 后期应改为任务队列异步执行，避免在上传接口中计算导致响应变慢

    """
    sha256 = hashlib.sha256()
    size = 0
    await upload_file.seek(0)
    while True:
        chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
        if not chunk:
            break
        sha256.update(chunk)
        size += len(chunk)
    await upload_file.seek(0)
    return sha256.hexdigest(), size


async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    将上传文件保存到指定路径

    使用 aiofiles 异步写入，分片读取避免内存占用过大
    写入完成后关闭上传文件句柄

    """
    await upload_file.seek(0)
    async with aiofiles.open(destination, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            await buffer.write(chunk)
    await upload_file.close()


def delete_local_file(path: str | Path) -> None:
    """
    删除本地文件

    同步操作，因为文件删除只是删除元数据
    使用 missing_ok=True，文件不存在时不会抛出异常

    异常处理：不捕获，让异常传播到调用方

    Args:
        path: 文件路径（字符串或 Path 对象）
    """
    # 验证路径不为空
    if not path:
        logger.warning("empty path for deletion, skipping")
        return

    # 转换为字符串进行验证
    path_str = str(path).strip()

    # 验证路径不是特殊值
    if not path_str or path_str in ('.', '..', '/', '\\'):
        logger.warning("invalid path for deletion: %r, skipping", path_str)
        return

    # 转换为 Path 对象
    path_obj = Path(path_str)

    # 验证路径不是目录
    if path_obj.exists() and path_obj.is_dir():
        logger.warning("cannot delete directory: %s, skipping", path_str)
        return

    # 删除文件
    path_obj.unlink(missing_ok=True)


def delete_file_by_storage(file_record) -> None:
    """
    根据存储类型删除文件 #TODO 后面再调用

    目前仅支持 local 存储类型，分布式存储待实现

    异常处理：不捕获，让异常传播到调用方

    Args:
        file_record: 文件记录对象（FileInDB）

    Raises:
        NotImplementedError: 不支持的存储类型
        OSError: 文件删除失败时
    """
    if file_record.storage_type == "local":
        delete_local_file(file_record.path)
    elif file_record.storage_type == "distributed":
        logger.warning(
            "distributed storage delete not implemented for file_id=%s",
            getattr(file_record, 'file_id', 'unknown')
        )
        raise NotImplementedError("Distributed storage delete not implemented")
    else:
        logger.warning(
            "unsupported storage type %s for file_id=%s",
            file_record.storage_type,
            getattr(file_record, 'file_id', 'unknown')
        )
        raise NotImplementedError(f"Unsupported storage type: {file_record.storage_type}")


def check_user_quota(user: UserInDB, file_size: int) -> bool:
    """
    检查用户配额是否足够

    综合检查：
    1. 文件数量是否超过限制
    2. 单个文件大小是否超过限制
    3. 总存储空间是否超过限制

    Returns:
        True 表示所有配额检查通过，False 表示至少有一项超出限制
    """
    # 检查文件数量
    current_count = user.file_count
    if current_count >= MAX_FILES_PER_USER:
        logger.debug(
            "file count quota exceeded user=%s current=%s limit=%s",
            user.id,
            current_count,
            MAX_FILES_PER_USER,
        )
        return False

    # 检查单个文件大小
    if file_size > MAX_FILE_SIZE:
        logger.debug(
            "file size quota exceeded user=%s size=%s limit=%s",
            user.id,
            file_size,
            MAX_FILE_SIZE,
        )
        return False

    # 检查总存储空间
    projected_total_size = user.total_file_size + file_size
    if projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        logger.debug(
            "storage quota exceeded user=%s projected=%s limit=%s",
            user.id,
            projected_total_size,
            MAX_TOTAL_STORAGE_PER_USER,
        )
        return False

    return True


def validate_total_parts(total_parts: int) -> bool:
    """
    验证分片数量是否合法

    Returns:
        True 表示分片数量合法，False 表示不合法
    """
    return 0 < total_parts <= MAX_CHUNKS_PER_UPLOAD
