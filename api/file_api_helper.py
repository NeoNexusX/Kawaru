#########################
# File API Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：文件校验、路径准备、文件IO操作
#########################
from __future__ import annotations

import hashlib
from pathlib import Path

import aiofiles
from fastapi import UploadFile

from constants import (
    STORAGE_ROOT,
    LOCAL_STORAGE_ROOT,
    TEMP_CHUNKS_ROOT,
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
    MAX_FILES_PER_USER,
    MAX_TOTAL_STORAGE_PER_USER,
    CHUNK_BUFFER_SIZE,
    MAX_FILE_SIZE,
)

from logger import get_logger

logger = get_logger("main.file_api_helper")

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
    Path(path).unlink(missing_ok=True)


def assert_file_owner(file_record, current_user) -> bool:
    """
    校验用户是否有权限操作该文件
    
    权限判断逻辑：
    1. 管理员拥有所有文件的权限
    2. 普通用户需要同时满足：
       - 用户ID在文件的 owners 列表中
       - 文件ID在用户的 file_ids 列表中（双向校验）
    
    Args:
        file_record: 文件记录对象（FileInDB）
        current_user: 当前用户对象（UserInDB）
    
    Returns:
        True 表示有权限，False 表示无权限
    """
    # 管理员拥有所有权限
    if current_user.identity == "admin":
        return True

    # 双向校验：用户在文件owners中 且 文件在用户file_ids中
    owners = set(file_record.owners or [])
    user_file_ids = set(current_user.file_ids or [])
    if current_user.id in owners and file_record.file_id in user_file_ids:
        return True

    logger.warning(
        "permission denied user=%s file_id=%s", current_user.id, file_record.file_id
    )
    return False


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


def check_user_quota(user, file_size: int) -> bool:
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
    current_count = user.file_count or len(user.file_ids or [])
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
    projected_total_size = (user.total_file_size or 0) + file_size
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
    return 0 < total_parts <= 20
