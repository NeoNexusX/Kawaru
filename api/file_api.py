#########################
# File API
#########################
from __future__ import annotations

import asyncio#高并发IO操作
import hashlib#提供hash算法 用于数据加密和校验
from pathlib import Path#提供面向对象的文件系统路径操作
from typing import Annotated, List

from fastapi import Depends, File, Form, HTTPException, UploadFile
from sqlmodel import select
from starlette import status

from api.user_api import token_authen
from envset.config import get_config
from database.db import SessionDep
from logger import get_logger
from models.file_models import FileDownload, FileInDB, FilePublic
from models.user_models import UserInDB

logger = get_logger("main.file_api")

# 存储类型不在允许列表异常
storage_type_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="storage type is not allowed",
)

# 用户文件列表同步异常
invalid_user_for_file_list_sync_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="invalid user for file id list sync")

# 存储类型尚未实现异常
storage_type_not_supported_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="storage type is not supported yet",
)

# 文件超出大小限制异常
file_size_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="file size exceeds limit",
)

# 文件不存在异常
file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="file not found",
)

# 无权限访问文件异常
permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="permission denied",
)

# 分布式存储删除未实现异常
distributed_storage_delete_not_implemented_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="distributed storage delete not implemented",
)

# 不支持的存储类型删除异常
storage_type_delete_unsupported_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="unsupported storage type",
)

# 当前存储类型下载未实现异常
download_not_implemented_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="download for this storage type not implemented",
)

# 文件内容缺失异常
file_content_missing_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="file content missing",
)

# 文件名不安全异常
filename_invalid_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="filename is invalid or contains unsafe characters",
)

# 文件类型不允许异常
file_type_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="file type is not allowed",
)

# 基础存储配置来自 config
storage_config = get_config().storage

STORAGE_ROOT = Path(storage_config.root)
LOCAL_STORAGE_ROOT = STORAGE_ROOT / storage_config.local_root
MAX_FILE_SIZE = storage_config.max_file_size
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
CHUNK_BUFFER_SIZE = storage_config.chunk_buffer_size
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
MAX_FILENAME_LENGTH = storage_config.max_filename_length


def ensure_storage_dirs():
    """确保存储目录存在"""
    for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def validate_filename(filename: str):
    """校验文件名安全性"""
    if not filename or not filename.strip():
        raise filename_invalid_exception
    
    # 检查文件名长度
    if len(filename) > MAX_FILENAME_LENGTH:
        raise filename_invalid_exception
    
    # 检查路径遍历攻击（../, ..\, /, \）
    unsafe_patterns = ["../", "..\\", "/", "\\", ".."]
    for pattern in unsafe_patterns:
        if pattern in filename:
            raise filename_invalid_exception
    
    # 检查危险字符
    dangerous_chars = ['<', '>', ':', '"', '|', '?', '*', '\x00']
    for char in dangerous_chars:
        if char in filename:
            raise filename_invalid_exception


def validate_file_type(filename: str):
    """校验文件类型"""
    if not filename:
        raise file_type_not_allowed_exception
    
    # 提取文件扩展名
    file_ext = Path(filename).suffix.lstrip('.').lower()
    
    if not file_ext:
        raise file_type_not_allowed_exception
    
    if file_ext not in ALLOWED_FILE_TYPES:
        raise file_type_not_allowed_exception


def to_file_public(record: FileInDB) -> FilePublic:#强行转换成了不带隐私数据的
    return FilePublic(
        file_id=record.file_id,
        filename=record.filename,
        file_type=record.file_type,
        size=record.size,
        storage_type=record.storage_type,
        hash_sha256=record.hash_sha256,
        uploaded_at=record.uploaded_at,
        download_count=record.download_count,
    )


def append_file_id(session: SessionDep, user: UserInDB, file_id: int):
    """上传成功后，将文件 ID 记录到用户的文件列表中"""
    if not user.id:
        raise invalid_user_for_file_list_sync_exception
    file_ids = list(user.file_ids or [])
    if file_id not in file_ids:
        file_ids.append(file_id)
        user.file_ids = file_ids
        session.add(user)


def remove_file_id(session: SessionDep, user: UserInDB, file_id: int):
    """删除文件后，从用户文件列表里移除对应 ID"""
    if not user.id:
        raise invalid_user_for_file_list_sync_exception
    file_ids = list(user.file_ids or [])
    if file_id in file_ids:
        file_ids.remove(file_id)
        user.file_ids = file_ids
        session.add(user)


def prepare_local_storage_path(owner_id: int, filename: str) -> Path:
    """为简单上传准备最终保存路径，若重名则自动追加序号"""
    user_storage_dir = LOCAL_STORAGE_ROOT / str(owner_id)
    user_storage_dir.mkdir(parents=True, exist_ok=True)
    final_path = user_storage_dir / filename
    if not final_path.exists():
        return final_path

    stem = final_path.stem
    suffix = final_path.suffix
    counter = 1
    while True:
        candidate = user_storage_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


async def save_upload_file(upload_file: UploadFile, destination: Path) -> tuple[str, int]:
    """
    将 UploadFile 保存到指定路径，并返回 (sha256, size)
    """
    sha256 = hashlib.sha256()
    size = 0
    with destination.open("wb") as buffer:#计算这个文件的hash和大小 
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            buffer.write(chunk)
            sha256.update(chunk)
            size += len(chunk)
    await upload_file.close()
    return sha256.hexdigest(), size


async def get_file_record(file_id: int, session: SessionDep) -> FileInDB:
    """根据 file_id 获取文件记录，若不存在抛出 404"""
    statement = select(FileInDB).where(FileInDB.file_id == file_id)
    file_record = session.exec(statement).first()
    if not file_record:
        logger.warning("file record missing file_id=%s", file_id)
        raise file_not_found_exception#404  文件不存在
    return file_record


async def assert_file_owner(file_record: FileInDB, current_user: UserInDB):
    """校验文件归属"""
    if file_record.owner_id != current_user.id:
        logger.warning("permission denied user=%s file_id=%s owner=%s",
                       current_user.id, file_record.file_id, file_record.owner_id)
        raise permission_denied_exception#没有权限，只能删除自己的文件


async def delete_local_file(path: str):
    """删除本地文件，忽略不存在情况"""
    file_path = Path(path)

    def _unlink():
        file_path.unlink(missing_ok=True)

    await asyncio.to_thread(_unlink)


async def delete_file_by_storage(file_record: FileInDB):
    """根据 storage_type 调用对应删除实现"""
    if file_record.storage_type == "local":
        await delete_local_file(file_record.path)
    elif file_record.storage_type == "distributed":
        #####此处留好了接口，等待下一次开发分布式存储的时候再实现
        raise distributed_storage_delete_not_implemented_exception#分布式存储删除未实现异常
    else:
        raise storage_type_delete_unsupported_exception#不支持的存储类型删除异常


#主上传函数
async def upload_simple_file(current_user: Annotated[UserInDB, Depends(token_authen)],
                             session: SessionDep,
                             storage_type: str = Form("local"),
                             file: UploadFile = File(...)) -> FilePublic:
    """
    简单上传接口：一次性接收完整文件，不做分片。
    - 根据 storage_type 选择存储方式，目前仅支持 local
    - 自动计算文件大小与 SHA256
    """
    ensure_storage_dirs()
    logger.info("simple upload start user=%s filename=%s storage=%s",
                current_user.id, file.filename, storage_type)
    
    # 基础校验：
    if storage_type not in ALLOWED_STORAGE_TYPES:
        raise storage_type_not_allowed_exception#存储类型不在允许列表异常

    if storage_type != "local":
        raise storage_type_not_supported_exception#存储类型尚未实现异常
    
    # 文件安全校验
    if not file.filename:
        raise filename_invalid_exception
    validate_filename(file.filename)
    validate_file_type(file.filename)
    final_path = prepare_local_storage_path(current_user.id, file.filename)
    hash_sha256, size = await save_upload_file(file, final_path)
    if size > MAX_FILE_SIZE:
        final_path.unlink(missing_ok=True)
        raise file_size_limit_exception#文件超出大小限制异常
    file_type = final_path.suffix.lstrip(".").lower()
    file_record = FileInDB(filename=final_path.name,
                           path=str(final_path),
                           owner_id=current_user.id,
                           file_type=file_type,
                           size=size,
                           storage_type=storage_type,
                           hash_sha256=hash_sha256)
    session.add(file_record)
    session.flush()
    append_file_id(session, current_user, file_record.file_id)
    session.commit()
    session.refresh(file_record)
    session.refresh(current_user)
    logger.info("simple upload completed user=%s file_id=%s filename=%s size=%s",
                current_user.id, file_record.file_id, file_record.filename, file_record.size)
    return to_file_public(file_record)



#主查询函数
async def list_user_files(current_user: Annotated[UserInDB, Depends(token_authen)],
                          session: SessionDep) -> List[FilePublic]:
    """
    查询当前用户的全部文件记录，依据用户记录的文件 ID 列表过滤
    """
    if not current_user.file_ids:
        return []
    statement = select(FileInDB).where(FileInDB.file_id.in_(current_user.file_ids))
    results = session.exec(statement).all()
    files = [to_file_public(record) for record in results]
    logger.debug("list user files user=%s count=%s", current_user.id, len(files))
    return files




#主删除函数
async def delete_user_file(file_id: int,
                           current_user: Annotated[UserInDB, Depends(token_authen)],
                           session: SessionDep) -> dict:
    """
    删除当前用户的指定文件：
    - 校验所有权
    - 根据 storage_type 调用对应删除实现
    - 移除数据库记录
    """
    file_record = await get_file_record(file_id, session)
    await assert_file_owner(file_record, current_user)
    await delete_file_by_storage(file_record)
    session.delete(file_record)
    session.flush()
    remove_file_id(session, current_user, file_id)
    session.commit()
    session.refresh(current_user)
    logger.info("delete file success user=%s file_id=%s", current_user.id, file_id)
    return {"file_id": file_id, "status": "deleted"}

#主下载函数
async def download_user_file(file_id: int,
                             current_user: Annotated[UserInDB, Depends(token_authen)],
                             session: SessionDep) -> FileDownload:
    """
    下载文件：
    - 校验身份与所有权
    - 仅支持 local 存储类型
    """
    file_record = await get_file_record(file_id, session)
    await assert_file_owner(file_record, current_user)

    logger.debug("download request user=%s file_id=%s", current_user.id, file_id)
    if file_record.storage_type != "local":
        raise download_not_implemented_exception#当前存储类型下载未实现异常

    path = Path(file_record.path)
    if not path.exists():
        logger.warning("file content missing user=%s file_id=%s path=%s",
                       current_user.id, file_id, path)
        raise file_content_missing_exception#文件内容缺失异常

    file_record.download_count += 1
    session.add(file_record)
    session.commit()
    logger.info("download success user=%s file_id=%s filename=%s",
                current_user.id, file_id, file_record.filename)

    return FileDownload(path=str(path), filename=file_record.filename)


DownloadFileDep = Annotated[FileDownload, Depends(download_user_file)]
DeleteFileDep = Annotated[dict, Depends(delete_user_file)]
SimpleUploadDep = Annotated[FilePublic, Depends(upload_simple_file)]
ListFilesDep = Annotated[List[FilePublic], Depends(list_user_files)]