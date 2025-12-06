#########################
# File API
#########################
from __future__ import annotations

import asyncio#高并发IO操作
import hashlib#提供hash算法 用于数据加密和校验
from pathlib import Path#提供面向对象的文件系统路径操作
from typing import Annotated, List

import aiofiles
from sqlmodel import select
from sqlalchemy.exc import SQLAlchemyError
from fastapi import Depends, File, Form, HTTPException, UploadFile
from starlette import status

from api.user_api import token_authen
from envset.config import get_config
from database.db import SessionDep
from logger import get_logger
from models.file_models import FileInDB, FilePublic, FileDownloadMeta, now_local_timezone
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

# 文件查询失败异常
file_query_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="failed to query file record",
)

# 数据库会话异常
invalid_db_session_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="database session not available",
)

# 文件写入失败异常
file_persist_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="failed to persist file content",
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

# 用户文件数量限制异常
user_file_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="user file count limit reached",
)

# 用户存储空间限制异常
user_storage_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="user storage limit exceeded",
)

# 文件重复异常
duplicate_file_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="duplicate filename or hash detected",
)

# 用户下载次数达到上限异常
user_download_limit_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="user download limit reached",
)

# 预检与上传不一致异常
preflight_size_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded file size does not match preflight size",
)
preflight_hash_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded file hash does not match preflight hash",
)
preflight_filename_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded filename does not match preflight filename",
)

# 基础存储配置来自 config
config = get_config()
if not config:
    logger.error("application configuration is not loaded")
    raise RuntimeError("Application configuration is not loaded")
storage_config = config.storage

STORAGE_ROOT = Path(storage_config.root)
LOCAL_STORAGE_ROOT = STORAGE_ROOT / storage_config.local_root
BYTES_IN_GB = 1024 ** 3
BYTES_IN_MB = 1024 ** 2
# 注意：YAML 不支持 Python 表达式，所以配置文件中写数字（如 10 表示 10GB），然后在代码中转换为字节
MAX_FILE_SIZE = int(storage_config.max_file_size) * BYTES_IN_GB
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
CHUNK_BUFFER_SIZE = int(storage_config.chunk_buffer_size) * BYTES_IN_MB
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
MAX_FILENAME_LENGTH = int(storage_config.max_filename_length)
# 从配置读取（带默认值）
MAX_FILES_PER_USER = int(getattr(storage_config, "max_files_per_user", 20))  # 用户上传最多文件数量
MAX_TOTAL_STORAGE_PER_USER = MAX_FILE_SIZE * MAX_FILES_PER_USER
# 若 storage.max_downloads_per_user 未配置，提供安全默认
MAX_DOWNLOADS_PER_USER = int(getattr(storage_config, "max_downloads_per_user", 50))
PREFLIGHT_TTL_MINUTES = int(getattr(storage_config, "preflight_ttl_minutes", 20))  # 预检记录清理阈值（分钟）

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
    file_ext = Path(filename).suffix.lstrip('.')

    if not file_ext:
        raise file_type_not_allowed_exception

    if file_ext not in ALLOWED_FILE_TYPES:
        raise file_type_not_allowed_exception

def append_file_id(session: SessionDep, user: UserInDB, file_id: int):
    """上传成功后，将文件 ID 记录到用户的文件列表中"""
    if not user.id:
        raise invalid_user_for_file_list_sync_exception
    file_ids = list(dict.fromkeys(user.file_ids or []))
    if file_id in file_ids:
        return
    if len(file_ids) >= MAX_FILES_PER_USER:
        raise user_file_limit_exception
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
    """为简单上传准备最终保存路径,在别处会检查文件名是否重复"""
    user_storage_dir = LOCAL_STORAGE_ROOT / str(owner_id)
    user_storage_dir.mkdir(parents=True, exist_ok=True)
    final_path = user_storage_dir / filename
    return final_path

def cleanup_stale_preflights(session: SessionDep, current_user: UserInDB, ttl_minutes: int = PREFLIGHT_TTL_MINUTES) -> int:
    """
    清理当前用户过期且未完成的预检记录（多归属）：
    条件：路径文件不存在 且 file_id 不在用户 file_ids 中 且 超过 TTL，且记录 owners 含当前用户。
    """
    now = now_local_timezone()
    user_ids = set(current_user.file_ids or [])
    deleted = 0 #计数器
    try:
        records = session.exec(select(FileInDB)).all()
        for rec in records:
            owners = set(rec.owners or [])
            if current_user.id not in owners:
                continue
            try:
                path_missing = not Path(rec.path).exists()
            except Exception:
                path_missing = True
            too_old = (now - rec.uploaded_at).total_seconds() >= ttl_minutes * 60
            if path_missing and (rec.file_id or 0) not in user_ids and too_old:
                session.delete(rec)
                deleted += 1
        if deleted:
            session.commit()
    except Exception as exc:
        logger.error("cleanup preflights failed user=%s error=%s", current_user.id, exc)
    return deleted #返回删除计数



async def compute_upload_file_metadata(upload_file: UploadFile) -> tuple[str, int]:
    """
    分片读取 UploadFile 的内容来计算 (sha256, size)，避免一次性将文件加载到内存。
    """
    sha256 = hashlib.sha256()
    size = 0
    await upload_file.seek(0)
    while True:
        chunk = await upload_file.read(CHUNK_BUFFER_SIZE)#这里一定要分片来计算hash值，不分片会消耗很大的内存
        if not chunk:
            break
        sha256.update(chunk)#同步操作 但是轻量级 应该没啥问题
        size += len(chunk)
    await upload_file.seek(0)
    return sha256.hexdigest(), size

async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    使用 aiofiles 将 UploadFile 内容写到指定路径。
    """
    await upload_file.seek(0)
    async with aiofiles.open(destination, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            await buffer.write(chunk)
    await upload_file.close()

async def get_file_record(file_id: int, session: SessionDep) -> FileInDB:

    """根据 file_id 获取文件记录，若不存在抛出 404"""
    try:
        statement = select(FileInDB).where(FileInDB.file_id == file_id)
        file_record = session.exec(statement).first()
    except SQLAlchemyError as exc:
        logger.error("database error when querying file_id=%s error=%s", file_id, exc)
        raise file_query_failed_exception from exc
    except AttributeError as exc:
        logger.error("invalid session when querying file_id=%s error=%s", file_id, exc)
        raise invalid_db_session_exception from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("unexpected error when querying file_id=%s", file_id)
        raise file_query_failed_exception from exc
    if not file_record:
        logger.warning("file record missing file_id=%s", file_id)
        raise file_not_found_exception#404  文件不存在
    return file_record

async def assert_file_owner(file_record: FileInDB, current_user: UserInDB):
    """校验文件归属（支持多归属 owners 或用户 file_ids 索引）"""

    if current_user.identity == "admin":
        return
    owners = set(file_record.owners or [])
    if current_user.id in owners:
        return
    user_file_ids = set(current_user.file_ids or [])
    if file_record.file_id in user_file_ids:
        return
    logger.warning("permission denied user=%s file_id=%s",
                   current_user.id, file_record.file_id)
    raise permission_denied_exception#没有权限

async def delete_local_file(path: str):
    """删除本地文件，忽略不存在情况"""
    file_path = Path(path)

    def _unlink():
        file_path.unlink(missing_ok=True)
        parent_dir = file_path.parent
        try:
            if parent_dir.exists() and not any(parent_dir.iterdir()):
                parent_dir.rmdir()
        except OSError:
            logger.error("failed to remove empty directory %s", parent_dir)

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

#预检函数
async def preflight_create_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    storage_type: str = Form("local"),
    filename: str = Form(...),
    hash_sha256: str = Form(...),
    size: int = Form(...),
) -> FilePublic:
    """
    预检：根据前端提供的元数据创建文件记录，但不写入用户 file_ids 与计数。
    仅当后续上传校验通过后，才在上传接口中更新用户计数与 file_ids。
    """
    ensure_storage_dirs()
    # 清理过期预检记录
    try:
        cleanup_stale_preflights(session, current_user)  # 清理过期预检记录，只在预检的时候调用一次
    except Exception:
        pass
    logger.info(
        "preflight start user=%s filename=%s storage=%s size=%s",
        current_user.id, filename, storage_type, size,
    )
    # 基础校验与存储类型
    if storage_type not in ALLOWED_STORAGE_TYPES:
        raise storage_type_not_allowed_exception
    if storage_type != "local":
        raise storage_type_not_supported_exception

    # 文件安全与类型校验
    validate_filename(filename)
    validate_file_type(filename)

    # 用户配额（基于预期大小）
    current_count = current_user.file_count or len(current_user.file_ids or [])
    if current_count >= MAX_FILES_PER_USER:
        raise user_file_limit_exception
    if size > MAX_FILE_SIZE:
        raise file_size_limit_exception
    projected_total_size = (current_user.total_file_size or 0) + size
    if projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        raise user_storage_limit_exception

    # 重复名（按用户维度）：用户已拥有同名文件则拒绝
    same_name_records = session.exec(select(FileInDB).where(FileInDB.filename == filename)).all() or []
    for rec in same_name_records:
        if (rec.file_id in (current_user.file_ids or [])) or (current_user.id in set(rec.owners or [])):
            raise duplicate_file_exception

    # 全局 hash 复用：若已有相同内容文件且路径存在，则直接绑定给当前用户并返回
    same_hash_records = session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256)).all() or []
    for rec in same_hash_records:
        if not Path(rec.path).exists():
            continue
        if (rec.file_id in (current_user.file_ids or [])) or (current_user.id in set(rec.owners or [])):
            raise duplicate_file_exception
        # 复用：建立多归属并更新用户侧统计
        append_file_id(session, current_user, rec.file_id)
        current_user.file_count = (current_user.file_count or 0) + 1
        current_user.total_file_size = (current_user.total_file_size or 0) + (rec.size or 0)
        rec.owners = list(dict.fromkeys((rec.owners or []) + [current_user.id]))#如果存在相同内容文件且路径相同，直接在文件所属列表中加入用户id即可
        session.add(current_user)
        session.add(rec)
        session.commit()
        session.refresh(rec)
        logger.info("preflight reused existing file user=%s file_id=%s filename=%s",
                    current_user.id, rec.file_id, rec.filename)
        return rec

    final_path = prepare_local_storage_path(current_user.id, filename) #为简单上传准备最终保存路径,在别处会检查文件名是否重复
    file_type = final_path.suffix.lstrip(".")
    file_record = FileInDB(
        filename=filename,
        path=str(final_path),
        owners=[current_user.id],
        file_type=file_type,
        size=size,
        storage_type=storage_type,
        hash_sha256=hash_sha256,
    )
    session.add(file_record)
    session.commit()
    session.refresh(file_record)
    logger.info(
        "preflight created user=%s file_id=%s filename=%s size=%s",
        current_user.id, file_record.file_id, file_record.filename, file_record.size,
    )
    return file_record

#主上传函数
async def upload_simple_file(current_user: Annotated[UserInDB, Depends(token_authen)],
                             session: SessionDep,
                             storage_type: str = Form("local"),
                             pre_file_id: int = Form(...),
                             file: UploadFile = File(...)):
    """
    简单上传接口：一次性接收完整文件，不做分片。
    - 根据 storage_type 选择存储方式，目前仅支持 local
    - 自动计算文件大小与 SHA256
    """
    ensure_storage_dirs()
    logger.info("simple upload start user=%s filename=%s storage=%s",
                current_user.id, file.filename, storage_type)
    # 校验part
    # 基础校验：
    if storage_type not in ALLOWED_STORAGE_TYPES:
        raise storage_type_not_allowed_exception#存储类型不在允许列表异常

    if storage_type != "local":
        raise storage_type_not_supported_exception#存储类型尚未实现异常

    # 预检记录验证
    pre_record = await get_file_record(pre_file_id, session)
    await assert_file_owner(pre_record, current_user)

    # 存储类型与文件名一致性
    if storage_type != pre_record.storage_type:
        session.delete(pre_record)
        session.commit()
        raise storage_type_not_allowed_exception
    if not file.filename:
        raise filename_invalid_exception
    validate_filename(file.filename)
    validate_file_type(file.filename)
    if file.filename != pre_record.filename:
        session.delete(pre_record)
        session.commit()
        raise preflight_filename_mismatch_exception #文件名不匹配

    # 计算实际文件hash与大小并与预检记录比对
    hash_sha256, size = await compute_upload_file_metadata(file)
    if size != pre_record.size:
        session.delete(pre_record)
        session.commit()
        raise preflight_size_mismatch_exception
    if hash_sha256 != (pre_record.hash_sha256 or ""):
        session.delete(pre_record)
        session.commit()
        raise preflight_hash_mismatch_exception

    # 用户配额（再次轻量校验，防止预检后状态变化）
    current_count = current_user.file_count or len(current_user.file_ids or [])
    if current_count >= MAX_FILES_PER_USER:
        raise user_file_limit_exception
    projected_total_size = (current_user.total_file_size or 0) + size
    if projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        raise user_storage_limit_exception

    # 全局 hash 复用（上传阶段）：若已存在相同内容文件且路径存在，则跳过写入，仅建立关联
    same_hash_records = session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256)).all() or []
    for rec in same_hash_records:
        # 跳过当前预检记录自身
        if rec.file_id == pre_record.file_id:
            continue
        try:
            if not Path(rec.path).exists():
                continue
        except Exception:
            continue
        # 若用户已拥有该内容，则视为重复，清理预检记录并报错
        if (rec.file_id in (current_user.file_ids or [])) or (current_user.id in set(rec.owners or [])):
            session.delete(pre_record)
            session.commit()
            raise duplicate_file_exception
        # 复用：绑定用户并清理预检记录
        append_file_id(session, current_user, rec.file_id)
        current_user.file_count = (current_user.file_count or 0) + 1
        current_user.total_file_size = (current_user.total_file_size or 0) + (rec.size or 0)
        rec.owners = list(dict.fromkeys((rec.owners or []) + [current_user.id]))
        session.add(current_user)
        session.add(rec)
        # 关闭上传文件句柄，避免资源泄露 关闭一下比较好
        try:
            await file.close()
        except Exception:
            pass
        session.delete(pre_record)
        session.commit()
        session.refresh(rec)
        logger.info("simple upload reused existing file user=%s file_id=%s filename=%s",
                    current_user.id, rec.file_id, rec.filename)
        return rec  #可以提前返回
    final_path = prepare_local_storage_path(current_user.id, file.filename)
    try:
        await save_upload_file(file, final_path)  #使用 aiofiles 将 UploadFile 内容写到指定路径。
    except Exception as exc:
        logger.error("save upload file failed user=%s filename=%s error=%s",
                     current_user.id, file.filename, exc)
        final_path.unlink(missing_ok=True)
        session.delete(pre_record)
        session.commit()
        raise file_persist_failed_exception from exc
    # 刷新 file表
    # 使用预检创建的记录作为最终记录
    file_record = pre_record
    session.flush()
    # 刷新 用户表
    append_file_id(session, current_user, file_record.file_id)
    current_user.file_count = (current_user.file_count or 0) + 1
    current_user.total_file_size = (current_user.total_file_size or 0) + size
    session.commit()
    session.refresh(file_record)
    session.refresh(current_user)
    logger.info("simple upload completed user=%s file_id=%s filename=%s size=%s",
                current_user.id, file_record.file_id, file_record.filename, file_record.size)
    return file_record

#主查询函数
async def list_user_files(current_user: Annotated[UserInDB, Depends(token_authen)],
                          session: SessionDep) -> List[FilePublic]:
    """
    查询当前用户的全部文件记录，依据用户记录的文件 ID 列表过滤
    """
    if not current_user.file_ids:
        return []
    seen_ids = list(dict.fromkeys(current_user.file_ids))[:MAX_FILES_PER_USER]
    if not seen_ids:
        return []

    statement = select(FileInDB).where(FileInDB.file_id.in_(seen_ids))
    results = session.exec(statement).all()
    files = [record for record in results]
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
    #
    file_record = await get_file_record(file_id, session)
    await assert_file_owner(file_record, current_user)  # 权限校验（管理员或拥有索引者）

    # 1) 先从当前用户的索引中移除
    remove_file_id(session, current_user, file_id)
    current_user.file_count = max(0, (current_user.file_count or 0) - 1)
    current_user.total_file_size = max(0, (current_user.total_file_size or 0) - file_record.size)
    session.add(current_user)
    session.flush()

    # 同步更新文件记录的 owners，移除当前用户归属
    current_owners = list(file_record.owners or [])
    if current_user.id in current_owners:
        current_owners.remove(current_user.id)
    file_record.owners = current_owners
    session.add(file_record)
    session.flush()

    # 2) 检查是否还有其他用户持有该文件索引（依据 owners）
    if file_record.owners:
        session.commit()
        session.refresh(current_user)
        logger.info("unlinked file reference only user=%s file_id=%s", current_user.id, file_id)
        return {"file_id": file_id, "status": "unlinked"}

    # 3) 无其他归属，删除物理文件与记录
    await delete_file_by_storage(file_record)
    session.delete(file_record)
    session.commit()
    session.refresh(current_user)
    logger.info("deleted file content and record user=%s file_id=%s", current_user.id, file_id)
    return {"file_id": file_id, "status": "deleted"}

#主下载函数
async def get_download_file_metadata(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep
) -> FileDownloadMeta:
    """
    返回文件元数据和路径，不处理文件流
    - 校验身份与所有权
    - 仅支持 local 存储类型
    - 更新下载计数
    """
    file_record = await get_file_record(file_id, session)
    await assert_file_owner(file_record, current_user)

    logger.debug("download request user=%s file_id=%s", current_user.id, file_id)
    # 用户下载次数限制（管理员豁免）
    if current_user.identity != "admin":
        used = current_user.download_used or 0
        if used >= MAX_DOWNLOADS_PER_USER:
            raise user_download_limit_exception
    if file_record.storage_type != "local":
        raise download_not_implemented_exception#当前存储类型下载未实现异常

    path = Path(file_record.path)
    if not path.exists():
        logger.warning("file content missing user=%s file_id=%s path=%s",
                       current_user.id, file_id, path)
        raise file_content_missing_exception#文件内容缺失异常

    # 更新下载计数
    file_record.download_count += 1
    if current_user.identity != "admin":
        current_user.download_used = (current_user.download_used or 0) + 1
        session.add(current_user)
    session.add(file_record)
    session.commit()
    logger.info("download success user=%s file_id=%s filename=%s",
                current_user.id, file_id, file_record.filename)

    return FileDownloadMeta.from_record(file_record)


DeleteFileDep = Annotated[dict, Depends(delete_user_file)]
SimpleUploadDep = Annotated[FilePublic, Depends(upload_simple_file)]
ListFilesDep = Annotated[List[FilePublic], Depends(list_user_files)]
DownloadFileDep = Annotated[FileDownloadMeta, Depends(get_download_file_metadata)]
PreflightDep = Annotated[FilePublic, Depends(preflight_create_file)]
