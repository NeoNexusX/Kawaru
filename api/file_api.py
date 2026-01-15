#########################
# File API
#########################
from __future__ import annotations

import asyncio  # 高并发IO操作
import hashlib  # 提供hash算法 用于数据加密和校验
from pathlib import Path  # 提供面向对象的文件系统路径操作
from typing import Annotated, List
import zipfile
import aiofiles
from sqlmodel import select
from sqlalchemy.exc import SQLAlchemyError
from fastapi import Depends, File, Form, HTTPException, UploadFile, BackgroundTasks
from api.user_api import token_authen
from api.experiment_api import process_imzml_experiment_background
from database.db import SessionDep
from logger import get_logger
from constants import *  # noqa: F403
from exceptions.file_exceptions import *  # noqa: F403
from models.file_models import (
    FileInDB,
    FilePublic,
    FileDownloadMeta,
    PartInDB,
    ChunkedUploadStatus,
    now_local_timezone,
)
from models.user_models import UserInDB
from models.experiment_models import ExperimentImzMLInDB, ExperimentStatus

logger = get_logger("main.file_api")

# 数据库操作层 ，不会触发任何依赖
def ensure_storage_dirs():
    """确保存储目录存在"""
    for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_CHUNKS_ROOT):
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
    dangerous_chars = ["<", ">", ":", '"', "|", "?", "*", "\x00"]
    for char in dangerous_chars:
        if char in filename:
            raise filename_invalid_exception


def validate_file_type(filename: str):
    """校验文件类型"""

    if not filename:
        raise file_type_not_allowed_exception

    # 提取文件扩展名
    file_ext = Path(filename).suffix.lstrip(".")

    # Not allow non-file-extension file
    if not file_ext:
        raise file_type_not_allowed_exception

    if file_ext not in ALLOWED_FILE_TYPES:
        raise file_type_not_allowed_exception


def append_file_id(session, user: UserInDB, file_id: int) -> None:
    """
    上传成功后，将文件 ID 记录到用户的文件列表中
    注意:这里只有add没有commit
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception
    # 同一份文件不会被多个用户所拥有
    file_ids = list(user.file_ids or [])

    # If file_id has been in user's file list, do nothing but return.
    if file_id in file_ids:
        return

    # Check if exceed the MAX_FILE_PER_USER limit
    if len(file_ids) >= MAX_FILES_PER_USER:
        raise user_file_limit_exception

    # append new file id to user
    file_ids.append(file_id)
    user.file_ids = file_ids
    session.add(user)


def remove_file_id(session , user: UserInDB, file_id: int) -> None:
    """删除文件后，从用户文件列表里移除对应 ID"""
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    file_ids = list(user.file_ids or [])

    if file_id in file_ids:
        file_ids.remove(file_id)
        user.file_ids = file_ids
        session.add(user)


def prepare_local_storage_path(filename: str) -> Path:
    """
    为简单上传准备最终保存路径，直接保存在 local 文件夹下,不按用户ID分文件夹
    """

    # 确保 local 存储目录存在
    LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    # 直接将文件保存在 local 文件夹下
    final_path = LOCAL_STORAGE_ROOT / filename
    return final_path


# TODO:未审查
async def cleanup_stale_preflights(
    session: SessionDep,
    current_user: UserInDB,
    ttl_minutes: int = PREFLIGHT_TTL_MINUTES,
) -> int:
    """
    清理当前用户过期且未完成的预检记录。
    条件：
        - 记录 owners 中包含当前用户
        - 文件路径不存在（说明为预检记录）
        - file_id 不在用户 file_ids
        - uploaded_at 超过 TTL
    使用 safe_cleanup_preflight_record 避免误删。
    """
    now = now_local_timezone()
    user_file_ids = set(current_user.file_ids or [])
    cleaned = 0

    try:
        # 只查询 owners 包含当前用户的记录（避免全表扫描）
        records = (
            await session.exec(
                select(FileInDB).where(FileInDB.owners.contains([current_user.id]))
            )
        ).all()

        for rec in records:
            owners = set(rec.owners or [])

            # 再保险：没有当前用户直接跳过
            if current_user.id not in owners:
                continue

            # 1. 文件不存在：表示预检未上传完成
            try:
                path_missing = not Path(rec.path).exists()
            except Exception:
                path_missing = True  # 任何异常都视为不存在

            if not path_missing:
                # 文件存在 = 已上传完成 或 复用文件 → 不能删
                continue

            # 2. 不在用户 file_ids = 预检记录（复用文件也满足，但有 path 存在保护）
            if rec.file_id in user_file_ids:
                continue

            # 3. TTL 判断
            too_old = (now - rec.uploaded_at).total_seconds() >= ttl_minutes * 60
            if not too_old:
                continue

            # 满足所有条件 → 清理预检记录
            safe_cleanup_preflight_record(session, current_user, rec)
            cleaned += 1

        if cleaned:
            await session.commit()

    except Exception as exc:
        logger.error("cleanup preflights failed user=%s error=%s", current_user.id, exc)

    return cleaned


async def compute_upload_file_metadata(upload_file: UploadFile) -> tuple[str, int]:
    """
    分片读取 UploadFile 的内容来计算 (sha256, size)，避免一次性将文件加载到内存。
    此操作无法异步，需要使用线程池来执行，但由于是轻量级的计算，直接在事件循环中执行应该也不会有太大问题。
    后期应改为任务队列异步执行，避免在上传接口中计算导致响应变慢。
    """
    sha256 = hashlib.sha256()
    size = 0
    await upload_file.seek(0)
    while True:
        chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
        # 这里一定要分片来计算hash值，不分片会消耗很大的内存
        if not chunk:
            break
        sha256.update(chunk)  # 同步操作 但是轻量级 应该没啥问题
        size += len(chunk)
    # 重置到文件开头？为什么要这样？ TODO:需要审查
    await upload_file.seek(0)
    return sha256.hexdigest(), size


# 写入文件函数
async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    使用 aiofiles 将 UploadFile 内容写到指定路径。
    """
    #内存缓存很小，如果全部落盘应该直接move就可以，不需要重新读取再写入
    await upload_file.seek(0)
    async with aiofiles.open(destination, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)  # 分片写入文件
            if not chunk:
                break
            await buffer.write(chunk)
    await upload_file.close()


# 查找文件记录
async def get_file_record(file_id: int, session) -> FileInDB|None:
    """
    根据 file_id 获取文件记录
    """

    # try:
    statement = select(FileInDB).where(FileInDB.file_id == file_id)
    file_record = (await session.exec(statement)).first()

    # except SQLAlchemyError as exc:
    #     logger.error("database error when querying file_id=%s error=%s", file_id, exc)
    #     raise file_query_failed_exception from exc
    # except AttributeError as exc:
    #     logger.error("invalid session when querying file_id=%s error=%s", file_id, exc)
    #     raise invalid_db_session_exception from exc
    # except Exception as exc:
    #     logger.exception("unexpected error when querying file_id=%s", file_id)
    #     raise file_query_failed_exception from exc

    # if not file_record:
    #     logger.warning("file record missing file_id=%s", file_id)
    #     raise file_not_found_exception  # 404  文件不存在

    return file_record


# 检查文件权限
async def assert_file_owner(file_record: FileInDB, current_user: UserInDB)-> bool:
    """校验文件归属（支持多归属 owners 或用户 file_ids 索引）"""

    if current_user.identity == "admin":
        return True

    # 双向检查正确了才返回True
    owners = set(file_record.owners or [])
    user_file_ids = set(current_user.file_ids or [])
    if current_user.id in owners:
        if file_record.file_id in user_file_ids:
            return True

    logger.warning("permission denied user=%s file_id=%s", current_user.id, file_record.file_id)

    return False
    # raise permission_denied_exception  # 没有权限


# 删除函数
async def delete_local_file(path: str):
    """删除本地文件，忽略不存在情况"""
    file_path = Path(path)

    def _unlink():
        file_path.unlink(missing_ok=True)
        # 注意：现在文件直接保存在 local 文件夹下，不删除 local 目录
        # 只有当父目录不是 LOCAL_STORAGE_ROOT 且为空时才删除
        parent_dir = file_path.parent
        try:
            if (parent_dir.exists()
                and parent_dir != LOCAL_STORAGE_ROOT
                and not any(parent_dir.iterdir())
                ):#TODO:没看懂啊，为什么要删除空目录？
                parent_dir.rmdir()
        except OSError:
            logger.error("failed to remove empty directory %s", parent_dir)

    await asyncio.to_thread(_unlink)#TODO：不该调用线程池


async def delete_file_by_storage(file_record: FileInDB):
    """根据 storage_type 调用对应删除实现"""
    if file_record.storage_type == "local":
        await delete_local_file(file_record.path)
    elif file_record.storage_type == "distributed":
        #####此处留好了接口，等待下一次开发分布式存储的时候再实现
        raise distributed_storage_delete_not_implemented_exception  # 分布式存储删除未实现异常
    else:
        raise storage_type_delete_unsupported_exception  # 不支持的存储类型删除异常

# TODO：层级不确定，需要审查，返回值不确定，需要审查
async def safe_cleanup_preflight_record(
    session: SessionDep, current_user: UserInDB, pre_record: FileInDB
):
    """
    安全清理预检记录：
    - 如果文件只属于当前用户，则删除整条记录
    - 如果文件属于多个用户，则只移除当前用户的关联
    """
    # 从文件的 owners 中移除当前用户
    current_owners = list(pre_record.owners or [])
    if current_user.id in current_owners:
        current_owners.remove(current_user.id)
    pre_record.owners = current_owners

    # 检查是否还有其他用户持有该文件
    if pre_record.owners:
        # 还有其他用户，只更新 owners，不删除记录
        session.add(pre_record)
        logger.debug(f"preflight cleanup: removed user={current_user.id} from file_id={pre_record.file_id} owners")
        return

    # owners 为空，进一步检查其他用户的 file_ids
    #TODO：这他吗在干嘛？
    all_users = (await session.exec(select(UserInDB))).all()
    for user in all_users:
        if user.id == current_user.id:
            continue
        if pre_record.file_id in (user.file_ids or []):
            # 发现其他用户仍持有该文件，只更新 owners
            session.add(pre_record)
            logger.debug(
                "preflight cleanup: file_id=%s still in other user file_ids",
                pre_record.file_id,
            )
            return

    ##
    # owners 为空，检查是否有其他用户持有该文件（利用数据库索引查询，不再全表扫描）
    # stmt = select(UserInDB).where(
    #     UserInDB.file_ids.contains([pre_record.file_id]), # 检查 file_ids 包含该文件ID
    #     UserInDB.id != current_user.id                     # 且不是当前用户
    # )
    # 如果检查了还有其他用户持有该文件，则不删除记录

    # 确认无其他用户持有，安全删除记录
    await session.delete(pre_record)
    logger.debug("preflight cleanup: deleted file_id=%s completely", pre_record.file_id)

# TODO： 层级不确定，如果有返回值可以判断，不需要catch，查看下调用时机。
# TODO：数据库层级级联一下
async def cleanup_stale_chunks(session: SessionDep,
                               current_user: UserInDB,
                               ttl_minutes: int = CHUNK_TTL_MINUTES) -> int:
    """
    清理当前用户过期的分片记录
    """
    now = now_local_timezone()
    cleaned = 0
    # 查询当前用户的过期分片
    chunks = (await session.exec(select(PartInDB).where(PartInDB.owner_id == current_user.id))).all()

    for chunk in chunks:
        too_old = (now - chunk.uploaded_at).total_seconds() >= ttl_minutes * 60
        if too_old:
            # 删除临时文件
            Path(chunk.temp_path).unlink(missing_ok=True)
            # 删除数据库记录
            await session.delete(chunk)
            cleaned += 1

        if cleaned:
            await session.commit()

    return cleaned


def prepare_temp_chunk_path(owner_id: int, file_hash: str, part_number: int) -> Path:
    """为分片准备临时存储路径，直接保存在 temp_chunks 文件夹下"""
    # 确保临时分片目录存在
    TEMP_CHUNKS_ROOT.mkdir(parents=True, exist_ok=True)
    # 使用用户ID和文件hash组合避免冲突：user_{owner_id}_{file_hash}_part_{part_number}
    chunk_filename = f"user_{owner_id}_{file_hash}_part_{part_number:06d}"
    return TEMP_CHUNKS_ROOT / chunk_filename


# 分片合并函数
async def validate_and_merge_chunks(
    session: SessionDep, file_record: FileInDB, current_user: UserInDB
) -> bool:
    """
    验证分片完整性并尝试合并，每次上传分片后调用
    返回 True 表示合并成功，False 表示尚未完成或失败
    """
    try:
        # 获取该文件的所有分片记录
        chunks = (
            await session.exec(
                select(PartInDB)
                .where(
                    PartInDB.file_id == file_record.file_id,
                    PartInDB.owner_id == current_user.id,
                )
                .order_by(PartInDB.part_number)
            )
        ).all()

        if not chunks:
            logger.warning(
                "no chunks found for file_id=%s user=%s",
                file_record.file_id,
                current_user.id,
            )
            return False

        # 获取预期的分片总数
        expected_total_parts = file_record.total_parts
        if not expected_total_parts:
            logger.error("total_parts not set for file_id=%s", file_record.file_id)
            return False

        # 检查是否有超出范围的分片编号
        for chunk in chunks:
            if chunk.part_number > expected_total_parts:
                logger.error(
                    "invalid part number %s exceeds total_parts %s, deleting chunk file_id=%s user=%s",
                    chunk.part_number,
                    expected_total_parts,
                    file_record.file_id,
                    current_user.id,
                )
                # 删除超出范围的分片文件和记录
                try:
                    Path(chunk.temp_path).unlink(missing_ok=True)
                except Exception:
                    pass
                session.delete(chunk)
                await session.commit()
                raise part_number_out_of_range_exception

        # 检查分片完整性
        uploaded_parts = sorted(
            [chunk.part_number for chunk in chunks]
        )  # 做了一个排序，在其他函数校验了不会上传重复分片
        expected_parts = list(range(1, expected_total_parts + 1))

        # 记录当前上传状态
        logger.info(
            "chunk upload status file_id=%s user=%s uploaded_parts=%s expected_parts=%s",
            file_record.file_id,
            current_user.id,
            uploaded_parts,
            expected_parts,
        )

        # 如果分片不完整，返回 False
        if len(uploaded_parts) < expected_total_parts:
            missing_parts = [p for p in expected_parts if p not in uploaded_parts]
            logger.info(
                "chunks incomplete file_id=%s user=%s missing_parts=%s",
                file_record.file_id,
                current_user.id,
                missing_parts,
            )
            return False

        # 检查分片序列是否连续
        if uploaded_parts != expected_parts:
            logger.error(
                "chunk sequence invalid file_id=%s user=%s uploaded=%s expected=%s",
                file_record.file_id,
                current_user.id,
                uploaded_parts,
                expected_parts,
            )
            raise incomplete_part_sequence_exception

        # 所有分片都已上传，开始合并
        logger.info(
            "all chunks uploaded, starting merge file_id=%s user=%s",
            file_record.file_id,
            current_user.id,
        )
        chunks = sorted(
            chunks, key=lambda c: c.part_number
        )  # 把所有上传的分片排序，保证写入的时候是按顺序写入的

        # 这种情况概率比较小，就是在两个用户同时上传同一个文件的时候出现这种问题。
        # 检查是否可以复用现有文件  #这个地方是用来检测有一种情况， 分片都已上传完，但在真正写最终文件之前，再次检查是否已经存在完全相同内容的文件，如果存在，就直接复用那个文件，而不生成新的物理文件，然后把分片记录全部删除
        existing_files = (
            await session.exec(
                select(FileInDB).where(
                    FileInDB.hash_sha256 == file_record.hash_sha256,
                    FileInDB.file_id != file_record.file_id,
                )
            )
        ).all()

        for existing_file in existing_files:
            if Path(existing_file.path).exists():
                logger.info(
                    "found existing file for reuse file_id=%s existing_path=%s user=%s",
                    file_record.file_id,
                    existing_file.path,
                    current_user.id,
                )

                # 清理分片记录和临时文件
                for chunk in chunks:
                    try:
                        Path(chunk.temp_path).unlink(missing_ok=True)
                    except Exception:
                        pass
                    session.delete(chunk)

                # 更新当前文件记录的路径为已存在文件的路径
                file_record.path = existing_file.path
                session.add(file_record)
                await session.commit()

                logger.info(
                    "chunks reused existing file file_id=%s user=%s reused_path=%s",
                    file_record.file_id,
                    current_user.id,
                    existing_file.path,
                )
                return True

        # 执行分片合并
        final_path = Path(file_record.path)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        total_hash = hashlib.sha256()
        async with aiofiles.open(final_path, "wb") as final_file:
            for chunk in chunks:
                chunk_path = Path(chunk.temp_path)
                if not chunk_path.exists():
                    logger.error(
                        "chunk file missing path=%s file_id=%s",
                        chunk_path,
                        file_record.file_id,
                    )
                    final_path.unlink(missing_ok=True)
                    raise chunk_file_missing_exception

                async with aiofiles.open(chunk_path, "rb") as chunk_file:
                    while True:
                        data = await chunk_file.read(CHUNK_BUFFER_SIZE)
                        if not data:
                            break
                        await final_file.write(data)
                        total_hash.update(data)

        # 验证合并后的文件哈希
        final_hash = total_hash.hexdigest()
        if final_hash != file_record.hash_sha256:
            logger.error(
                "merged file hash mismatch file_id=%s expected=%s actual=%s",
                file_record.file_id,
                file_record.hash_sha256,
                final_hash,
            )
            # 删除错误的合并文件
            final_path.unlink(missing_ok=True)
            # 清理分片记录和临时文件
            for chunk in chunks:
                try:
                    Path(chunk.temp_path).unlink(missing_ok=True)
                except Exception:
                    pass
                session.delete(chunk)
            # 删除文件记录
            session.delete(file_record)
            await session.commit()
            raise merged_file_hash_mismatch_exception

        # 合并成功，清理分片记录和临时文件
        for chunk in chunks:
            try:
                Path(chunk.temp_path).unlink(missing_ok=True)
            except Exception:
                pass
            session.delete(chunk)

        await session.commit()
        logger.info(
            "chunks merged successfully file_id=%s user=%s final_path=%s",
            file_record.file_id,
            current_user.id,
            final_path,
        )
        return True

    except Exception as exc:
        logger.error(
            "chunk validation and merge failed file_id=%s user=%s error=%s",
            file_record.file_id,
            current_user.id,
            exc,
        )
        raise chunk_merge_failed_exception from exc


# 预检函数
async def preflight_create_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    storage_type: str = Form("local"),
    filename: str = Form(...),
    hash_sha256: str = Form(...),
    size: int = Form(...),
    # 【实验类型】由前端指定，用于后续查询时定位对应实验表
    experiment_type: str = Form(..., description="实验类型（imzML/raw等）"),
    # 样本信息字段
    organism: str = Form(..., description="样本所属物种"),
    organism_part: str = Form(..., description="样本来源组织"),
    condition: str = Form(..., description="样本状态"),
    sample_growth_conditions: str = Form("", description="样本生长或处理条件（选填）"),
    sample_stabilisation: str = Form(..., description="样本稳定方式"),
    tissue_modification: str = Form(..., description="组织修饰方式"),
    maldi_matrix: str = Form(..., description="MALDI 基质"),
    maldi_matrix_application: str = Form(..., description="基质施加方式"),
    solvent: str = Form("", description="使用溶剂（选填）"),
) -> FilePublic:
    """
    预检：根据前端提供的元数据创建文件记录，但不写入用户 file_ids 与计数。
    仅当后续上传校验通过后，才在上传接口中更新用户计数与 file_ids。
    """
    ensure_storage_dirs()
    # 清理过期预检记录
    try:
        # safe_cleanup_preflight_record(session, current_user, pre_record)  # 安全清理预检记录
        cleanup_stale_preflights(
            session, current_user
        )  # 清理过期预检记录，只在预检的时候调用一次
    except Exception:
        pass
    logger.info(
        "preflight start user=%s filename=%s storage=%s size=%s",
        current_user.id,
        filename,
        storage_type,
        size,
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

    # 检查 1：用户是否已上传过同名文件（通过 file_ids 检查）
    user_file_ids = set(current_user.file_ids or [])
    if user_file_ids:
        # 查询用户已拥有的所有文件
        user_files = (
            await session.exec(
                select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))
            )
        ).all()
        for user_file in user_files:
            # 检查文件名是否重复
            if user_file.filename == filename:
                logger.warning(
                    "preflight rejected: user=%s already has file with name=%s",
                    current_user.id,
                    filename,
                )
                raise duplicate_file_exception  # 表示该用户已经上传过相同的文件

    # 检查 2：是否存在相同内容的文件（通过 hash 检查）
    same_hash_records = (
        await session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256))
    ).all() or []
    for rec in same_hash_records:
        # 检查用户是否已拥有该文件
        if (rec.file_id in user_file_ids) or (current_user.id in set(rec.owners or [])):
            logger.warning(
                "preflight rejected: user=%s already has file with hash=%s",
                current_user.id,
                hash_sha256,
            )
            raise duplicate_file_exception  # 表示该用户已经上传过相同的文件

        # 发现相同 hash 的文件，先将当前用户添加到 owners 列表中
        current_owners = list(rec.owners or [])
        if current_user.id not in current_owners:
            current_owners.append(current_user.id)
            rec.owners = current_owners
            session.add(rec)
            session.flush()  # 立即写入数据库但不提交

        # 检查该文件路径是否存在（文件是否已完成上传）
        if Path(rec.path).exists():
            # 文件已存在，直接将文件ID添加到用户的 file_ids 中，并更新用户统计
            append_file_id(session, current_user, rec.file_id)
            current_user.file_count = (current_user.file_count or 0) + 1
            current_user.total_file_size = (current_user.total_file_size or 0) + (
                rec.size or 0
            )
            session.add(current_user)
            await session.commit()
            await session.refresh(rec)
            await session.refresh(current_user)
            logger.info(
                "preflight reused existing completed file user=%s file_id=%s filename=%s (file already uploaded by others, no need to upload again)",
                current_user.id,
                rec.file_id,
                rec.filename,
            )
            return rec
        else:
            # 文件路径不存在，说明是预检记录，返回该记录供后续上传使用
            await session.commit()
            await session.refresh(rec)
            logger.info(
                "preflight found existing record user=%s file_id=%s filename=%s (added to owners, file not yet completed)",
                current_user.id,
                rec.file_id,
                rec.filename,
            )
            return rec

    final_path = prepare_local_storage_path(current_user.id, filename)
    file_type = final_path.suffix.lstrip(".")
    file_record = FileInDB(
        filename=filename,
        path=str(final_path),
        owners=[current_user.id],  # 预期归属，但不立即写入用户 file_ids
        file_type=file_type,
        experiment_type=experiment_type,
        size=size,
        storage_type=storage_type,
        hash_sha256=hash_sha256,
        # 样本信息字段
        organism=organism,
        organism_part=organism_part,
        condition=condition,
        sample_growth_conditions=sample_growth_conditions,
        sample_stabilisation=sample_stabilisation,
        tissue_modification=tissue_modification,
        maldi_matrix=maldi_matrix,
        maldi_matrix_application=maldi_matrix_application,
        solvent=solvent,
    )
    session.add(file_record)
    await session.commit()
    await session.refresh(file_record)
    logger.info(
        "preflight created new file record user=%s file_id=%s filename=%s size=%s (not yet in user file_ids)",
        current_user.id,
        file_record.file_id,
        file_record.filename,
        file_record.size,
    )
    return file_record


# 分片记录预检
async def preflight_create_chunked_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    storage_type: str = Form("local"),
    filename: str = Form(...),
    hash_sha256: str = Form(...),
    size: int = Form(...),
    total_parts: int = Form(...),
    # 【实验类型】由前端指定，用于后续查询时定位对应实验表
    experiment_type: str = Form(..., description="实验类型（imzML/raw等）"),
    # 样本信息字段
    organism: str = Form(..., description="样本所属物种"),
    organism_part: str = Form(..., description="样本来源组织"),
    condition: str = Form(..., description="样本状态"),
    sample_growth_conditions: str = Form("", description="样本生长或处理条件（选填）"),
    sample_stabilisation: str = Form(..., description="样本稳定方式"),
    tissue_modification: str = Form(..., description="组织修饰方式"),
    maldi_matrix: str = Form(..., description="MALDI 基质"),
    maldi_matrix_application: str = Form(..., description="基质施加方式"),
    solvent: str = Form("", description="使用溶剂（选填）"),
) -> FilePublic:
    """
    分片上传预检：创建文件记录，准备接收分片
    """
    ensure_storage_dirs()
    # 清理过期记录
    try:
        cleanup_stale_preflights(
            session, current_user
        )  # 清理过期预检记录，只在预检的时候调用一次
        cleanup_stale_chunks(
            session, current_user
        )  # 清理过期分片记录，只在分片预检的时候调用一次
    except Exception:
        pass

    logger.info(
        "chunked preflight start user=%s filename=%s storage=%s size=%s parts=%s",
        current_user.id,
        filename,
        storage_type,
        size,
        total_parts,
    )

    # 基础校验
    if storage_type not in ALLOWED_STORAGE_TYPES:
        raise storage_type_not_allowed_exception
    if storage_type != "local":
        raise storage_type_not_supported_exception
    if total_parts <= 0:
        raise total_parts_must_greater_than_exception  # 分片数要大于0
    if total_parts > 20:
        raise total_part_too_much_exception  # 分片数太多了

    # 文件安全与类型校验
    validate_filename(filename)
    validate_file_type(filename)

    # 用户配额校验
    current_count = current_user.file_count or len(current_user.file_ids or [])
    if current_count >= MAX_FILES_PER_USER:
        raise user_file_limit_exception
    if size > MAX_FILE_SIZE:
        raise file_size_limit_exception
    projected_total_size = (current_user.total_file_size or 0) + size
    if projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        raise user_storage_limit_exception

    # 检查 1：用户是否已上传过同名文件（通过 file_ids 检查）
    user_file_ids = set(current_user.file_ids or [])
    if user_file_ids:
        # 查询用户已拥有的所有文件
        user_files = (
            await session.exec(
                select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))
            )
        ).all()
        for user_file in user_files:
            # 检查文件名是否重复
            if user_file.filename == filename:
                logger.warning(
                    "chunked preflight rejected: user=%s already has file with name=%s",
                    current_user.id,
                    filename,
                )
                raise duplicate_file_exception  # 表示该用户已经上传过同名文件

    # 检查 2：是否存在相同内容的文件（通过 hash 检查）
    same_hash_records = (
        await session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256))
    ).all() or []
    for rec in same_hash_records:
        # 检查用户是否已拥有该文件
        if (rec.file_id in user_file_ids) or (current_user.id in set(rec.owners or [])):
            logger.warning(
                "chunked preflight rejected: user=%s already has file with hash=%s",
                current_user.id,
                hash_sha256,
            )
            raise duplicate_file_exception  # 表示该用户已经上传过相同的文件

        # 发现相同 hash 的文件，先将当前用户添加到 owners 列表中
        current_owners = list(rec.owners or [])
        if current_user.id not in current_owners:
            current_owners.append(current_user.id)
            rec.owners = current_owners

        # 分片复用时需要修改 total_parts 字段
        rec.total_parts = total_parts
        session.add(rec)
        session.flush()  # 立即写入数据库但不提交

        # 检查该文件路径是否存在（文件是否已完成上传）
        if Path(rec.path).exists():
            # 文件已存在，直接将文件ID添加到用户的 file_ids 中，并更新用户统计
            append_file_id(session, current_user, rec.file_id)
            current_user.file_count = (current_user.file_count or 0) + 1
            current_user.total_file_size = (current_user.total_file_size or 0) + (
                rec.size or 0
            )
            session.add(current_user)
            await session.commit()
            await session.refresh(rec)
            await session.refresh(current_user)
            logger.info(
                "chunked preflight reused existing completed file user=%s file_id=%s filename=%s (file already uploaded by others, no need to upload again)",
                current_user.id,
                rec.file_id,
                rec.filename,
            )
            return rec
        else:
            # 文件路径不存在，说明是预检记录，返回该记录供后续分片上传使用
            await session.commit()
            await session.refresh(rec)
            logger.info(
                "chunked preflight found existing record user=%s file_id=%s filename=%s (added to owners, updated total_parts=%s, file not yet completed)",
                current_user.id,
                rec.file_id,
                rec.filename,
                total_parts,
            )
            return rec

    # 检查相同文件名的文件（不同内容但同名）
    same_name_records = (
        await session.exec(select(FileInDB).where(FileInDB.filename == filename))
    ).all() or []
    for rec in same_name_records:
        # 检查用户是否已拥有同名文件
        if (rec.file_id in user_file_ids) or (current_user.id in set(rec.owners or [])):
            logger.warning(
                "chunked preflight rejected: user=%s already has file with name=%s",
                current_user.id,
                filename,
            )
            raise duplicate_file_exception  # 该用户已经上传过同名文件

    # 创建新的分片预检记录（注意：此时不写入用户的 file_ids）
    # owners 字段记录预期归属，但用户表的 file_ids 只在分片上传完成并合并成功后才更新
    final_path = prepare_local_storage_path(current_user.id, filename)
    file_type = final_path.suffix.lstrip(".")
    file_record = FileInDB(
        filename=filename,
        path=str(final_path),
        owners=[current_user.id],  # 预期归属，但不立即写入用户 file_ids
        file_type=file_type,
        experiment_type=experiment_type,
        size=size,
        storage_type=storage_type,
        hash_sha256=hash_sha256,
        total_parts=total_parts,  # 分片上传特有字段
        # 样本信息字段
        organism=organism,
        organism_part=organism_part,
        condition=condition,
        sample_growth_conditions=sample_growth_conditions,
        sample_stabilisation=sample_stabilisation,
        tissue_modification=tissue_modification,
        maldi_matrix=maldi_matrix,
        maldi_matrix_application=maldi_matrix_application,
        solvent=solvent,
    )
    session.add(file_record)
    await session.commit()
    await session.refresh(file_record)

    logger.info(
        "chunked preflight created new file record user=%s file_id=%s filename=%s size=%s parts=%s (not yet in user file_ids)",
        current_user.id,
        file_record.file_id,
        file_record.filename,
        file_record.size,
        total_parts,
    )
    return file_record


# 上传单个文件分片
async def upload_file_chunk(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    background_tasks: BackgroundTasks,
    file_hash: str = Form(...),
    part_number: int = Form(...),
    part_hash: str = Form(...),
    chunk: UploadFile = File(...),
) -> dict:
    """
    上传单个文件分片，每次上传后验证并尝试合并
    """
    ensure_storage_dirs()
    logger.info(
        "chunk upload start user=%s file_hash=%s part=%s",
        current_user.id,
        file_hash,
        part_number,
    )

    # 验证分片编号
    if part_number <= 0:
        raise invalid_part_number_exception

    # 验证对应的文件记录存在
    file_record = (
        await session.exec(select(FileInDB).where(FileInDB.hash_sha256 == file_hash))
    ).first()
    if not file_record:
        raise file_not_found_exception

    # 验证用户权限
    await assert_file_owner(file_record, current_user)

    # 验证分片编号不超出总分片数
    if file_record.total_parts and part_number > file_record.total_parts:
        logger.error(
            "part number %s exceeds total_parts %s for file_id=%s user=%s",
            part_number,
            file_record.total_parts,
            file_record.file_id,
            current_user.id,
        )
        raise part_number_out_of_range_exception

    # 检查分片是否已存在
    existing_chunk = (
        await session.exec(
            select(PartInDB).where(
                PartInDB.file_id == file_record.file_id,
                PartInDB.part_number == part_number,
                PartInDB.owner_id == current_user.id,
            )
        )
    ).first()
    if existing_chunk:
        logger.warning(
            "chunk already exists user=%s file_id=%s part=%s",
            current_user.id,
            file_record.file_id,
            part_number,
        )
        raise part_already_exists_exception

    # 计算分片哈希和大小
    chunk_hash, chunk_size = await compute_upload_file_metadata(chunk)
    if chunk_hash != part_hash:
        raise preflight_hash_mismatch_exception

    # 保存分片到临时目录
    temp_path = prepare_temp_chunk_path(current_user.id, file_hash, part_number)
    try:
        await save_upload_file(chunk, temp_path)
    except Exception as exc:
        logger.error(
            "save chunk failed user=%s file_id=%s part=%s error=%s",
            current_user.id,
            file_record.file_id,
            part_number,
            exc,
        )
        temp_path.unlink(missing_ok=True)
        raise file_persist_failed_exception from exc

    # 创建分片记录
    part_record = PartInDB(
        file_hash=file_hash,
        part_number=part_number,
        part_hash=part_hash,
        part_size=chunk_size,
        temp_path=str(temp_path),
        owner_id=current_user.id,
        file_id=file_record.file_id,
        total_file_size=file_record.size,
        total_parts=file_record.total_parts or 0,
    )
    session.add(part_record)
    await session.commit()

    logger.info(
        "chunk saved user=%s file_id=%s part=%s size=%s",
        current_user.id,
        file_record.file_id,
        part_number,
        chunk_size,
    )

    # 每次上传分片后都调用验证和合并函数
    try:
        merge_success = await validate_and_merge_chunks(
            session, file_record, current_user
        )  # 每次都会检查是否满足合并条件
        if merge_success:
            # 合并成功，更新用户统计
            append_file_id(session, current_user, file_record.file_id)
            current_user.file_count = (current_user.file_count or 0) + 1
            current_user.total_file_size = (
                current_user.total_file_size or 0
            ) + file_record.size
            session.add(current_user)
            await session.commit()
            await session.refresh(current_user)

            logger.info(
                "chunked upload completed user=%s file_id=%s",
                current_user.id,
                file_record.file_id,
            )

            # 如果是ZIP文件，自动触发解压处理
            if file_record.file_type == "zip":
                experiment_name = Path(file_record.filename).stem
                background_tasks.add_task(
                    process_imzml_experiment_background,
                    current_user.id,
                    file_record.file_id,
                    experiment_name,
                )
                logger.info(
                    "auto extraction triggered for chunked ZIP file user=%s file_id=%s",
                    current_user.id,
                    file_record.file_id,
                )

            return {
                "part_number": part_number,
                "status": "uploaded_and_merged",
                "file_id": file_record.file_id,
                "message": "all chunks uploaded and merged successfully",
            }
    except HTTPException:
        # 验证或合并失败，重新抛出异常
        raise
    except Exception as exc:
        # 其他未预期的异常，记录日志并抛出通用异常
        logger.error(
            "unexpected error during chunk upload user=%s file_id=%s part=%s error=%s",
            current_user.id,
            file_record.file_id,
            part_number,
            exc,
        )
        raise chunk_merge_failed_exception from exc

    # 分片上传成功但尚未完成合并
    return {
        "part_number": part_number,
        "status": "uploaded",
        "file_id": file_record.file_id,
        "message": "chunk uploaded successfully, waiting for more chunks",
    }


# 查询分片状态
async def get_chunked_upload_status(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> ChunkedUploadStatus:
    """
    查询分片上传状态 - 通过 file_id 查询，节约资源
    """
    # 通过 file_id 直接查询文件记录（更高效）
    file_record = await get_file_record(file_id, session)

    # 验证用户权限
    await assert_file_owner(file_record, current_user)

    # 检查文件是否已完成（存在于用户file_ids中）
    user_file_ids = set(current_user.file_ids or [])
    if file_record.file_id in user_file_ids:
        # 文件已完成
        return ChunkedUploadStatus(
            file_hash=file_record.hash_sha256 or "",
            filename=file_record.filename,
            total_parts=0,  # 已完成，不需要分片信息
            uploaded_parts=[],
            total_size=file_record.size,
            is_complete=True,
        )

    # 查询已上传的分片（使用 file_id 索引，更高效）
    uploaded_chunks = (
        await session.exec(
            select(PartInDB)
            .where(PartInDB.file_id == file_id, PartInDB.owner_id == current_user.id)
            .order_by(PartInDB.part_number)
        )
    ).all()

    uploaded_parts = [chunk.part_number for chunk in uploaded_chunks]

    return ChunkedUploadStatus(
        file_hash=file_record.hash_sha256 or "",
        filename=file_record.filename,
        total_parts=file_record.total_parts or 0,
        uploaded_parts=uploaded_parts,
        total_size=file_record.size,
        is_complete=False,
    )


# 普通文件上传函数
async def upload_simple_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    background_tasks: BackgroundTasks,
    storage_type: str = Form("local"),
    pre_file_id: int = Form(...),
    file: UploadFile = File(...),
):
    """
    简单上传接口：一次性接收完整文件，不做分片。
    - 根据 storage_type 选择存储方式，目前仅支持 local
    - 自动计算文件大小与 SHA256
    """
    ensure_storage_dirs()
    logger.info(
        "simple upload start user=%s filename=%s storage=%s",
        current_user.id,
        file.filename,
        storage_type,
    )
    # 校验part
    # 基础校验：
    if storage_type not in ALLOWED_STORAGE_TYPES:
        raise storage_type_not_allowed_exception  # 存储类型不在允许列表异常

    if storage_type != "local":
        raise storage_type_not_supported_exception  # 存储类型尚未实现异常

    # 预检记录验证
    pre_record = await get_file_record(pre_file_id, session)
    await assert_file_owner(pre_record, current_user)

    # 再次检查用户是否已上传过该文件（防止预检后到上传前的状态变化）
    user_file_ids = set(current_user.file_ids or [])
    if user_file_ids:
        user_files = (
            await session.exec(
                select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))
            )
        ).all()
        for user_file in user_files:
            if user_file.filename == file.filename:
                # safe_cleanup_preflight_record(session, current_user, pre_record)
                await session.commit()
                logger.warning(
                    "upload rejected: user=%s already has file with name=%s",
                    current_user.id,
                    file.filename,
                )
                raise duplicate_file_exception  # 信息为该用户已经上传该文件

    # 存储类型与文件名一致性校验
    if storage_type != pre_record.storage_type:
        safe_cleanup_preflight_record(
            session, current_user, pre_record
        )  # 安全清理预检记录
        await session.commit()
        raise storage_type_not_allowed_exception
    if not file.filename:
        raise filename_invalid_exception
    validate_filename(file.filename)
    validate_file_type(file.filename)
    if file.filename != pre_record.filename:
        safe_cleanup_preflight_record(
            session, current_user, pre_record
        )  # 安全清理预检记录
        await session.commit()
        raise preflight_filename_mismatch_exception  # 文件名不匹配

    # 计算实际文件hash与大小并与预检记录比对
    hash_sha256, size = await compute_upload_file_metadata(file)
    if size != pre_record.size:
        safe_cleanup_preflight_record(session, current_user, pre_record)
        await session.commit()
        raise preflight_size_mismatch_exception
    if hash_sha256 != (pre_record.hash_sha256 or ""):
        safe_cleanup_preflight_record(session, current_user, pre_record)
        await session.commit()
        logger.error(
            "preflight hash mismatch user=%s expected=%s actual=%s",
            current_user.id,
            pre_record.hash_sha256,
            hash_sha256,
        )
        raise preflight_hash_mismatch_exception

    # 用户配额（再次轻量校验，防止预检后状态变化）
    current_count = current_user.file_count or len(current_user.file_ids or [])
    if current_count >= MAX_FILES_PER_USER:
        raise user_file_limit_exception
    projected_total_size = (current_user.total_file_size or 0) + size
    if projected_total_size > MAX_TOTAL_STORAGE_PER_USER:
        raise user_storage_limit_exception  # 用户存储空间已满异常

    # 检查预检记录是否已经建立了文件复用关系
    # 如果当前用户已经在预检记录的 owners 中，且存在其他相同 hash 的文件，说明预检阶段已经复用了
    if current_user.id in set(pre_record.owners or []):
        same_hash_records = (
            await session.exec(
                select(FileInDB).where(
                    FileInDB.hash_sha256 == hash_sha256,
                    FileInDB.file_id != pre_record.file_id,  # 排除当前预检记录
                )
            )
        ).all() or []

        for rec in same_hash_records:
            try:
                if not Path(rec.path).exists():
                    continue
            except Exception:
                continue

            append_file_id(session, current_user, rec.file_id)
            current_user.file_count = (current_user.file_count or 0) + 1
            current_user.total_file_size = (current_user.total_file_size or 0) + (
                rec.size or 0
            )

            # 确保用户在 owners 列表中
            if current_user.id not in set(rec.owners or []):
                rec.owners = list(dict.fromkeys((rec.owners or []) + [current_user.id]))
                session.add(rec)

            session.add(current_user)

            # 关闭上传文件句柄，避免资源泄露
            try:
                await file.close()
            except Exception:
                pass

            # 使用安全清理函数处理预检记录（因为我们复用了已存在的文件）
            # safe_cleanup_preflight_record(session, current_user, pre_record)
            await session.commit()
            await session.refresh(rec)
            logger.info(
                "simple upload reused existing file user=%s file_id=%s filename=%s (no file write to storage)",
                current_user.id,
                rec.file_id,
                rec.filename,
            )
            return rec  # 复用成功，直接返回已存在的文件记录
    final_path = prepare_local_storage_path(current_user.id, file.filename)
    try:
        await save_upload_file(
            file, final_path
        )  # 使用 aiofiles 将 UploadFile 内容写到指定路径。
    except Exception as exc:
        logger.error(
            "save upload file failed user=%s filename=%s error=%s",
            current_user.id,
            file.filename,
            exc,
        )
        final_path.unlink(missing_ok=True)
        safe_cleanup_preflight_record(session, current_user, pre_record)
        await session.commit()
        raise file_persist_failed_exception from exc
    # 刷新 file表
    # 使用预检创建的记录作为最终记录
    file_record = pre_record
    session.flush()
    # 刷新 用户表
    append_file_id(session, current_user, file_record.file_id)
    current_user.file_count = (current_user.file_count or 0) + 1
    current_user.total_file_size = (current_user.total_file_size or 0) + size
    await session.commit()  # 提交事务
    await session.refresh(file_record)
    await session.refresh(current_user)
    logger.info(
        "simple upload completed user=%s file_id=%s filename=%s size=%s",
        current_user.id,
        file_record.file_id,
        file_record.filename,
        file_record.size,
    )

    # 如果是ZIP文件，自动触发解压处理
    if file_record.file_type == "zip":
        experiment_name = Path(file_record.filename).stem  # 使用文件名作为实验名称
        background_tasks.add_task(
            process_imzml_experiment_background,
            current_user.id,
            file_record.file_id,
            experiment_name,
        )
        logger.info(
            "auto extraction triggered for ZIP file user=%s file_id=%s",
            current_user.id,
            file_record.file_id,
        )

    return file_record


# 主查询函数
async def list_user_files(
    current_user: Annotated[UserInDB, Depends(token_authen)], session: SessionDep
) -> List[FilePublic]:
    """
    查询当前用户的全部文件记录，依据用户记录的文件 ID 列表过滤
    """
    if not current_user.file_ids:
        return []
    seen_ids = list(dict.fromkeys(current_user.file_ids))[
        :MAX_FILES_PER_USER
    ]  # 只提取MAX_FILE_PER_USER限定的前几个文件
    if not seen_ids:
        return []

    statement = select(FileInDB).where(FileInDB.file_id.in_(seen_ids))
    results = (await session.exec(statement)).all()
    files = [record for record in results]

    logger.debug("list user files user=%s count=%s", current_user.id, len(files))
    return files


# 主删除函数
async def delete_user_file(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> dict:
    """
    删除当前用户的指定文件：
    - 校验所有权
    - 删除关联的实验记录和解压后的文件（imzML/ibd）
    - 移除数据库记录（仅当无其他用户持有时）
    注意：原始 ZIP 文件在解压时已被删除，此处不再处理
    """

    file_record = await get_file_record(file_id, session)
    await assert_file_owner(file_record, current_user)  # 权限校验（管理员或拥有索引者）

    # 1) 先从当前用户的索引中移除
    remove_file_id(session, current_user, file_id)
    current_user.file_count = max(0, (current_user.file_count or 0) - 1)
    current_user.total_file_size = max(
        0, (current_user.total_file_size or 0) - file_record.size
    )
    session.add(current_user)
    session.flush()

    # 2) 同步更新文件记录的 owners，移除当前用户归属
    current_owners = list(file_record.owners or [])
    if current_user.id in current_owners:
        current_owners.remove(current_user.id)
    file_record.owners = current_owners
    session.add(file_record)
    session.flush()

    # 3) 检查是否还有其他用户持有该文件（以用户表的 file_ids 字段为准）
    # 查询所有用户，检查是否有人的 file_ids 中包含该文件
    all_users = (await session.exec(select(UserInDB))).all()
    other_users_have_file = False

    for user in all_users:
        if user.id == current_user.id:
            continue  # 跳过当前用户
        if file_id in (user.file_ids or []):
            # 发现其他用户仍持有该文件
            other_users_have_file = True
            logger.info(
                "found other user still has file user_id=%s file_id=%s",
                user.id,
                file_id,
            )
            break

    if other_users_have_file:
        # 其他用户仍持有该文件，仅解除当前用户关联，不删除文件
        await session.commit()
        await session.refresh(current_user)
        logger.info(
            "unlinked file reference only user=%s file_id=%s (other users still have this file)",
            current_user.id,
            file_id,
        )
        return {"file_id": file_id, "status": "unlinked"}

    # 4) 确认无其他用户持有，删除关联的实验记录和解压后的文件
    # 【删除实验记录】根据 experiment_type 查找并删除对应实验表记录
    experiment_type = file_record.experiment_type
    if experiment_type == "imzML":
        # 查找 imzML 实验记录
        experiment = session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_id
            )
        ).first()

        if experiment:
            # 删除解压后的 imzML 和 ibd 文件
            if experiment.imzml_path:
                await asyncio.to_thread(
                    lambda: Path(experiment.imzml_path).unlink(missing_ok=True)
                )
                logger.info("deleted imzml file path=%s", experiment.imzml_path)
            if experiment.ibd_path:
                await asyncio.to_thread(
                    lambda: Path(experiment.ibd_path).unlink(missing_ok=True)
                )
                logger.info("deleted ibd file path=%s", experiment.ibd_path)

            # 删除实验目录（如果为空）
            if experiment.imzml_path:
                experiment_dir = Path(experiment.imzml_path).parent

                def _rmdir_if_empty(d: Path):
                    if d.exists() and not any(d.iterdir()):
                        d.rmdir()

                await asyncio.to_thread(_rmdir_if_empty, experiment_dir)

            # 删除实验数据库记录
            session.delete(experiment)
            logger.info(
                "deleted experiment record exp_id=%s file_id=%s", experiment.id, file_id
            )

    # 【预留】其他实验类型的删除逻辑
    # elif experiment_type == "raw":
    #     # 删除 RAW 实验记录和文件
    #     pass

    # 5) 删除文件数据库记录（原始 ZIP 文件在解压时已删除，无需删除物理文件）
    session.delete(file_record)
    await session.commit()
    await session.refresh(current_user)
    logger.info(
        "deleted file content and record user=%s file_id=%s", current_user.id, file_id
    )
    return {"file_id": file_id, "status": "deleted"}


# 主下载函数
async def get_download_file_metadata(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> FileDownloadMeta:
    """
    返回文件元数据和路径，不处理文件流
    - 校验身份与所有权
    - 如果文件路径存在，直接返回
    - 如果文件路径为空（ZIP已删除），从实验表查找并压缩 imzML/ibd 文件
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
        raise download_not_implemented_exception

    path = Path(file_record.path) if file_record.path else None

    # 【检查文件路径是否存在】
    if path and path.exists():
        # 文件存在，直接返回
        pass
    else:
        # 文件路径为空或不存在，需要从实验表查找并压缩
        experiment_type = file_record.experiment_type

        if experiment_type == "imzML":
            # 查找 imzML 实验记录（通过 source_file_id 查询，不需要 user_id 前面验证过了）
            experiment = session.exec(
                select(ExperimentImzMLInDB).where(
                    ExperimentImzMLInDB.source_file_id == file_id
                )
            ).first()

            if not experiment or experiment.status != ExperimentStatus.COMPLETED:
                logger.warning(
                    "experiment not found or not completed file_id=%s", file_id
                )
                raise file_content_missing_exception

            # 检查 imzML 和 ibd 文件是否存在
            imzml_path = Path(experiment.imzml_path) if experiment.imzml_path else None
            ibd_path = Path(experiment.ibd_path) if experiment.ibd_path else None

            if (
                not imzml_path
                or not ibd_path
                or not imzml_path.exists()
                or not ibd_path.exists()
            ):
                logger.warning("experiment files missing file_id=%s", file_id)
                raise file_content_missing_exception

            # 【压缩文件】保存到下载临时目录
            zip_filename = file_record.filename
            DOWNLOAD_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
            zip_path = DOWNLOAD_TEMP_ROOT / zip_filename

            # 异步压缩
            def _create_zip():
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.write(imzml_path, imzml_path.name)
                    zf.write(ibd_path, ibd_path.name)

            await asyncio.to_thread(_create_zip)

            # 更新文件表中的路径和大小（写回数据库）
            file_record.path = str(zip_path)
            file_record.size = zip_path.stat().st_size

            logger.info(
                "created download zip file_id=%s path=%s size=%s",
                file_id,
                zip_path,
                file_record.size,
            )

        # 【预留】其他实验类型
        # elif experiment_type == "raw":
        #     pass
        else:
            raise file_content_missing_exception

    # 更新下载计数（管理员和普通用户一样）
    file_record.download_count += 1
    current_user.download_used = (current_user.download_used or 0) + 1
    session.add(current_user)
    session.add(file_record)
    await session.commit()
    logger.info(
        "download success user=%s file_id=%s filename=%s",
        current_user.id,
        file_id,
        file_record.filename,
    )

    # 构造返回的文件元数据
    return FileDownloadMeta.from_record(file_record)


DeleteFileDep = Annotated[dict, Depends(delete_user_file)]
SimpleUploadDep = Annotated[FilePublic, Depends(upload_simple_file)]  # 上传普通函数
ListFilesDep = Annotated[List[FilePublic], Depends(list_user_files)]
DownloadFileDep = Annotated[FileDownloadMeta, Depends(get_download_file_metadata)]
PreflightDep = Annotated[FilePublic, Depends(preflight_create_file)]  # 普通文件上传预检
ChunkedPreflightDep = Annotated[
    FilePublic, Depends(preflight_create_chunked_file)
]  # 分片上传预检
ChunkUploadDep = Annotated[dict, Depends(upload_file_chunk)]  # 上传单个文件分片
ChunkedStatusDep = Annotated[
    ChunkedUploadStatus, Depends(get_chunked_upload_status)
]  # 查询分片状态
