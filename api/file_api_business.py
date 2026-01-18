#########################
# File API Business - 业务逻辑层
# 职责：业务逻辑处理，数据库操作，但不直接暴露API
# 包含：用户文件关联管理、文件记录查询、权限校验、清理逻辑、分片合并
#########################
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import aiofiles
from sqlmodel import select

from api.file_api_helper import delete_local_file
from api.file_api_helper import check_user_quota
from constants import (
    CHUNK_BUFFER_SIZE,
    MAX_FILES_PER_USER,
    PREFLIGHT_TTL_MINUTES,
)
from exceptions import (
    chunk_file_missing_exception,
    duplicate_file_exception,
    incomplete_part_sequence_exception,
    invalid_user_for_file_list_sync_exception,
    merged_file_hash_mismatch_exception,
    user_file_limit_exception,
)
from logger import get_logger
from models.file_models import FileInDB, PartInDB, now_local_timezone, FileCreate
from models.user_models import UserInDB
from models.experiment_models import ExperimentImzMLInDB
from database.db import SessionDep

logger = get_logger("main.file_api_business")



def append_file_id(session: SessionDep, user: UserInDB, file_id: int) -> None:
    """
    将文件ID添加到用户的文件列表中
    
    上传成功后调用，建立用户与文件的关联关系
    注意：只执行 session.add()，不执行 commit，由调用方统一提交事务
    
    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    file_ids = list(user.file_ids or [])

    # 文件已在列表中，无需重复添加
    if file_id in file_ids:
        return

    # 检查是否超过文件数量限制
    if len(file_ids) >= MAX_FILES_PER_USER:
        raise user_file_limit_exception

    file_ids.append(file_id)
    user.file_ids = file_ids
    session.add(user)


def link_file_to_user(
    session: SessionDep,
    user: UserInDB,
    file_id: int,
    file_size: int,
) -> None:
    """
    将文件关联到用户，并更新用户的文件统计信息
    
    完整的文件关联流程：
    1. 将文件ID添加到用户的 file_ids 列表
    2. 增加用户的文件计数
    3. 增加用户的总存储空间
    4. 将用户对象添加到会话（准备提交）
    
    注意：只执行 session.add()，不执行 commit，由调用方统一提交事务
    
    Args:
        session: 数据库会话
        user: 用户对象
        file_id: 文件ID
        file_size: 文件大小（字节）
        
    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    # 1. 将文件ID添加到用户的文件列表
    append_file_id(session, user, file_id)
    
    # 2. 更新用户的文件统计信息
    user.file_count = (user.file_count or 0) + 1
    user.total_file_size = (user.total_file_size or 0) + file_size
    
    # 3. 将用户对象添加到会话
    session.add(user)


async def validate_and_try_reuse_file(
    session: SessionDep,
    current_user: UserInDB,
    file_data: FileCreate,
) -> FileInDB | None:
    """
    【文件上传预检】验证上传条件并尝试秒传优化
    
    这是文件上传的完整预检流程，包含验证和优化两部分：
    
    验证部分（必须通过，否则抛出异常）：
    1. 用户配额检查 - 确保用户有足够的存储空间
    2. 重复文件名检查 - 确保用户没有同名文件
    3. 重复文件哈希检查 - 确保用户没有重复上传相同内容
    
    优化部分（秒传机制）：
    4. 尝试复用其他用户已上传的相同哈希文件
       - 如果其他用户已上传过相同文件，直接复用（秒传）
       - 避免重复存储，节省空间和带宽
       - 这是现代文件系统的标准优化，不是可选功能
    
    为什么第3步和第4步不矛盾？
    - 第3步：检查**当前用户**是否已拥有该文件 → 有则拒绝
    - 第4步：查找**其他用户**是否已上传该文件 → 有则复用
    
    事务管理：不 commit，由调用方统一管理
    异常处理：验证失败时抛出对应的异常
    
    Args:
        session: 数据库会话
        current_user: 当前用户对象
        file_data: 文件元数据（包含哈希、大小、文件名等）
        
    Returns:
        FileInDB | None: 
        - 如果找到可复用的文件，返回该文件记录（秒传成功）
        - 如果没有可复用的文件，返回 None（需要实际上传）
        
    Raises:
        user_file_limit_exception: 用户配额不足（文件数量或存储空间）
        duplicate_file_exception: 文件名重复或用户已拥有该文件
    """
    
    # 1. 用户配额检查（工具层）
    if not check_user_quota(current_user, file_data.size):
        logger.warning(
            "quota check failed user=%s filename=%s size=%s",
            current_user.id,
            file_data.filename,
            file_data.size,
        )
        raise user_file_limit_exception

    # 2. 重复文件名检查（业务层）
    if await check_duplicate_filename(session, current_user, file_data.filename):
        logger.warning(
            "duplicate filename user=%s filename=%s",
            current_user.id,
            file_data.filename,
        )
        raise duplicate_file_exception

    # 3. 重复文件哈希检查（业务层）
    if await check_duplicate_file_hash(session, current_user, file_data.hash_sha256):
        logger.warning(
            "duplicate file hash user=%s hash=%s",
            current_user.id,
            file_data.hash_sha256,
        )
        raise duplicate_file_exception

    # 4. 尝试复用相同哈希的文件（业务层）
    reused_file = await try_reuse_file_by_hash(session, current_user, file_data.hash_sha256)
    return reused_file


def remove_file_id(session: SessionDep, user: UserInDB, file_id: int) -> None:
    """
    从用户的文件列表中移除指定文件ID
    
    删除文件时调用，解除用户与文件的关联关系
    注意：只执行 session.add()，不执行 commit，由调用方统一提交事务
    
    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    file_ids = list(user.file_ids or [])

    if file_id in file_ids:
        file_ids.remove(file_id)
        user.file_ids = file_ids
        session.add(user)


async def get_file_record(file_id: int, session: SessionDep) -> FileInDB | None:
    """
    根据文件ID查询文件记录
    
    Returns:
        文件记录对象，不存在则返回 None
    """
    statement = select(FileInDB).where(FileInDB.file_id == file_id)
    file_record = (await session.exec(statement)).first()
    return file_record


async def safe_cleanup_preflight_record(#TODO 后续可以用定时任务来清理
    session: SessionDep, current_user: UserInDB, pre_record: FileInDB
) -> None:
    """
    安全清理预检记录
    
    处理逻辑：
    - 如果文件属于多个用户：只移除当前用户的关联，保留记录
    - 如果文件只属于当前用户：删除整条记录
    
    注意：只执行数据库操作，不执行 commit，由调用方统一提交事务
    """
    # 从 owners 列表移除当前用户
    current_owners = list(pre_record.owners or [])
    if current_user.id in current_owners:
        current_owners.remove(current_user.id)
    pre_record.owners = current_owners

    if pre_record.owners:
        # 还有其他用户持有，只更新 owners
        session.add(pre_record)
        logger.debug(
            "preflight cleanup: removed user=%s from file_id=%s owners",
            current_user.id,
            pre_record.file_id,
        )
    else:
        # 没有其他用户持有，删除记录（包括物理文件和分片）
        await delete_file_and_parts(session, pre_record.file_id, current_user)#TODOTODO 如果这里该文件被多个用户拥有  该文件的分片删不掉
        logger.debug(
            "preflight cleanup: deleted file_id=%s completely", pre_record.file_id
        )


async def cleanup_stale_preflights(
    session: SessionDep,
    current_user: UserInDB,
    ttl_minutes: int = PREFLIGHT_TTL_MINUTES,
) -> int:
    """
    清理当前用户过期且未完成的预检记录
    
    清理条件（需同时满足）：
    1. 记录 owners 中包含当前用户
    2. 文件路径不存在（说明为预检记录，未完成上传）
    3. file_id 不在用户 file_ids 中
    4. uploaded_at 超过 TTL
    
    事务管理：自己 commit（清理操作独立于主业务流程）
    异常处理：不捕获，让异常传播到 API 层统一处理
    
    Returns:
        清理的记录数量
    """
    now = now_local_timezone()
    user_file_ids = set(current_user.file_ids or [])
    cleaned = 0

    # 查询 owners 包含当前用户的记录
    records = (#TODO 效率低 后续应该换一种方式来查询
        await session.exec(
            select(FileInDB).where(
                FileInDB.owners.contains([current_user.id])  # pylint: disable=no-member
            )
        )
    ).all()

    for rec in records:
        
        # 检查文件是否存在，存在，不能删
        path_missing = Path(rec.path).exists()
        if path_missing:
            continue

        # 在用户 file_ids 中 = 已完成关联，不能删
        if rec.file_id in user_file_ids:
            continue

        # TTL 判断 - 确保时区一致
        if rec.uploaded_at.tzinfo is None:
            # 如果 uploaded_at 没有时区信息，假设它是本地时区
            rec_time = rec.uploaded_at.replace(tzinfo=now.tzinfo)
        else:
            rec_time = rec.uploaded_at
        
        too_old = (now - rec_time).total_seconds() >= ttl_minutes * 60
        if not too_old:
            continue

        # 满足所有条件，清理预检记录
        await safe_cleanup_preflight_record(session, current_user, rec)
        cleaned += 1

    if cleaned:
        await session.commit()

    return cleaned


async def validate_and_merge_chunks(
    session: SessionDep, file_record: FileInDB, current_user: UserInDB
) -> bool:
    """
    验证分片完整性并尝试合并
    
    每次上传分片后调用，检查是否所有分片都已上传完成
    如果完成则执行合并操作
    
    合并流程：
    1. 查询该文件的所有分片记录
    2. 验证分片编号是否超出范围
    3. 检查分片是否完整（数量和序列）
    4. 尝试复用已存在的相同内容文件
    5. 执行分片合并，边写入边计算哈希
    6. 验证合并后的文件哈希
    7. 清理分片临时文件和记录
    
    事务管理：不 commit，由调用方（API 层）统一管理
    异常处理：不捕获，让异常传播到 API 层
        
    Raises:
        part_number_out_of_range_exception: 分片编号超出范围
        incomplete_part_sequence_exception: 分片序列不完整
        chunk_file_missing_exception: 分片文件丢失
        merged_file_hash_mismatch_exception: 合并后哈希不匹配
    """
    # 1. 查询该文件的所有分片记录
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

    expected_total_parts = file_record.total_parts
    if not expected_total_parts:
        logger.error("total_parts not set for file_id=%s", file_record.file_id)
        return False

    # 2. 检查分片完整性
    uploaded_parts = sorted([chunk.part_number for chunk in chunks])
    expected_parts = list(range(1, expected_total_parts + 1))

    logger.info(
        "chunk status file_id=%s uploaded=%s expected=%s",
        file_record.file_id,
        uploaded_parts,
        expected_parts,
    )

    # 分片不完整，等待更多分片
    if len(uploaded_parts) < expected_total_parts:
        return False

    # 分片序列不连续
    if uploaded_parts != expected_parts:
        logger.error(
            "chunk sequence invalid file_id=%s uploaded=%s expected=%s",
            file_record.file_id,
            uploaded_parts,
            expected_parts,
        )
        raise incomplete_part_sequence_exception

    # 4. 所有分片已上传，开始合并
    logger.info(
        "all chunks uploaded, starting merge file_id=%s user=%s",
        file_record.file_id,
        current_user.id,
    )
    chunks = sorted(chunks, key=lambda c: c.part_number)

    # 5. 执行分片合并
    await merge_chunks_to_file(session, file_record, chunks, current_user)

    logger.info(
        "chunks merged successfully file_id=%s user=%s",
        file_record.file_id,
        current_user.id,
    )
    return True


async def merge_chunks_to_file(
    session: SessionDep,
    file_record: FileInDB,
    chunks: list[PartInDB],
    current_user: UserInDB,
) -> None:
    """
    执行分片合并，将所有分片写入最终文件
    
    边写入边计算哈希，写入完成后验证哈希是否匹配
    如果哈希不匹配，删除错误文件和所有相关记录
    
    事务管理：不 commit，由调用方（API 层）统一管理
    """
    final_path = Path(file_record.path)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    # 边写入边计算哈希
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
                delete_local_file(final_path)
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
        # 删除错误的合并文件、分片和文件记录
        await delete_file_and_parts(session, file_record.file_id, current_user)
        raise merged_file_hash_mismatch_exception

    # 合并成功，清理分片临时文件和记录
    await delete_parts_by_file_id(session, file_record.file_id)


async def check_duplicate_filename(
    session: SessionDep, user: UserInDB, filename: str
) -> bool:
    """
    检查用户是否已有同名文件
    
    事务管理：只读查询，无需 commit
    异常处理：不抛出异常，返回检查结果
        
    Returns:
        True 表示存在重复文件名，False 表示不重复
    """
    user_file_ids = set(user.file_ids or [])
    if not user_file_ids:
        return False

    user_files = (
        await session.exec(
            select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))  # pylint: disable=no-member
        )
    ).all()

    for user_file in user_files:
        if user_file.filename == filename:
            logger.debug(
                "duplicate filename found user=%s filename=%s",
                user.id,
                filename,
            )
            return True

    return False


async def check_duplicate_file_hash(
    session: SessionDep, user: UserInDB, hash_sha256: str
) -> bool:
    """
    检查用户是否已拥有该哈希的文件
    
    事务管理：只读查询，无需 commit
    异常处理：不抛出异常，返回检查结果
    
        
    Returns:
        True 表示用户已拥有该文件，False 表示未拥有
    """
    user_file_ids = set(user.file_ids or [])

    same_hash_records = (
        await session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256))
    ).all() or []

    for rec in same_hash_records:
        if (rec.file_id in user_file_ids) or (user.id in set(rec.owners or [])):
            logger.debug(
                "duplicate file hash found user=%s hash=%s",
                user.id,
                hash_sha256,
            )
            return True

    return False


async def try_reuse_file_by_hash(#todo 用户怎么知道自己有没有复用呢？
    session: SessionDep,
    user: UserInDB,
    hash_sha256: str,
) -> FileInDB | None:
    """
    尝试复用已存在的相同哈希文件
    
    注意：调用此函数前，应先调用 check_duplicate_file_hash 检查是否重复
    
    处理逻辑：
    1. 查询相同哈希的文件记录
    2. 跳过用户已拥有的文件（调用方应该先检查）
    3. 如果未拥有但文件存在，将用户添加到 owners 并复用
    4. 如果文件不存在，将用户添加到 owners 但返回记录（等待上传）
    
    事务管理：不 commit，由调用方统一管理
    异常处理：不抛出异常，返回结果
        
    Returns:
        如果找到可复用的文件记录，返回该记录；否则返回 None
    """
    user_file_ids = set(user.file_ids or [])

    same_hash_records = (
        await session.exec(select(FileInDB).where(FileInDB.hash_sha256 == hash_sha256))
    ).all() or []

    for rec in same_hash_records:
        # 跳过用户已拥有的文件
        if (rec.file_id in user_file_ids) or (user.id in set(rec.owners or [])):
            continue

        # 将当前用户添加到 owners
        current_owners = list(rec.owners or [])
        if user.id not in current_owners:
            current_owners.append(user.id)
            rec.owners = current_owners
            session.add(rec)
            await session.flush()

        # 如果文件已存在，可以直接复用
        if Path(rec.path).exists():
            # 建立用户与文件的关联
            link_file_to_user(session, user, rec.file_id, rec.size or 0)

            logger.info(
                "reused existing file user=%s file_id=%s hash=%s",
                user.id,
                rec.file_id,
                hash_sha256,
            )
            return rec

        # 文件不存在，返回记录但不建立关联（等待上传）
        logger.info(
            "found existing record but file missing user=%s file_id=%s hash=%s",
            user.id,
            rec.file_id,
            hash_sha256,
        )
        return rec

    # 没有找到相同哈希的文件
    return None


async def delete_parts_by_file_id(
    session: SessionDep,
    file_id: int,
) -> int:
    """
    根据 file_id 删除分片记录（物理文件 + 数据库记录）
    
    删除流程：
    1. 查询该文件的所有分片记录
    2. 删除每个分片的物理文件（调用 delete_local_file）
    3. 删除数据库中的分片记录
    
    事务管理：不 commit，由调用方统一管理
    异常处理：不捕获，让异常传播到调用方
    
        
    Returns:
        删除的分片数量
        
    Raises:
        OSError: 物理文件删除失败时
    """
    # 构建查询条件
    parts = await session.exec(
            select(PartInDB).where(
                PartInDB.file_id == file_id,
            )
        )

    parts = parts.all()
    
    if not parts:
        logger.debug(
            "no parts found for file_id=%s owner_id=%s",
            file_id,
        )
        return 0
    
    deleted_count = 0
    
    # 先删除物理文件，再删除数据库记录
    for part in parts:
        # 删除分片物理文件
        delete_local_file(part.temp_path)
        logger.debug(
            "deleted part file part_id=%s temp_path=%s",
            part.part_id,
            part.temp_path
        )
        
        # 删除数据库记录
        await session.delete(part)
        deleted_count += 1
    
    logger.info(
        "deleted %s parts for file_id=%s",
        deleted_count,
        file_id,
    )
    
    return deleted_count


async def delete_file_and_parts(
    session: SessionDep,
    file_id: int,
    current_user: UserInDB,
) -> None:
    """
    根据 file_id 删除文件记录及其所有分片（物理文件 + 数据库记录）

    删除流程：
    1. 查询文件记录
    2. 查询所有分片记录，删除分片物理文件（数据库记录会在删除文件时自动级联删除）
    3. 删除文件的物理文件
    4. 从所有关联用户的 file_ids 中移除该文件ID
    5. 更新所有关联用户的统计信息（file_count、total_file_size）
    6. 删除文件数据库记录（数据库会自动级联删除分片记录）

    事务管理：不 commit，由调用方统一管理
    异常处理：不捕获，让异常传播到调用方

    Raises:
        ValueError: 文件不存在
        PermissionError: 用户无权限删除该文件
        OSError: 物理文件删除失败时
    """
    # 1. 查询文件记录
    file_record = await get_file_record(file_id, session)
    if not file_record:
        logger.error("file not found file_id=%s", file_id)
        raise ValueError(f"File not found: file_id={file_id}")

    # 2. 查询所有分片记录，删除分片物理文件（数据库记录会在删除文件时自动级联删除）
    parts = await session.exec(
        select(PartInDB).where(PartInDB.file_id == file_id)
    )
    parts = parts.all()

    deleted_parts_count = 0
    for part in parts:
        delete_local_file(part.temp_path)
        deleted_parts_count += 1
        logger.debug(
            "deleted part file part_id=%s temp_path=%s",
            part.part_id,
            part.temp_path
        )

    # 3. 删除文件的物理文件
    delete_local_file(file_record.path)
    logger.debug(
        "deleted file physical file path=%s file_id=%s",
        file_record.path,
        file_id
    )

    # 5. 删除文件数据库记录（数据库会自动级联删除分片记录）
    await session.delete(file_record)

    logger.info(
        "deleted file and all parts file_id=%s user=%s",
        file_id,
        current_user.id
    )


async def delete_experiment_files(
    session: SessionDep,
    file_id: int,
) -> None:
    """
    删除与文件关联的实验文件（imzML、ibd 等后续还可以扩展

    处理逻辑：
    1. 查询与 source_file_id 关联的实验记录
    2. 删除实验相关的物理文件（imzml_path、ibd_path）
    3. 删除实验目录（如果为空）
    4. 删除实验数据库记录

    事务管理：不 commit，由调用方统一管理
    异常处理：不捕获，让异常传播到调用方

    Raises:
        OSError: 物理文件删除失败时
    """

    # 查询 imzML 实验记录
    experiment = (
        await session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_id
            )
        )
    ).first()

    if not experiment:
        logger.debug("no experiment found for file_id=%s", file_id)
        return

    # 删除 imzML 文件
    if experiment.imzml_path:
        await asyncio.to_thread(
            lambda: delete_local_file(experiment.imzml_path)
        )
        logger.info("deleted imzml file path=%s", experiment.imzml_path)

    # 删除 ibd 文件
    if experiment.ibd_path:
        await asyncio.to_thread(
            lambda: delete_local_file(experiment.ibd_path)
        )
        logger.info("deleted ibd file path=%s", experiment.ibd_path)

    # 删除实验目录（如果为空）
    if experiment.imzml_path:
        experiment_dir = Path(experiment.imzml_path).parent

        def _rmdir_if_empty(d: Path) -> None:
            if d.exists() and not any(d.iterdir()):
                d.rmdir()

        await asyncio.to_thread(_rmdir_if_empty, experiment_dir)
        logger.debug("cleaned up experiment directory path=%s", experiment_dir)

    # 删除实验数据库记录
    await session.delete(experiment)
    logger.info(
        "deleted experiment record exp_id=%s file_id=%s", experiment.id, file_id
    )
