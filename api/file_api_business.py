#########################
# File API Business - 业务逻辑层
# 职责：业务逻辑处理，数据库操作，但不直接暴露API
# 包含：用户文件关联管理、文件记录查询、权限校验、清理逻辑、分片合并
#########################
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING
from datetime import timedelta

from sqlalchemy.orm import selectinload
from sqlmodel import select, exists, and_, not_, func
from sqlmodel.ext.asyncio.session import AsyncSession

from api.file_api_helper import (
    delete_local_file,
    check_user_quota,
    validate_total_parts,
    compute_upload_file_metadata,
    validate_file_metadata,
    validate_upload_params,
    build_zip_file,
    merge_files_with_hash,
    delete_files_async,
)
from constants.file_constants import (
    MAX_FILES_PER_USER,
    PREFLIGHT_TTL_MINUTES,
    ALLOWED_STORAGE_TYPES,
    DOWNLOAD_TEMP_ROOT,
)
from exceptions import (
    chunk_file_missing_exception,
    duplicate_file_exception,
    invalid_user_for_file_list_sync_exception,
    merged_file_hash_mismatch_exception,
    user_file_limit_exception,
    file_content_missing_exception,
    invalid_request_parameters_exception,
)
from logger import get_logger
from models.file_models import FileInDB, PartInDB, now_local_timezone, FileCreate
from models.user_models import UserInDB
from models.link_models import UserFileLink
from models.experiment_models import ExperimentImzMLInDB, ExperimentStatus

if TYPE_CHECKING:
    from fastapi import UploadFile

logger = get_logger("main.file_api_business")


async def get_experiment_by_file_id(
    session: AsyncSession,
    file_id: int
) -> ExperimentImzMLInDB | None:
    """
    根据文件ID查询实验记录

    """
    return (
        await session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_id
            )
        )
    ).first()


async def validate_chunk_metadata(
    session: AsyncSession,
    file_id: int,
    part_number: int,
    owner_id: int,
) -> bool:
    """
    检查分片是否已重复上传

    Returns:
        True 表示分片已存在，False 表示不存在
    """
    existing_chunk = (
        await session.exec(
            select(PartInDB).where(
                PartInDB.file_id == file_id,
                PartInDB.part_number == part_number,
                PartInDB.owner_id == owner_id,
            )
        )
    ).first()
    return existing_chunk is not None


def validate_upload_request(
    storage_type: str,
    filename: str,
    total_parts: int | None = None
) -> bool:
    """
    统一验证上传请求参数

    Args:
        storage_type: 存储类型
        filename: 文件名validate_upload_params
        total_parts: 分片总数（可选，分片上传时需要）

    Returns:
        True 表示验证通过，False 表示验证失败
    """
    # 1. 存储类型验证
    if storage_type not in ALLOWED_STORAGE_TYPES:
        return False

    # 2. 文件名和类型验证
    if not validate_file_metadata(filename):
        return False

    # 3. 分片数量验证（如果需要）
    if total_parts is not None and not validate_total_parts(total_parts):
        return False

    return True


async def prepare_download_file(
    session: AsyncSession,
    file_record: FileInDB,
) -> Path:
    """
    准备下载文件，处理实验文件的ZIP打包

    Returns:
        文件路径

    Raises:
        file_content_missing_exception: 文件内容缺失
    """
    path = Path(file_record.path) if file_record.path else None

    # 文件存在，直接返回
    if path and path.exists():
        return path

    # 处理实验文件（imzML）
    if file_record.experiment_type == "imzML":
        assert file_record.file_id is not None
        experiment = await get_experiment_by_file_id(session, file_record.file_id)

        if not experiment or experiment.status != ExperimentStatus.COMPLETED:
            logger.warning(
                "experiment not found or not completed file_id=%s", file_record.file_id
            )
            raise file_content_missing_exception

        imzml_path = Path(experiment.imzml_path) if experiment.imzml_path else None
        ibd_path = Path(experiment.ibd_path) if experiment.ibd_path else None

        if (not imzml_path or not ibd_path or not imzml_path.exists() or not ibd_path.exists()):
            logger.warning("experiment files missing file_id=%s", file_record.file_id)
            raise file_content_missing_exception

        # 创建 ZIP 包
        zip_filename = file_record.filename
        DOWNLOAD_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        zip_path = DOWNLOAD_TEMP_ROOT / zip_filename

        # 准备文件列表：(文件路径, ZIP中的文件名)
        files_to_zip = [
            (imzml_path, imzml_path.name),
            (ibd_path, ibd_path.name),
        ]

        await build_zip_file(files_to_zip, zip_path)

        # 更新文件记录
        file_record.path = str(zip_path)
        file_record.size = zip_path.stat().st_size

        logger.info(
            "created download zip file_id=%s path=%s size=%s",
            file_record.file_id,
            zip_path,
            file_record.size,
        )
        return zip_path

    raise file_content_missing_exception


async def update_download_stats(
    session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
) -> None:
    """
    更新下载统计

    """
    file_record.download_count += 1
    current_user.download_used = (current_user.download_used or 0) + 1
    session.add(current_user)
    session.add(file_record)


async def unlink_user_from_file(
    session: AsyncSession,
    user: UserInDB,
    file_record: FileInDB,
) -> bool:
    """
    解除用户与文件的关联

    Returns:
        True 表示文件还有其他用户持有（只解除关联）
        False 表示文件没有其他用户持有（需要完全删除）
    """
    # 1. 更新当前用户的文件关联和统计信息
    assert file_record.file_id is not None
    await remove_file_link(session, user, file_record)
    session.add(file_record)

    # 2. 更新统计信息
    user.file_count = max(0, (user.file_count or 0) - 1)
    user.total_file_size = max(
        0, (user.total_file_size or 0) - file_record.size
    )
    session.add(user)

    # 3. 刷新事务
    await session.flush()

    # 4. 返回是否还有其他用户持有该文件
    owner_counts = (await session.exec(
        select(func.count()).where(UserFileLink.file_id == file_record.file_id) # pylint: disable=not-callable
    )).one()

    return bool(owner_counts)


async def assert_file_owner(session: AsyncSession, file_record: FileInDB, current_user: UserInDB) -> bool:
    """
    校验用户是否有权限操作该文件

    权限判断逻辑：
    1. 管理员拥有所有文件的权限
    2. 普通用户需要满足：
       - UserFileLink 含有该用户与该文件的链接

    Args:
        file_record: 文件记录对象（FileInDB）
        current_user: 当前用户对象（UserInDB）

    Returns:
        True 表示有权限，False 表示无权限
    """
    # 管理员拥有所有权限
    if current_user.identity == "admin":
        return True

    # 校验该文件属于该用户
    is_owner = (await session.exec(
        select(exists().where(
            and_(UserFileLink.user_id == current_user.id,
                 UserFileLink.file_id == file_record.file_id)))
    )).one()

    if is_owner:
        return True

    logger.warning(
        "permission denied user=%s file_id=%s", current_user.id, file_record.file_id
    )
    return False


async def validate_simple_upload(
    session: AsyncSession,
    current_user: UserInDB,
    file: "UploadFile",
    storage_type: str,
    pre_record: FileInDB,
) -> tuple[str, int]:
    """
    验证简单上传的所有条件

    Raises:
        HTTPException: 验证失败时抛出异常
    """
    try:
        # 1. 基础参数验证
        if not validate_upload_params(storage_type,file.filename or "",pre_record.storage_type,pre_record.filename):
            logger.warning(
                "validation failed user=%s filename=%s storage=%s",
                current_user.id,
                file.filename if file.filename else "None",
                storage_type,
            )
            raise invalid_request_parameters_exception

        # 2. 重复文件名检查
        assert file.filename is not None
        if await check_duplicate_filename(session, current_user, file.filename):
            raise duplicate_file_exception

        # 3. 验证文件元数据（哈希和大小）
        hash_sha256, size = await compute_upload_file_metadata(file)
        if size != pre_record.size or hash_sha256 != (pre_record.hash_sha256 or ""):
            logger.error(
                "metadata mismatch user=%s expected_hash=%s actual_hash=%s expected_size=%s actual_size=%s",
                current_user.id,
                pre_record.hash_sha256,
                hash_sha256,
                pre_record.size,
                size,
            )
            raise invalid_request_parameters_exception

        # 4. 用户配额检查
        if not check_user_quota(current_user, size):
            raise user_file_limit_exception

        return hash_sha256, size

    except Exception:
        # 统一的清理逻辑
        await safe_cleanup_preflight_record(session, current_user, pre_record)
        await session.commit()
        await session.refresh(current_user)
        raise



async def append_file_link(session: AsyncSession, user: UserInDB, file: FileInDB) -> UserFileLink | None:
    """
    将文件添加到用户的文件列表中
    上传成功后调用，建立用户与文件的关联关系，
    并返回新的linker(如果已存在关联关系，则返回None)。

    Returns:
        UserFileLink | None

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    # 文件已在列表中，无需重复添加
    is_alread_exist = (await session.exec(
                        select(exists().where(and_(UserFileLink.user_id == user.id)))
                    )).one()
    if is_alread_exist:
        return None

    if user.file_count >= MAX_FILES_PER_USER:
        raise user_file_limit_exception

    # 新建 UserFileLink 链接
    new_linker = UserFileLink(user_id=user.id, file_id=file.file_id)
    return new_linker


async def link_file_to_user(
    session: AsyncSession,
    user: UserInDB,
    file: FileInDB,
    file_size: int,
) -> None:
    """
    将文件关联到用户，并更新用户的文件统计信息

    完整的文件关联流程：
    1. 添加文件和用户的 UserFileLinker 关联
    2. 增加用户的文件计数
    3. 增加用户的总存储空间
    4. 将用户对象添加到会话（准备提交）

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    # 1. 将文件添加到用户的文件列表
    new_linker = await append_file_link(session, user, file)

    # 2. 更新用户的文件统计信息
    if new_linker:
        user.file_count = (user.file_count or 0) + 1
        user.total_file_size = (user.total_file_size or 0) + file_size

    # 3. 将用户对象和链接器对象添加到会话
    session.add(new_linker)
    session.add(user)


async def validate_and_try_reuse_file(
    session: AsyncSession,
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
    assert file_data.hash_sha256 is not None
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


async def remove_file_link(session: AsyncSession, user: UserInDB, file: FileInDB) -> None:
    """
    删除文件时调用，解除用户与文件的关联关系

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    user_file_linkers = (await session.exec(
        select(UserFileLink).where(
            and_(UserFileLink.user_id==user.id,
                 UserFileLink.file_id==file.file_id)
        )
    )).all()

    for linker in user_file_linkers:
        await session.delete(linker)
    session.expire(user, ["files"])#todo
    session.expire(file, ["owners"])


async def get_file_record(
    file_id: int,
    session: AsyncSession,
    load_parts: bool = False
) -> FileInDB | None:
    """
    根据文件ID查询文件记录

    可选参数：
    load_parts: bool 是否同时加载分片信息

    Returns:
        文件记录对象，不存在则返回 None
    """
    statement = select(FileInDB).where(FileInDB.file_id == file_id)
    if load_parts:
        statement = statement.options(selectinload(FileInDB.parts)) # type: ignore
    file_record = (await session.exec(statement)).first()
    return file_record

async def get_user_file_ids(session: AsyncSession, user_id: int) -> list[int|None]:
    """获取用户所有文件 ID，不加载文件对象"""
    stmt = select(UserFileLink.file_id).where(UserFileLink.user_id == user_id)
    res = await session.exec(stmt)
    return list(res.all())


async def safe_cleanup_preflight_record(#TODO 后续可以用定时任务来清理
    session: AsyncSession, current_user: UserInDB, pre_record: FileInDB
) -> None:
    """
    安全清理预检记录

    处理逻辑：
    - 如果文件属于多个用户：只移除当前用户的关联，保留记录
    - 如果文件只属于当前用户：删除整条记录
    """
    # 先删当前用户，再过滤数据库是否还有其他的linker(代表其他用户也拥有该文件)
    pre_record_linker = (await session.exec(
        select(UserFileLink).where(and_(UserFileLink.file_id == pre_record.file_id, UserFileLink.user_id == current_user.id))
    )).first()

    # 从 owners 列表移除当前用户
    if pre_record_linker:#todo 要增加为空判断，第一次用户上传文件参数不正确，会删除空
        await session.delete(pre_record_linker)
        await session.flush([pre_record_linker])
        session.expire(current_user, ["files"])
        session.expire(pre_record, ["owners"])

    # 检测是否还有其他用户持有该文件，如有则不删除文件
    owner_count = (await session.exec( #todotodo
        select(func.count("*")).where(UserFileLink.file_id == pre_record.file_id) # pylint: disable=not-callable
    )).one()

    if owner_count > 0:
        # 还有其他用户持有，只更新 owners
        logger.debug(
            "preflight cleanup: removed user=%s from file_id=%s owners",
            current_user.id,
            pre_record.file_id,
        )
    else:
        # 没有其他用户持有，删除记录（包括物理文件和分片）
        assert pre_record.file_id is not None
        await delete_file_and_parts(session, pre_record.file_id, current_user)
        logger.debug(
            "preflight cleanup: deleted file_id=%s completely", pre_record.file_id
        )


async def cleanup_stale_preflights(
    session: AsyncSession,
    current_user: UserInDB,
    ttl_minutes: int = PREFLIGHT_TTL_MINUTES,
) -> int:
    """
    清理当前用户过期且未完成的预检记录

    清理条件（需同时满足）：
    1. 记录 owners 中包含当前用户
    2. 文件物理路径不存在（说明为预检记录，未完成上传）
    3. file_id 不在用户 file_ids 中
    4. uploaded_at 超过 TTL

    更正清理条件：
    在多对多设计的情况下，简化为满足条件2和4即可清理

    Returns:
        清理的记录数量
    """
    # 代表 uploaded_at 超过 TTL 的时间点
    now = now_local_timezone()
    cutoff_time = now - timedelta(minutes=ttl_minutes)
    cleaned = 0

    # 数据库层面先过滤出owners包含当前用户的所有文件记录
    records = (await session.exec(
        select(FileInDB)
        .join(UserFileLink, and_(FileInDB.file_id == UserFileLink.file_id))
        .where(UserFileLink.user_id == current_user.id)
    )).all()

    for rec in records:

        # 检查文件是否存在，存在，不能删
        file_existing = Path(rec.path).exists()
        if file_existing:
            continue

        # TTL 判断 - 所有时间都是UTC时间，直接比较
        if not rec.uploaded_at < cutoff_time:
            continue

        # 满足所有条件，清理预检记录
        await safe_cleanup_preflight_record(session, current_user, rec)
        cleaned += 1

    if cleaned:
        await session.commit()

    return cleaned


async def validate_and_merge_chunks(
    session: AsyncSession, file_record: FileInDB, current_user: UserInDB
) -> bool:
    """
    验证分片完整性并尝试合并

    每次上传分片后调用，检查是否所有分片都已上传完成
    如果完成则执行合并操作

    合并流程：
    1. 查询该文件的所有分片记录
    2. 检查分片完整性（数量和序列）
    3. 如果分片不完整，返回 False 等待更多分片
    4. 如果所有分片已上传，执行分片合并
    5. 合并成功后清理分片临时文件

    Raises:
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
            .order_by(PartInDB.part_number) # type: ignore
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

    # 2. 检查分片完整性（数量和序列）
    uploaded_parts = sorted([chunk.part_number for chunk in chunks])
    expected_parts = list(range(1, expected_total_parts + 1))

    logger.info(
        "chunk status file_id=%s uploaded=%s expected=%s",
        file_record.file_id,
        uploaded_parts,
        expected_parts,
    )

    # 3. 分片不完整，等待更多分片
    if len(uploaded_parts) < expected_total_parts:
        return False

    # 分片序列不连续（这是正常的中间状态，等待缺失的分片）
    if uploaded_parts != expected_parts:
        logger.debug(
            "chunk sequence incomplete file_id=%s uploaded=%s expected=%s",
            file_record.file_id,
            uploaded_parts,
            expected_parts,
        )
        return False

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
    session: AsyncSession,
    file_record: FileInDB,
    chunks: list[PartInDB],
    current_user: UserInDB,
) -> None:
    """
    执行分片合并，将所有分片写入最终文件

    使用 helper 层的 merge_files_with_hash 函数进行合并和哈希验证
    如果哈希不匹配，删除错误文件和所有相关记录

    事务管理：不 commit，由调用方（API 层）统一管理
    """
    final_path = Path(file_record.path)
    chunk_paths = [Path(chunk.temp_path) for chunk in chunks]

    assert file_record.file_id is not None

    try:
        # 使用 helper 层函数合并文件并验证哈希
        await merge_files_with_hash(
            chunk_paths,
            final_path,
            file_record.hash_sha256
        )
    except FileNotFoundError as exc:
        # 分片文件丢失
        logger.error(
            "chunk file missing file_id=%s error=%s",
            file_record.file_id,
            exc,
        )
        delete_local_file(final_path)
        await delete_file_and_parts(session, file_record.file_id, current_user)
        raise chunk_file_missing_exception from exc
    except ValueError as exc:
        # 哈希不匹配
        logger.error(
            "merged file hash mismatch file_id=%s error=%s",
            file_record.file_id,
            exc,
        )
        await delete_file_and_parts(session, file_record.file_id, current_user)
        raise merged_file_hash_mismatch_exception from exc

    # 合并成功，清理分片临时文件和记录
    await delete_parts_by_file_id(session, file_record.file_id)


async def check_duplicate_filename(
    session: AsyncSession, user: UserInDB, filename: str
) -> bool:
    """
    检查用户是否已有同名文件

    事务管理：只读查询，无需 commit
    异常处理：不抛出异常，返回检查结果

    Returns:
        True 表示存在重复文件名，False 表示不重复
    """
    # TODO: 整个用户文件共享架构可能都需要修改，
    # 存在问题：在当前多对多的架构下，A用户上传某一文件，该文件关联至用户A，
    # 此时另一B用户上传同名不同哈希文件，该文件由于不和B存在联系，故能够通
    # 过重名检查，但实际上存储路径已有A用户上传的同名文件，会导致文件系统
    # 文件名重复错误。

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

    has_duplicate_file = (await session.exec(stmt)).one()

    if has_duplicate_file:
        logger.debug(
            "duplicate filename found user=%s filename=%s",
            user.id,
            filename,
        )
        return True

    return False


async def check_duplicate_file_hash(
    session: AsyncSession, user: UserInDB, hash_sha256: str
) -> bool:
    """
    检查用户是否已拥有该哈希的文件（只针对本用户）

    Returns:
        True 表示用户已拥有该文件，False 表示未拥有
    """

    # 通过预加载获取当前用户文件列表中和传入哈希相同的文件列表
    # 直接 exist 子句高效获取布尔值
    has_same_hash_file = (await session.exec(
        select(
            exists().where(
                and_(
                    (UserFileLink.user_id == user.id),
                    (UserFileLink.file_id == FileInDB.file_id),
                    (FileInDB.hash_sha256 == hash_sha256)
                )
            )
        )
    )).one()

    if has_same_hash_file:
        logger.debug(
            "duplicate file hash found user=%s hash=%s",
            user.id,
            hash_sha256,
        )
        return True

    return False


async def try_reuse_file_by_hash(
    session: AsyncSession,
    user: UserInDB,
    hash_sha256: str,
) -> FileInDB | None:
    """
    尝试复用已存在的相同哈希文件

    注意：调用此函数前，应先调用 check_duplicate_file_hash 检查是否重复

    处理逻辑：
    1. 查询相同哈希的文件记录
    2. 跳过用户已拥有的文件（调用方应该先检查）
    3. 如果文件不存在，将用户添加到 owners 但返回记录（等待上传）

    Returns:
        如果找到可复用的文件记录，返回该记录；否则返回 None
    """

    # 直接通过 SQL 子句找到 hash 相同但不属于该用户的文件
    same_hash_file = (
        await session.exec(
            select(FileInDB)
            .where(FileInDB.hash_sha256 == hash_sha256)
            .where(
                not_(exists()
                     .where(and_(
                            (UserFileLink.user_id == user.id),
                            (UserFileLink.file_id == FileInDB.file_id)
                        )
                    )
                )
            )
        )
    ).first()

    # 没有找到相同哈希的文件
    if not same_hash_file:
        return None

    # 如果物理文件已存在，可以直接复用
    if Path(same_hash_file.path).exists():
        # 建立用户与文件的关联
        assert same_hash_file.file_id is not None
        await link_file_to_user(session, user, same_hash_file, same_hash_file.size or 0)

        logger.info(
            "reused existing file user=%s file_id=%s hash=%s",
            user.id,
            same_hash_file.file_id,
            hash_sha256,
        )
        # 返回文件记录，由调用方提交事务后抛出异常
        return same_hash_file

    # 物理文件不存在，建立关联，返回记录（等待上传）
    assert same_hash_file.file_id is not None
    await link_file_to_user(session, user, same_hash_file, same_hash_file.size or 0)
    logger.info(
        "found existing record but file missing user=%s file_id=%s hash=%s",
        user.id,
        same_hash_file.file_id,
        hash_sha256,
    )
    return same_hash_file


async def delete_parts_by_file_id(
    session: AsyncSession,
    file_id: int,
) -> int:
    """
    根据 file_id 删除分片记录（物理文件 + 数据库记录）

    删除流程：
    1. 查询该文件的所有分片记录
    2. 删除每个分片的物理文件（调用 delete_local_file）
    3. 删除数据库中的分片记录

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
    session: AsyncSession,
    file_id: int,
    current_user: UserInDB,
) -> None:
    """
    根据 file_id 删除文件记录及其所有分片（物理文件 + 数据库记录）

    删除流程：
    1. 查询文件记录，同时预加载对应的分片记录
    2. 删除分片物理文件（数据库记录会在删除文件时自动级联删除）
    3. 删除文件的物理文件
    4. 从所有关联用户的 file_ids 中移除该文件ID
    5. 更新所有关联用户的统计信息（file_count、total_file_size）
    6. 删除文件数据库记录（数据库会自动级联删除分片记录）
    Raises:
        ValueError: 文件不存在
        PermissionError: 用户无权限删除该文件
        OSError: 物理文件删除失败时
    """
    # 1. 查询文件记录，同时预加载对应的分片记录
    file_record = await get_file_record(file_id, session, load_parts=True)
    if not file_record:
        logger.error("file not found file_id=%s", file_id)
        raise ValueError(f"File not found: file_id={file_id}")

    # 2. 删除分片物理文件（数据库记录会在删除文件时自动级联删除）
    deleted_parts_count = 0
    for part in file_record.parts:
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
    session: AsyncSession,
    file_id: int,
) -> None:
    """
    删除与文件关联的实验文件（imzML、ibd 等）

    处理逻辑：
    1. 查询与 source_file_id 关联的实验记录
    2. 删除实验相关的物理文件（imzml_path、ibd_path）
    3. 删除实验目录（如果为空）
    4. 删除实验数据库记录

    Raises:
        OSError: 物理文件删除失败时
    """
    # 查询 imzML 实验记录
    experiment = await get_experiment_by_file_id(session, file_id)

    if not experiment:
        logger.debug("no experiment found for file_id=%s", file_id)
        return

    # 收集需要删除的文件
    files_to_delete = []
    if experiment.imzml_path:
        files_to_delete.append(experiment.imzml_path)
    if experiment.ibd_path:
        files_to_delete.append(experiment.ibd_path)

    # 批量删除实验文件
    if files_to_delete:
        deleted_count = await delete_files_async(files_to_delete)
        logger.info("deleted %s experiment files for file_id=%s", deleted_count, file_id)

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
