#########################
# File Service - 业务逻辑层
# 职责:业务逻辑处理,数据库操作,但不直接暴露API
# 包含:用户文件关联管理、文件记录查询、权限校验、OSS 清理逻辑
#########################
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from sqlmodel import select, exists, and_, not_, func, col, delete
from sqlmodel.ext.asyncio.session import AsyncSession

from database.db import get_async_engine
from database.redis_client import redis_client
from database.atomic_ops import atomic_increment, atomic_decrement, atomic_set
from api_services.user_service import get_user_by_id
from api_services.experiment_service import get_origin_imzml_by_file_id
from api_helpers.experiment_helper import validate_experiment_type, packup_oss_file
from api_helpers.file_helper import (
    check_user_quota,
    validate_file_metadata,
    validate_total_parts,
)
from storage.oss.oss_storage import OssStorage

from constants.file_constants import (
    ALLOWED_STORAGE_TYPES,
    MAX_SINGLE_FILE_SIZE_BYTES,
    OSS_PACKUP_ZIP_STORAGE_ROOT,
    SIGNED_URL_EXPIRE_SECONDS,
    PACKUP_CACHE_TTL_SECONDS,
)
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from exceptions import (
    duplicate_file_exception,
    file_not_found_exception,
    file_content_missing_exception,
    invalid_request_parameters_exception,
    invalid_user_for_file_list_sync_exception,
    user_file_limit_exception,
    origin_type_not_supported_exception,
)
from exceptions.common_exceptions import internal_server_error_exception
from tools import get_logger,utc_time
from models.file_models import FileCreate, FileInDB, FilePatch, FileStatus, PackupStatus, OssDownloadFile
from models.user_models import UserInDB
from models.link_models import UserFileLink
from models.experiment_models import ExperimentStatus, OriginImzMLInDB
logger = get_logger("api_services.file_service")


#########################
# File 查 - 基础记录查询（DB）
#########################
async def valid_duplicate_filename(
    async_session: AsyncSession, user: UserInDB, filename: str
) -> bool:
    """
    检查用户是否已有同名文件

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
    *,
    include_deleting: bool = False,
) -> FileInDB:
    """
    根据文件ID查询文件记录

    默认拒绝删除流程中的文件：DELETING / DELETE_FAILED 一律按不存在处理
    （404 不泄露「删除中」状态），删除登记等管理路径用 include_deleting=True 显式放行。

    Returns:
        文件记录对象
    Raises:
        file_not_found_exception: 文件记录不存在（或处于删除流程且未放行）
    """
    statement = select(FileInDB).where(FileInDB.file_id == file_id)

    file_record = (await async_session.exec(statement)).first()

    if not file_record:
        logger.warning("file record not found file_id=%s", file_id)
        raise file_not_found_exception

    if not include_deleting and file_record.status in (FileStatus.DELETING, FileStatus.DELETE_FAILED):
        logger.warning(
            "file is being deleted, treat as not found file_id=%s status=%s",
            file_id, file_record.status,
        )
        raise file_not_found_exception

    return file_record


#########################
# File  - 增
#########################
async def append_file_link(async_session: AsyncSession, user: UserInDB, file: FileInDB) -> UserFileLink | None:
    """
    将文件添加到用户的文件列表中
    上传成功后调用,建立用户与文件的关联关系,
    并返回新的linker(如果已存在关联关系,则返回None)。

    Returns:
        UserFileLink | None

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    # 文件已在列表中,无需重复添加
    is_alread_exist = (await async_session.exec(
                        select(exists()
                               .where(and_(UserFileLink.user_id == user.id,
                                           UserFileLink.file_id == file.file_id))
                               )
                    )).one()
    if is_alread_exist:
        return None

    # 新建 UserFileLink 链接
    new_linker = UserFileLink(user_id=user.id, file_id=file.file_id)
    return new_linker


async def link_file_to_user(
    async_session: AsyncSession,
    user: UserInDB,
    file: FileInDB,
    file_size: int,
) -> None:
    """
    将文件关联到用户,并更新用户的文件统计信息

    并发安全：使用原子 SQL UPDATE 更新统计字段，避免 read-modify-write
    在并发场景（同一用户并行上传/秒传）下的丢失更新问题。

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    # 1. 将文件添加到用户的文件列表
    new_linker = await append_file_link(async_session, user, file)

    # 2. 更新用户的文件统计信息（仅在新建关联时才更新；原子操作防止并发丢失更新）
    if new_linker:
        assert user.id is not None
        await atomic_increment(async_session, UserInDB, user.id, "file_count")
        await atomic_increment(async_session, UserInDB, user.id, "total_file_size", file_size)

    # 3. 将链接器对象添加到会话
    #    无需再 async_session.add(user)：统计信息已通过原子 SQL 直接写入数据库，
    #    调用方上层会执行 session.refresh(current_user) 拉取最新值。
    if new_linker:
        async_session.add(new_linker)

#########################
# File  - 查
#########################
def build_file_order(sort_by: str, order: str) -> tuple[Any, Any]:
    """文件列表排序子句：sort_by/order 映射为排序列 + file_id DESC 兜底

    供个人文件列表与公共文件列表共用。取值白名单由接口层 Query pattern 约束
    （sort_by: uploaded_at|size，order: asc|desc），未命中时回落默认排序
    （uploaded_at 倒序，与接口默认值一致）。

    Returns:
        (主排序子句, 兜底排序子句)——同值行之间按 file_id 倒序，保证翻页不重不漏
    """
    column = FileInDB.size if sort_by == "size" else FileInDB.uploaded_at
    primary = col(column).asc() if order == "asc" else col(column).desc()
    return (primary, col(FileInDB.file_id).desc()) # pylint: disable=no-member


async def get_user_file_ids(session: AsyncSession, user_id: int) -> list[int | None]:
    """获取用户所有文件 ID,不加载文件对象"""
    stmt = select(UserFileLink.file_id).where(UserFileLink.user_id == user_id)
    res = await session.exec(stmt)
    return list(res.all())


async def get_reuse_file_by_hash(
    async_session: AsyncSession,
    user: UserInDB,
    hash_md5: str,
) -> FileInDB | None:
    """
    查询是否存在可复用的相同哈希文件(不建立关联)

    Returns:
        如果找到可复用的文件记录,返回该记录；否则返回 None
    """
    # 直接通过 SQL 子句找到 hash 相同但不属于该用户的文件。
    # 只允许复用 COMPLETED 记录：COMPLETED 由回调在哈希校验、大小核对、
    # 路径落库全部通过后才设置，蕴含"origin 完整、哈希有效"；
    # UPLOADING / FAILED / 预检初始态（""）一概禁止复用。
    # path != "" 与 size > 0 进一步排除路径未确认或大小异常的记录。
    same_hash_file = (
        await async_session.exec(
            select(FileInDB)
            .where(FileInDB.file_verify_code == hash_md5)
            .where(FileInDB.storage_type == "oss")
            .where(FileInDB.status == FileStatus.COMPLETED)
            .where(FileInDB.path != "")
            .where(FileInDB.size > 0)
            .where(not_(exists()
                        .where(
                            and_((UserFileLink.user_id == user.id),
                                 (UserFileLink.file_id == FileInDB.file_id))
                              )
                        )
                )
            )
    ).first()

    if same_hash_file:
        logger.info(f"found reusable file record user={user.id} file_id={same_hash_file.file_id} hash={hash_md5}")

    return same_hash_file


async def assert_file_owner(async_session: AsyncSession, file_record: FileInDB, current_user: UserInDB) -> bool:
    """
    校验用户是否有权限操作该文件

    Returns:
        True 表示有权限,False 表示无权限
    """
    # 管理员拥有所有权限
    if current_user.identity == "admin":
        return True

    # 校验该文件属于该用户
    is_owner = (await async_session.exec(
        select(exists().where(
            and_(UserFileLink.user_id == current_user.id,
                 UserFileLink.file_id == file_record.file_id)))
    )).one()

    if is_owner:
        return True

    logger.warning("permission denied user=%s file_id=%s", current_user.id, file_record.file_id)
    return False


def packup_cache_key(file_id: int) -> str:
    """打包状态缓存键（Redis Hash）。按 file_id 隔离：文件名只在同一用户内唯一，以文件名为键会让同名文件跨用户串号"""
    return f"oss_packup-{file_id}"


def packup_zip_path(file_id: int) -> str:
    """打包 zip 的 OSS 对象键：{OSS_PACKUP_ZIP_STORAGE_ROOT}/{file_id}.zip"""
    return f"{OSS_PACKUP_ZIP_STORAGE_ROOT.strip('/')}/{file_id}.zip"  # type: ignore


def parse_packup_zip_file_id(zip_path: str | None) -> int | None:
    """packup_zip_path 的逆操作：从打包回调带回的 zip 对象键解析 file_id，格式不符返回 None"""
    if not zip_path:
        return None
    stem = Path(zip_path).stem
    if not (stem.isascii() and stem.isdigit()):
        return None
    file_id = int(stem)
    # 必须与发起打包时下发给 FC 的对象键完全一致，其他目录下的同名 zip 不认
    return file_id if zip_path.strip("/") == packup_zip_path(file_id) else None


async def prepare_download_file_oss(
    session: AsyncSession,
    file_record: FileInDB,
) -> str:
    """
    准备下载文件,处理实验文件的ZIP打包

    Returns:
        文件下载URL，如果正在打包则返回 "<PACKING>"

    Raises:
        file_content_missing_exception: 文件内容缺失
    """
    assert file_record.file_id is not None
    # 打包状态与触发锁均按 file_id 隔离（见 packup_cache_key）
    oss_packup_key = packup_cache_key(file_record.file_id)
    oss_packup_lock_key = f"oss_packup_lock-{file_record.file_id}"

    # path 为空说明上传从未完成（预检记录或上传中断），既无源 zip 可签、
    # origin 也不存在（无法打包），直接判定内容缺失，避免对空路径走 OSS 签名。
    if not file_record.path:
        logger.warning("download requested but file path is empty, filename=%s", file_record.filename)
        raise file_content_missing_exception

    try:
        packup = await redis_client.hgetall(oss_packup_key)  # type: ignore
        # 返回值已无 fc_request_id：FC 日志回捞链路已删除
        should_pack, zip_download_url, zip_path = await _handle_packup_status_check(
            file_record, packup, oss_packup_key, oss_packup_lock_key
        )

        if packup.get("status") == PackupStatus.COMPLETED and zip_download_url is None and zip_path:
            zip_download_url = await _prepare_download_url_for_completed_packup(
                file_record, zip_path, oss_packup_key
            )

        if should_pack:
            zip_download_url = await _execute_packup_flow(session, file_record, oss_packup_key)

        if not zip_download_url:
            logger.error("zip_download_url is None, filename=%s", file_record.filename)
            raise file_content_missing_exception

        if zip_download_url != "<PACKING>":
            await redis_client.delete(oss_packup_lock_key)

        return zip_download_url

    except HTTPException as e:
        logger.error(f"prepare_download_file failed: {e}")
        raise e
    except Exception as e:
        logger.error(f"prepare_download_file failed: {e}")
        raise internal_server_error_exception from e


# 允许签发上传 STS / 允许上传失败回滚清理的文件状态：初始预检记录（""）与上传中断后重试的 UPLOADING
UPLOADABLE_FILE_STATUSES = ("", FileStatus.UPLOADING)


async def validate_simple_upload(
    async_session: AsyncSession,
    current_user: UserInDB,
    filename: str | None,
    pre_record: FileInDB,
    storage_type: str,
) -> None:
    """
    验证简单上传的所有条件：归属、状态、名字、存储类型

    Raises:
        file_not_found_exception: 预检记录不属于当前用户（对外隐藏存在性）
        HTTPException: 其他验证失败时抛出异常
    """
    # 归属校验：当前用户必须拥有该预检记录，防止越权签发 STS 覆盖他人对象
    # 使用 404 而非 403，避免泄露该 pre_file_id 是否存在
    if not await assert_file_owner(async_session, pre_record, current_user):
        logger.warning(
            f"unauthorized upload attempt user={current_user.id} pre_file_id={pre_record.file_id}"
        )
        raise file_not_found_exception

    # 状态校验：仅允许初始预检记录（""）或上传中断后重试的 UPLOADING 状态重新签发 STS
    if pre_record.status not in UPLOADABLE_FILE_STATUSES:
        logger.warning(
            f"invalid upload state user={current_user.id} file_id={pre_record.file_id} status={pre_record.status}"
        )
        raise invalid_request_parameters_exception

    # 存储类型检查
    if storage_type != pre_record.storage_type:
        logger.error(f"storage_type mismatch user={current_user.id}, expected={pre_record.storage_type}, actual={storage_type}")
        raise invalid_request_parameters_exception

    # 文件名检查
    if not filename:
        logger.error(f"filename missing user={current_user.id}")
        raise invalid_request_parameters_exception


#########################
# File  - 改
#########################
async def set_file_public_service(
    async_session: AsyncSession,
    file_record: FileInDB,
) -> None:
    """将文件标记为公开"""
    await update_file_record(async_session, file_record, is_public=True)
    await async_session.flush()


async def update_file_metadata(
    async_session: AsyncSession,
    file_record: FileInDB,
    patch: "FilePatch",
) -> None:
    """部分更新文件元数据（持有者即可修改，权限由 api 层校验）

    只改请求中出现的字段（exclude_unset）；可改范围见 FilePatch 注释。
    公开状态（is_public）不在修改范围，走 set_public 专用接口（公开不可逆）。
    spectrum_mode / storage_mode 的合法取值由请求模型 Literal 约束（非法值 422）。
    """
    updates = patch.model_dump(exclude_unset=True)

    for field, value in updates.items():
        setattr(file_record, field, value)
    async_session.add(file_record)


async def update_download_stats(
    session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB | None = None,
) -> None:
    """
    更新下载统计（原子操作，防止并发丢失更新）

    使用数据库级别的 UPDATE SET field = field + 1，
    避免 ORM read-modify-write 模式在高并发下的竞态条件。

    current_user 为空时（免登录下载）只更新文件维度的统计，跳过用户已用下载次数。
    """
    now = utc_time()
    assert file_record.file_id is not None

    # 原子递增文件下载次数 + 更新最近下载时间
    await atomic_increment(
        session, FileInDB, file_record.file_id,
        "download_count", pk_column="file_id",
    )
    await atomic_set(
        session, FileInDB, file_record.file_id,
        "last_downloaded_at", now, pk_column="file_id",
    )

    # 原子递增用户已用下载次数（仅登录用户）
    if current_user is not None and current_user.id is not None:
        await atomic_increment(
            session, UserInDB, current_user.id,
            "download_used",
        )


async def update_file_record(
    async_session: AsyncSession,
    file_record: FileInDB,
    *,
    status: FileStatus | None = None,
    path: str | None = None,
    is_public: bool | None = None,
    uploaded_at: datetime | None = None,
    image_path: str | None = None,
) -> None:
    """
    更新文件记录的若干字段。

    仅传入需要更新的字段（None 表示保持原值），修改内存对象并加入会话，
    不 flush / commit，由调用方控制事务边界（与 update_origin 一致）。

    注意：download_count / last_downloaded_at 不在此处更新，高并发下载场景应
    使用 update_download_stats 的原子操作，避免 read-modify-write 丢失更新。
    """
    updates = {
        "status": status,
        "path": path,
        "is_public": is_public,
        "uploaded_at": uploaded_at,
        "image_path": image_path,
    }
    for field, value in updates.items():
        if value is not None:
            setattr(file_record, field, value)
    async_session.add(file_record)


async def remove_file_link(async_session: AsyncSession, user: UserInDB, file: FileInDB) -> bool:
    """
    删除文件时调用,解除用户与文件的关联关系

    直接执行 DELETE 并按受影响行数判断是否真正解除了关联：DELETE 是当前读，
    并发请求中只有一个能删掉该行；"先查再删"在快照读下可能看到已被删除的关联，导致重复扣减配额。

    Returns:
        True 表示确实删除了关联行,False 表示该用户本就不持有此文件

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    result = await async_session.exec(
        delete(UserFileLink).where(
            and_(UserFileLink.user_id == user.id,
                 UserFileLink.file_id == file.file_id)
        )
    )
    async_session.expire(user, ["files"])
    async_session.expire(file, ["owners"])
    return result.rowcount > 0


async def unlink_user_from_file(
    session: AsyncSession,
    user: UserInDB,
    file_record: FileInDB,
) -> bool:
    """
    解除用户与文件的关联

    并发安全：使用原子 SQL UPDATE 递减用户统计字段，避免 read-modify-write
    在并发场景（同一用户并行删除多个文件）下的丢失更新问题。
    `atomic_decrement` 内部使用 GREATEST(field - amount, 0) 保证字段不低于 0。

    Returns:
        True 表示文件还有其他用户持有(只解除关联)
        False 表示文件没有其他用户持有(需要完全删除)
    """
    # 1. 更新当前用户的文件关联和统计信息
    assert file_record.file_id is not None and user.id is not None
    link_removed = await remove_file_link(session, user, file_record)
    session.add(file_record)

    # 2. 更新用户统计信息（原子操作，防止并发丢失更新）
    #    仅在确实删除了关联行时扣减：该用户本就不持有此文件时（越权请求、管理员操作他人文件等）
    #    扣减会让计数虚减，可被用来绕过配额
    if link_removed:
        await atomic_decrement(session, UserInDB, user.id, "file_count")
        await atomic_decrement(session, UserInDB, user.id, "total_file_size", file_record.size)

    # 3. 刷新事务
    await session.flush()
    await session.refresh(file_record)
    # 4. 返回是否还有其他用户持有该文件
    #    注意：此处的 owner_counts 检查与 remove_file_link 之间仍然存在 TOCTOU 竞态
    #    （并发删除最后两个 owner 时可能都判定 count=0 而触发双重物理删除），
    #    该问题超出原子计数器范畴，需要后续通过分布式锁或 SELECT FOR UPDATE 解决。
    owner_counts = (await session.exec(
        select(func.count()).where(UserFileLink.file_id == file_record.file_id) # pylint: disable=not-callable
    )).one()

    return bool(owner_counts)


async def safe_cleanup_preflight_record(
    async_session: AsyncSession, current_user: UserInDB, pre_record: FileInDB
) -> None:
    """
    安全清理预检记录（仅数据库记录和额度）。

    预检后 FC 若始终没有回调，上传对象不会被应用层主动定位；未打持久化
    tag 的 OSS 对象由 bucket 生命周期规则自动回收。
    """
    # 解除用户与文件的关联,并更新用户统计信息
    has_other_owners = await unlink_user_from_file(async_session, current_user, pre_record)

    if has_other_owners:
        # 还有其他用户持有,只更新 owners
        logger.debug(f"preflight cleanup: removed user={current_user.id} from file_id={pre_record.file_id} owners")
    else:
        # 预检记录的 path 尚未由回调确认，不能据此清理 OSS 对象。
        await async_session.delete(pre_record)
        logger.debug(f"preflight cleanup: deleted file_id={pre_record.file_id} completely")


async def rollback_simple_upload(
    async_session: AsyncSession,
    current_user: UserInDB,
    pre_record: FileInDB | None = None,
) -> None:
    """
    简单上传失败时的统一回滚清理
    """
    # 在rollback之前临时保存后面新事务所需要的对象信息，防止rollback之后对象信息失效
    if pre_record is None:
        return

    assert pre_record is not None
    _record_id = pre_record.file_id
    _user_id = current_user.id
    # 回滚事务，撤销已有的数据库操作
    await async_session.rollback()

    if pre_record:
        async with AsyncSession(get_async_engine()) as async_session: # 开启新事务，安全清理预检记录和用户关联
            assert _record_id is not None and _user_id is not None
            pre_record = await get_file_record_by_id(_record_id, async_session)
            # 只清理仍可上传的记录：并发到达的上传回调可能已把它置为 COMPLETED 等状态，此时不能删除
            if pre_record.status not in UPLOADABLE_FILE_STATUSES:
                logger.warning(
                    "skip upload rollback, file is no longer uploadable file_id=%s status=%s",
                    _record_id, pre_record.status,
                )
                return
            current_user = await get_user_by_id(_user_id, async_session) # type: ignore
            await safe_cleanup_preflight_record(async_session, current_user, pre_record)
            await async_session.commit() # 提交事务,使清理生效

    
async def assert_origin_files_ready(
    session: AsyncSession,
    file_record: FileInDB,
) -> OriginImzMLInDB:
    """
    校验 origin 文件就绪——RAW 和 ZIP 打包两条路共用。

    Returns:
        OriginImzMLInDB: 校验通过的 origin 记录

    Raises:
        file_content_missing_exception: origin 不存在、状态异常或文件缺失
    """
    assert file_record.file_id is not None
    origin = await get_origin_imzml_by_file_id(session, file_record.file_id)

    if not origin or origin.status not in [ExperimentStatus.COMPLETED, ExperimentStatus.PROCESSING]:
        logger.warning(
            "origin not found or not completed file_id=%s", file_record.file_id
        )
        raise file_content_missing_exception

    imzml_path = origin.imzml_path if origin.imzml_path else None
    ibd_path = origin.ibd_path if origin.ibd_path else None
    if (
        not imzml_path or not ibd_path or
        not await OssStorage.check_file_exist(imzml_path) or
        not await OssStorage.check_file_exist(ibd_path)
    ):
        logger.warning("origin files missing file_id=%s", file_record.file_id)
        raise file_content_missing_exception

    return origin


async def prepare_download_raw_oss(
    session: AsyncSession,
    file_record: FileInDB,
) -> list[OssDownloadFile]:
    """
    直链下载：不打包，为 origin 的 imzML/ibd 各签一个预签名 URL。

    - 复用 OssStorage.sign_download_url（零改动），下载名由 OSS 对象 basename 决定，
      imzML/ibd 同 stem 配对天然保留。
    - 不触碰 file_record.path —— 避免被 cleanup_old_download_zips 误删 origin 永久文件。
    """
    origin = await assert_origin_files_ready(session, file_record)

    imzml_url = await OssStorage.sign_download_url(origin.imzml_path, expires=SIGNED_URL_EXPIRE_SECONDS)
    ibd_url = await OssStorage.sign_download_url(origin.ibd_path, expires=SIGNED_URL_EXPIRE_SECONDS)
    if not imzml_url or not ibd_url:
        raise file_content_missing_exception

    return [
        OssDownloadFile(filename=Path(origin.imzml_path).name, url=imzml_url, size=origin.imzml_size),
        OssDownloadFile(filename=Path(origin.ibd_path).name, url=ibd_url, size=origin.ibd_size),
    ]


async def get_origin_path_for_packup(
    session: AsyncSession,
    file_record: FileInDB,
) -> str:
    """
    获取用于打包的 origin 文件路径

    Raises:
        file_content_missing_exception: origin 文件不存在或未完成
        origin_type_not_supported_exception: 不支持的实验类型
    """
    if file_record.experiment_type == "imzML":
        origin = await assert_origin_files_ready(session, file_record)
        return f"{Path(origin.imzml_path).parent}/"
    else:
        logger.error("experiment_type not supported, filename=%s, experiment_type=%s", file_record.filename, file_record.experiment_type)
        raise origin_type_not_supported_exception


async def _handle_packup_status_check(
    file_record: FileInDB,
    packup: dict | None = None,
    oss_packup_key: str | None = None,
    oss_packup_lock_key: str | None = None,
) -> tuple[bool, str | None, str | None]:
    """
    检查Redis中的打包状态并处理相应逻辑

    Returns:
        tuple: (should_pack, zip_download_url, zip_path)
    """
    should_pack = False
    zip_download_url = None
    zip_path = None

    if not packup:
        if file_record.path == SOURCE_FILE_PATH_CLEARED:
            # 源 zip 已被定时清理（删除统一按 path 执行，删后必写哨兵），
            # origin imzML/ibd 仍在 → 触发重新打包。
            # 不做 check_file_exist 兜底：秒传链路（preflight_create_file）已保证
            # zip 被清理后该记录不可复用，不会有新对象写回原 key。
            # 原子加锁（SET NX EX），防止多个并发请求同时触发打包。
            # SET key value NX EX seconds 是 Redis 单条原子命令：
            #   - NX：仅当 key 不存在时才设置成功
            #   - EX：设置 10 秒过期，防止持锁进程崩溃导致死锁
            # 替代原来的 GET-then-SET 反模式，彻底消除 check-then-act 竞态窗口。
            lock_acquired = await redis_client.set(oss_packup_lock_key, "1", nx=True, ex=10)  # type: ignore
            if not lock_acquired:
                logger.warning(
                    "packup is in progress by other user, now wait for it to finish, filename=%s",
                    file_record.filename,
                )
                zip_download_url = "<PACKING>"
            else:
                should_pack = True
        else:
            zip_download_url = await OssStorage.sign_download_url(file_record.path, expires=SIGNED_URL_EXPIRE_SECONDS)
            await redis_client.hset(oss_packup_key, mapping={ # type: ignore
                "status": PackupStatus.COMPLETED,
                "file_path": file_record.path,
                "download_url": zip_download_url})  # type: ignore
            await redis_client.expire(oss_packup_key, PACKUP_CACHE_TTL_SECONDS) # type: ignore

    elif packup.get("status") == PackupStatus.PACKING:
        logger.info("packup is in progress, file_id=%s", file_record.file_id)
        zip_download_url = "<PACKING>"

    elif packup.get("status") == PackupStatus.COMPLETED:
        zip_path = packup.get("file_path")
        zip_download_url = packup.get("download_url")
        if not zip_path:
            logger.warning("packup completed but not file_path, now start new packup, filename=%s", file_record.filename)
            should_pack = True
        elif not await OssStorage.check_file_exist(zip_path):
            logger.warning(f"packup completed but zip has been cleaned up, now start new packup, filename={file_record.filename}")
            should_pack = True

    elif packup.get("status") == PackupStatus.FAILED:
        logger.error("packup failed, filename=%s", file_record.filename)
        await redis_client.delete(oss_packup_key) # type: ignore 删除打包状态，允许重新打包
        raise file_content_missing_exception

    else:
        logger.warning("invalid packup status in redis, now start new packup, filename=%s", file_record.filename)
        should_pack = True

    return should_pack, zip_download_url, zip_path


async def _prepare_download_url_for_completed_packup(
    file_record: FileInDB,
    zip_path: str,
    oss_packup_key: str | None,
) -> str | None:
    """为已完成的打包生成下载URL"""
    zip_size = await OssStorage.get_file_size(zip_path)
    if zip_size is None:
        raise file_content_missing_exception

    zip_download_url = await OssStorage.sign_download_url(zip_path, expires=SIGNED_URL_EXPIRE_SECONDS)

    await redis_client.hset(oss_packup_key, mapping={"download_url": zip_download_url})  # type: ignore
    await redis_client.expire(oss_packup_key, PACKUP_CACHE_TTL_SECONDS) # type: ignore

    # 回写 path 指向 packup zip：cleanup_old_download_zips 靠它定位清理对象，
    # 且 FileDownloadMeta 返回的 path 与实际下载内容一致。
    # 注意：此处不 flush/commit，依赖外层 get_download_file_metadata 的事务提交生效。
    # 绝不回写 size：size 是计费字段，恒为压缩前大小，不能被 packup zip 大小污染。
    file_record.path = zip_path

    logger.info(
        "prepared download zip file_id=%s path=%s zip_size=%s",
        file_record.file_id,
        zip_path,
        zip_size,
    )

    return zip_download_url


async def _execute_packup_flow(
    session: AsyncSession,
    file_record: FileInDB,
    oss_packup_key: str,
) -> str:
    """
    执行常规打包流程

    Returns:
        "<PACKING>" 表示正在打包

    Raises:
        internal_server_error_exception: 内部服务器错误
    """
    origin_path = await get_origin_path_for_packup(session, file_record)

    # packup zip 使用 file_id 命名，与原始文件名解耦（同名文件秒传复用场景不冲突）
    fc_request_id = await packup_oss_file(
        filename=file_record.filename,
        packup_folder=origin_path,  # type: ignore
        zip_file_path=packup_zip_path(file_record.file_id),  # type: ignore
    )

    if fc_request_id is None:
        logger.error("packup_oss_file trigger failed, filename=%s", file_record.filename)
        raise internal_server_error_exception

    # 不再把 fc_request_id 写入 Redis：FC 日志回捞链路已删除，此处仅用于判断触发是否成功
    await redis_client.hset(oss_packup_key, mapping={"status": PackupStatus.PACKING})  # type: ignore
    await redis_client.expire(oss_packup_key, PACKUP_CACHE_TTL_SECONDS)

    return "<PACKING>"


async def validate_upload_request(
    async_session: AsyncSession,
    current_user: UserInDB,
    file_data: FileCreate,
) -> bool:
    """
    统一验证上传请求参数

    Returns:
        True 表示验证通过,False 表示验证失败
    """
    # 1. 存储类型验证
    if file_data.storage_type not in ALLOWED_STORAGE_TYPES:
        return False

    # 2. 文件名和类型验证
    if not validate_file_metadata(file_data.filename, file_data.file_type):
        return False

    # 3. 实验类型验证(如果提供)
    if file_data.experiment_type and not validate_experiment_type(file_data.experiment_type):
        return False

    # 4. 分片数量验证(如果需要)
    if file_data.total_parts is not None and not validate_total_parts(file_data.total_parts):
        return False

    # 5. 单文件大小上限校验（max_file_size_gb 未配置时不限制）
    if MAX_SINGLE_FILE_SIZE_BYTES and file_data.size and file_data.size > MAX_SINGLE_FILE_SIZE_BYTES:
        logger.error(f"file size exceeds limit user={current_user.id} size={file_data.size} limit={MAX_SINGLE_FILE_SIZE_BYTES}")
        return False

    # 6. 用户配额检查
    if not check_user_quota(current_user, file_data.size):
        raise user_file_limit_exception

    # 7. 重复文件名检查
    if await valid_duplicate_filename(async_session, current_user, file_data.filename):
        raise duplicate_file_exception

    # 8. 重复文件哈希检查
    assert file_data.file_verify_code is not None
    if await valid_user_duplicate_file_hash(async_session, current_user, file_data.file_verify_code):
        raise duplicate_file_exception

    return True
