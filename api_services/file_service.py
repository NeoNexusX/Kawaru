#########################
# File Service - 业务逻辑层
# 职责:业务逻辑处理,数据库操作,但不直接暴露API
# 包含:用户文件关联管理、文件记录查询、权限校验、清理逻辑、分片合并
#########################
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import HTTPException
from sqlalchemy.orm import selectinload
from sqlmodel import select, exists, and_, not_, func
from sqlmodel.ext.asyncio.session import AsyncSession
from celery.result import AsyncResult

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
    merge_files_and_get_hash,
    delete_file_by_storage,
)
from storage.local.local_storage import LocalStorage
from storage.oss.oss_storage import OssStorage

from celery_runtime.result import wait_for_result
from background.file_background_helper import build_zip_file_background
from constants.file_constants import (
    ALLOWED_STORAGE_TYPES,
    LOCAL_STORAGE_ROOT,
    OSS_PACKUP_ZIP_STORAGE_ROOT,
    SIGNED_URL_EXPIRE_SECONDS,
    PACKUP_CACHE_TTL_SECONDS,
)
from exceptions import (
    duplicate_file_exception,
    part_file_missing_exception,
    file_not_found_exception,
    file_content_missing_exception,
    invalid_request_parameters_exception,
    invalid_user_for_file_list_sync_exception,
    merged_file_hash_mismatch_exception,
    part_already_exists_exception,
    part_number_out_of_range_exception,
    user_file_limit_exception,
    hash_size_validation_failed_exception,
    origin_type_not_supported_exception,
)
from exceptions.common_exceptions import internal_server_error_exception
from tools import get_logger,utc_time
from models.file_models import FileCreate, FileInDB, PartInDB, PackupStatus, OssDownloadFile
from models.user_models import UserInDB
from models.link_models import UserFileLink
from models.experiment_models import ExperimentStatus, OriginImzMLInDB


from boot.config import get_config

if TYPE_CHECKING:
    from models.file_models import PartCreate

logger = get_logger("api_services.file_service")

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time


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
    load_parts: bool = False
) -> FileInDB:
    """
    根据文件ID查询文件记录

    Returns:
        文件记录对象
    Raises:
        file_not_found_exception: 文件记录不存在
    """
    statement = select(FileInDB).where(FileInDB.file_id == file_id)

    if load_parts:
        statement = statement.options(selectinload(FileInDB.parts)) # type: ignore
    file_record = (await async_session.exec(statement)).first()

    if not file_record:
        logger.warning("file record not found file_id=%s", file_id)
        raise file_not_found_exception

    return file_record


async def get_file_by_filename(
    filename: str,
    async_session: AsyncSession,
    load_parts: bool = False,
) -> FileInDB:
    """
    根据文件名查询文件记录

    Returns:
        文件记录对象
    Raises:
        file_not_found_exception: 文件记录不存在
    """
    statement = select(FileInDB).where(FileInDB.filename == filename)

    if load_parts:
        statement = statement.options(selectinload(FileInDB.parts)) # type: ignore
    file_record = (await async_session.exec(statement)).first()

    if not file_record:
        logger.warning("file record not found filename=%s", filename)
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
async def get_file_record_by_hash(
    async_session: AsyncSession,
    file_hash: str,
) -> FileInDB:
    """
    根据文件哈希查询文件记录

    Returns:
        文件记录对象
    Raises:
        file_not_found_exception: 文件记录不存在
    """
    file_record = (
        await async_session.exec(
            select(FileInDB).where(FileInDB.file_verify_code == file_hash)
        )
    ).first()
    if not file_record:
        raise file_not_found_exception
    return file_record


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
    # 直接通过 SQL 子句找到 hash 相同但不属于该用户的文件
    same_hash_file = (
        await async_session.exec(
            select(FileInDB)
            .where(FileInDB.file_verify_code == hash_md5)
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


async def prepare_download_file_local(
    session: AsyncSession,
    file_record: FileInDB,
) -> Path:
    """
    准备下载文件,处理实验文件的ZIP打包

    Returns:
        文件路径

    Raises:
        file_content_missing_exception: 文件内容缺失
    """
    try:
        # 检查源文件是否存在
        path = Path(file_record.path) if file_record.path else None
        # 文件存在,直接返回
        if path and path.exists():
            return path

        # 处理 origin 文件(imzML) # TODO： 封装函数
        if file_record.experiment_type == "imzML":
            assert file_record.file_id is not None
            origin = await get_origin_imzml_by_file_id(session, file_record.file_id)

            if not origin or origin.status not in [ExperimentStatus.COMPLETED, ExperimentStatus.PROCESSING]:
                logger.warning(
                    "origin not found or not completed file_id=%s", file_record.file_id
                )
                raise file_content_missing_exception

            # 获取 origin 文件路径
            imzml_path = Path(origin.imzml_path) if origin.imzml_path else None
            ibd_path = Path(origin.ibd_path) if origin.ibd_path else None
            if (not imzml_path or not ibd_path or not imzml_path.exists() or not ibd_path.exists()):
                logger.warning("origin files missing file_id=%s", file_record.file_id)
                raise file_content_missing_exception

            # 打包文件
            # 创建 ZIP 包(存放在本地存储根目录下)
            zip_filename = f"{file_record.filename}.{file_record.file_type }"
            LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
            zip_path = LOCAL_STORAGE_ROOT / zip_filename

            # 准备文件列表:(文件路径, ZIP中的文件名)，通过 str 传递给 Celery
            assert isinstance(imzml_path, Path) and isinstance(ibd_path, Path)
            files_to_zip = [
                (str(imzml_path), imzml_path.name),
                (str(ibd_path), ibd_path.name),
            ]

            task_result: AsyncResult = build_zip_file_background.delay(files_to_zip, str(zip_path)) # type: ignore
            zip_size = await wait_for_result(task_result, poll_interval=0.1, timeout=large_file_poll_time) # type: ignore
            if zip_size is None:
                raise file_content_missing_exception

            # 更新文件记录
            file_record.path = str(zip_path)
            file_record.size = zip_size # type: ignore

            logger.info(
                "created download zip file_id=%s path=%s size=%s",
                file_record.file_id,
                zip_path,
                file_record.size,
            )

            return zip_path

        raise file_content_missing_exception

    except HTTPException as e:
        logger.error(f"prepare_download_file failed: {e}")
        raise e
    except Exception as e:
        logger.error(f"prepare_download_file failed: {e}")
        raise internal_server_error_exception from e


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
    oss_packup_key = f"oss_packup-{file_record.filename}"
    oss_packup_lock_key = f"oss_packup_lock-{file_record.filename}"

    try:
        packup = await redis_client.hgetall(oss_packup_key)  # type: ignore
        should_pack, zip_download_url, zip_path, fc_request_id = await _handle_packup_status_check(
            file_record, packup, oss_packup_key, oss_packup_lock_key
        )

        if packup.get("status") == PackupStatus.COMPLETED and zip_download_url is None and zip_path:
            zip_download_url = await _prepare_download_url_for_completed_packup(
                file_record, zip_path, fc_request_id, oss_packup_key
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


async def validate_simple_upload(
    current_user: UserInDB,
    filename: str | None,
    pre_record: FileInDB,
    storage_type: str,
) -> None:
    """
    验证简单上传的所有条件：包含名字、存储类型

    Raises:
        HTTPException: 验证失败时抛出异常
    """
    # 存储类型检查
    if storage_type != pre_record.storage_type:
        logger.error(f"storage_type mismatch user={current_user.id}, expected={pre_record.storage_type}, actual={storage_type}")
        raise invalid_request_parameters_exception

    # 文件名检查
    if not filename:
        logger.error(f"filename missing user={current_user.id}")
        raise invalid_request_parameters_exception


async def validate_hash_and_size(
    expected_hash: str,
    expected_size: int,
    file_path: str,
    storage_type: str,
) -> tuple[str, int]:
    """
    验证哈希和大小是否匹配，适用于文件或分片的验证

    Raises:
        hash_size_validation_failed_exception: 验证失败时抛出异常
    """
    act_hash, act_size = "", 0
    # 读取文件内容
    if storage_type == "local":
        #哈希、文件大小检查
        act_hash, act_size = await LocalStorage.compute_upload_file_hash(file_path)
    elif storage_type == "oss":
        # TODO： 从 OSS 中获取文件哈希和大小
        # 默认oss上传成功后，哈希已验证，无需再验证
        act_size = await OssStorage.get_file_size(file_path)
        act_hash = expected_hash

    if act_size != expected_size or act_hash != expected_hash:
        logger.error(f"file or part hash or size mismath\n"
                     f"expected_hash={expected_hash} actual_hash={act_hash}\n"
                     f"expected_size={expected_size} actual_size={act_size}")
        raise hash_size_validation_failed_exception

    return act_hash, act_size


#########################
# File  - 改
#########################
async def set_file_public_service(
    async_session: AsyncSession,
    file_record: FileInDB,
) -> None:
    """将文件标记为公开"""
    file_record.is_public = True
    async_session.add(file_record)
    await async_session.flush()


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


#########################
# File  - 删
#########################
async def delete_file_and_parts(
    async_session: AsyncSession,
    file_id: int,
    current_user: UserInDB,
) -> None:
    """
    根据 file_id 删除文件记录及其所有分片(物理文件 + 数据库记录)

    Raises:
        ValueError: 文件不存在
        PermissionError: 用户无权限删除该文件
        OSError: 物理文件删除失败时
    """
    # 1. 查询文件记录,同时预加载对应的分片记录
    file_record = await get_file_record_by_id(file_id, async_session, load_parts=True)
    storage_type =  file_record.storage_type

    # 2. 删除分片物理文件（oss不需要删除）(数据库记录会在删除文件记录时自动级联删除)
    if storage_type == "local":
        for part in file_record.parts:
            await delete_file_by_storage(part.temp_path, storage_type)
            logger.debug(f"deleted part file part_id={part.part_id} temp_path={part.temp_path}")

    # 3. 删除文件的物理文件
    await delete_file_by_storage(file_record.path, storage_type)
    logger.debug(f"deleted file physical file path={file_record.path} file_id={file_id}")

    # 5. 删除文件数据库记录(数据库会自动级联删除分片记录)
    await async_session.delete(file_record)

    logger.info(f"deleted file and all parts file_id={file_id} user={current_user.id}")


async def remove_file_link(async_session: AsyncSession, user: UserInDB, file: FileInDB) -> None:
    """
    删除文件时调用,解除用户与文件的关联关系

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
    """
    if not user.id:
        raise invalid_user_for_file_list_sync_exception

    user_file_linkers = (await async_session.exec(
        select(UserFileLink).where(
            and_(UserFileLink.user_id == user.id,
                 UserFileLink.file_id == file.file_id)
        )
    )).all()

    for linker in user_file_linkers:
        await async_session.delete(linker)
    async_session.expire(user, ["files"])
    async_session.expire(file, ["owners"])


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
    await remove_file_link(session, user, file_record)
    session.add(file_record)

    # 2. 更新用户统计信息（原子操作，防止并发丢失更新）
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
    安全清理预检记录
    """
    # 解除用户与文件的关联,并更新用户统计信息
    has_other_owners = await unlink_user_from_file(async_session, current_user, pre_record)

    if has_other_owners:
        # 还有其他用户持有,只更新 owners
        logger.debug(f"preflight cleanup: removed user={current_user.id} from file_id={pre_record.file_id} owners")
    else:
        # 没有其他用户持有,删除记录(包括物理文件和分片)
        assert pre_record.file_id is not None
        await delete_file_and_parts(async_session, pre_record.file_id, current_user)
        logger.debug(f"preflight cleanup: deleted file_id={pre_record.file_id} completely")


async def rollback_simple_upload(
    async_session: AsyncSession,
    current_user: UserInDB,
    pre_record: FileInDB | None = None,
    final_path: "Path | str | None" = None,
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
    storage_type = pre_record.storage_type

    # 回滚事务，撤销已有的数据库操作
    await async_session.rollback()

    if final_path:
        await delete_file_by_storage(final_path, storage_type)
    if pre_record:
        async with AsyncSession(get_async_engine()) as async_session: # 开启新事务，安全清理预检记录和用户关联
            assert _record_id is not None and _user_id is not None
            pre_record = await get_file_record_by_id(_record_id, async_session)
            current_user = await get_user_by_id(_user_id, async_session) # type: ignore
            await safe_cleanup_preflight_record(async_session, current_user, pre_record)
            await async_session.commit() # 提交事务,使清理生效


async def rollback_part_upload(async_session: AsyncSession,
                               path: Path | None,
                               file_record:FileInDB | None) -> None:
    """
    分片上传失败时的统一回滚清理
    """
    if path and file_record:
        await delete_file_by_storage(path, file_record.storage_type)

    await async_session.rollback()


#########################
# Part  - 增
#########################
async def merge_parts_to_file(
    async_session: AsyncSession,
    file_record: FileInDB,
    parts: list[PartInDB],
    current_user: UserInDB,
) -> None:
    """
    执行分片合并,将所有分片写入最终文件

    事务管理:不 commit,由调用方(API 层)统一管理
    """
    part_paths = [Path(part.temp_path) for part in parts]
    final_path = None
    assert file_record.file_id is not None

    try:
        # 分片上传预检时 path 为空，合并前构建最终存储路径
        if not file_record.path:
            final_path = LocalStorage.prepare_storage_path(file_record.filename, file_record.file_type)
            file_record.path = str(final_path)
            async_session.add(file_record)
        else:
            final_path = Path(file_record.path)
        # 使用 helper 层函数合并文件并验证哈希
        await merge_files_and_get_hash(part_paths, final_path, file_record.file_verify_code)

    except FileNotFoundError as exc:
        # 分片文件丢失
        logger.error(f"part file missing file_id={file_record.file_id} error={exc}")
        await delete_file_and_parts(async_session, file_record.file_id, current_user)
        raise part_file_missing_exception from exc

    except ValueError as exc:
        # 哈希不匹配
        logger.error(f"merged file hash mismatch file_id={file_record.file_id} error={exc}")
        await delete_file_and_parts(async_session, file_record.file_id, current_user)
        raise merged_file_hash_mismatch_exception from exc

    # 合并成功,更新首次上传时的 download 时间 (等于 uploaded_at)
    file_record.uploaded_at = utc_time()
    file_record.last_downloaded_at = file_record.uploaded_at
    async_session.add(file_record)

    # 清理分片临时文件和记录
    await delete_parts_by_file_id(async_session, file_record.file_id)


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
) -> tuple[bool, str | None, str | None, str | None]:
    """
    检查Redis中的打包状态并处理相应逻辑

    Returns:
        tuple: (should_pack, zip_download_url, zip_path, fc_request_id)
    """
    should_pack = False
    zip_download_url = None
    zip_path = None
    fc_request_id = None

    if not packup:
        if not await OssStorage.check_file_exist(file_record.path):
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
        fc_request_id = packup.get("fc_request_id")
        zip_download_url = packup.get("download_url")
        if not zip_path:
            logger.warning("packup completed but not file_path, now start new packup, filename=%s", file_record.filename)
            should_pack = True
        elif not await OssStorage.check_file_exist(zip_path):
            logger.warning(f"packup completed but zip has been cleaned up, now start new packup, filename={file_record.filename}")
            should_pack = True

    elif packup.get("status") == PackupStatus.FAILED:
        logger.error("packup failed, filename=%s", file_record.filename)
        fc_request_id = packup.get("fc_request_id")
        await redis_client.delete(oss_packup_key) # type: ignore 删除打包状态，允许重新打包
        raise file_content_missing_exception

    else:
        logger.warning("invalid packup status in redis, now start new packup, filename=%s", file_record.filename)
        should_pack = True

    return should_pack, zip_download_url, zip_path, fc_request_id


async def _prepare_download_url_for_completed_packup(
    file_record: FileInDB,
    zip_path: str,
    fc_request_id: str | None,
    oss_packup_key: str | None,
) -> str | None:
    """为已完成的打包生成下载URL"""
    zip_size = await OssStorage.get_file_size(zip_path)
    if zip_size is None:
        raise file_content_missing_exception

    zip_download_url = await OssStorage.sign_download_url(zip_path, expires=SIGNED_URL_EXPIRE_SECONDS)

    await redis_client.hset(oss_packup_key, mapping={"download_url": zip_download_url})  # type: ignore
    await redis_client.expire(oss_packup_key, PACKUP_CACHE_TTL_SECONDS) # type: ignore

    file_record.path = zip_path
    file_record.size = zip_size  # type: ignore

    logger.info(
        "prepared download zip file_id=%s path=%s size=%s",
        file_record.file_id,
        zip_path,
        file_record.size,
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

    fc_request_id = await packup_oss_file(
        filename=file_record.filename,
        packup_folder=origin_path,  # type: ignore
        zip_file_path=f"{OSS_PACKUP_ZIP_STORAGE_ROOT.strip('/')}/{Path(file_record.filename).stem}.zip",  # type: ignore
    )

    if fc_request_id is None:
        logger.error("packup_oss_file trigger failed, filename=%s", file_record.filename)
        raise internal_server_error_exception

    await redis_client.hset(oss_packup_key, mapping={"status": PackupStatus.PACKING, "fc_request_id": fc_request_id})  # type: ignore
    await redis_client.expire(oss_packup_key, PACKUP_CACHE_TTL_SECONDS)

    return "<PACKING>"


#########################
# Part  - 查
#########################
async def validate_part_upload(
    async_session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
    part_data: PartCreate,
) -> None:
    """
    分片上传前置验证(编号范围 + 重复检查)

    Raises:
        part_number_out_of_range_exception: 分片编号越界
        part_already_exists_exception: 分片已上传
    """
    # 1. 分片编号范围校验
    if not part_data.part_number or not file_record.total_parts:
        raise part_number_out_of_range_exception

    if not 1 <= part_data.part_number <= file_record.total_parts:
        raise part_number_out_of_range_exception

    # 2. 重复检查
    if not file_record.file_id or not current_user.id:
        raise invalid_request_parameters_exception
    if await validate_part_duplicate(async_session,
                                     file_record.file_id,
                                     part_data.part_number,
                                     current_user.id):
        raise part_already_exists_exception


async def validate_part_duplicate(
    async_session: AsyncSession,
    file_id: int,
    part_number: int,
    owner_id: int,
) -> bool:
    """
    检查分片是否已重复上传
    Returns:
        True 表示分片已存在,False 表示不存在
    """
    stmt = select(exists().where(
        and_(
            PartInDB.file_id == file_id,
            PartInDB.part_number == part_number,
            PartInDB.owner_id == owner_id,
        )
    ))
    # 返回布尔值
    return (await async_session.exec(stmt)).one()


async def validate_and_merge_parts_business(
    session: AsyncSession, file_record: FileInDB, current_user: UserInDB
) -> bool:
    """
    验证分片完整性并尝试合并

    Raises:
        part_file_missing_exception: 分片文件丢失
        merged_file_hash_mismatch_exception: 合并后哈希不匹配
    """
    # 1. 查询该文件的所有分片记录
    parts = (
        await session.exec(
            select(PartInDB)
            .where(
                PartInDB.file_id == file_record.file_id,
                PartInDB.owner_id == current_user.id,
            )
            .order_by(PartInDB.part_number) # type: ignore
        )
    ).all()

    if not parts:
        logger.warning(f"no parts found for file_id={file_record.file_id} user={current_user.id}")
        return False

    expected_total_parts = file_record.total_parts
    if not expected_total_parts:
        logger.error("total_parts not set for file_id=%s", file_record.file_id)
        return False

    # 2. 检查分片完整性(数量和序列)
    uploaded_parts = sorted([part.part_number for part in parts])
    expected_parts = list(range(1, expected_total_parts + 1))
    logger.info(f"part status file_id={file_record.file_id} uploaded={uploaded_parts} expected={expected_parts}")

    # 3. 分片不完整,等待更多分片
    if len(uploaded_parts) < expected_total_parts:
        return False

    # 分片序列不连续(这是正常的中间状态,等待缺失的分片)
    if uploaded_parts != expected_parts:
        logger.warning(f"part sequence incomplete file_id={file_record.file_id} uploaded={uploaded_parts} expected={expected_parts}")
        return False

    # 4. 所有分片已上传,开始合并
    logger.info(f"all parts uploaded, starting merge file_id={file_record.file_id} user={current_user.id}")
    parts = sorted(parts, key=lambda c: c.part_number)

    # 5. 执行分片合并
    await merge_parts_to_file(session, file_record, parts, current_user)

    await session.commit()
    await session.refresh(file_record)
    await session.refresh(current_user) #TODO 此处确实会破坏事物原子性
    logger.info(f"parts merged successfully file_id={file_record.file_id} user={current_user.id}")
    return True


#########################
# Part  - 删
#########################
async def delete_parts_by_file_id(
    session: AsyncSession,
    file_id: int,
) -> int:
    """
    根据 file_id 删除分片记录(物理文件 + 数据库记录)

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

    # 先删除物理文件,再删除数据库记录
    for part in parts:
        # 删除分片物理文件
        LocalStorage.delete_file(part.temp_path)
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

    # 5. 用户配额检查
    if not check_user_quota(current_user, file_data.size):
        raise user_file_limit_exception

    # 6. 重复文件名检查
    if await valid_duplicate_filename(async_session, current_user, file_data.filename):
        raise duplicate_file_exception

    # 7. 重复文件哈希检查
    assert file_data.file_verify_code is not None
    if await valid_user_duplicate_file_hash(async_session, current_user, file_data.file_verify_code):
        raise duplicate_file_exception

    return True
