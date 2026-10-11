#########################
# File Service - 业务逻辑层
# 职责:业务逻辑处理,数据库操作,但不直接暴露API
# 包含:用户文件关联管理、文件记录查询、权限校验、OSS 清理逻辑
#########################
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from sqlmodel import select, exists, and_, or_, not_, func, col, delete
from sqlmodel.ext.asyncio.session import AsyncSession

from database.db import get_async_engine
from database.atomic_ops import atomic_increment, atomic_decrement, atomic_set
from api_services.user_service import get_user_by_id
from api_services.experiment_service import get_origin_imzml_by_file_id
from api_helpers.experiment_helper import validate_experiment_type
from api_helpers.file_helper import (
    check_user_quota,
    validate_file_metadata,
)
from storage.oss.oss_storage import OssStorage

from constants.file_constants import (
    ALLOWED_STORAGE_TYPES,
    MAX_SINGLE_FILE_SIZE_BYTES,
    OSS_BUCKET_NAME,
    OSS_REGION_ID,
    SIGNED_URL_EXPIRE_SECONDS,
)
from exceptions import (
    duplicate_file_exception,
    file_not_found_exception,
    file_content_missing_exception,
    file_persist_failed_exception,
    invalid_request_parameters_exception,
    invalid_user_for_file_list_sync_exception,
    user_file_limit_exception,
)
from exceptions.common_exceptions import internal_server_error_exception
from tools import get_logger,utc_time
from models.file_models import (
    FileCreate, FileInDB, FileOssUploadResponse, FilePatch, FileStatus, OssDownloadFile,
)
from models.filter_models import FileFilter
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


async def get_file_record_by_public_id(
    public_id: str,
    async_session: AsyncSession,
    *,
    include_deleting: bool = False,
) -> FileInDB:
    """
    根据 public_id 查询文件记录（对外接口统一入口，内部 file_id 不再对外暴露）

    删除流程中的文件处理与 get_file_record_by_id 完全一致：DELETING / DELETE_FAILED
    默认按不存在处理，删除登记等管理路径用 include_deleting=True 显式放行。
    不校验公开状态与持有关系，权限由调用方判断。

    Returns:
        文件记录对象
    Raises:
        file_not_found_exception: 文件记录不存在（或处于删除流程且未放行）
    """
    statement = select(FileInDB).where(FileInDB.public_id == public_id)

    file_record = (await async_session.exec(statement)).first()

    if not file_record:
        logger.warning("file record not found public_id=%s", public_id)
        raise file_not_found_exception

    if not include_deleting and file_record.status in (FileStatus.DELETING, FileStatus.DELETE_FAILED):
        logger.warning(
            "file is being deleted, treat as not found public_id=%s file_id=%s status=%s",
            public_id, file_record.file_id, file_record.status,
        )
        raise file_not_found_exception

    return file_record


async def get_file_ids_by_public_ids(
    async_session: AsyncSession,
    public_ids: list[str],
) -> dict[str, int]:
    """批量换算 public_id → file_id（API 层对外标识换算入口，业务层一律走 file_id）

    只返回能查到的映射；查不到的 public_id 不在结果中，缺失语义由调用方决定
    （批量加成员/创建集合 409、移除成员记 skipped、调序 409）。
    不过滤删除流程中的记录（含 DELETING）——移除成员等场景需要能换算到它们。

    Returns:
        {public_id: file_id}（仅含存在的记录）
    """
    if not public_ids:
        return {}

    unique_ids = list(dict.fromkeys(public_ids))
    rows = (await async_session.exec(
        select(FileInDB.public_id, FileInDB.file_id).where(
            FileInDB.public_id.in_(unique_ids)  # type: ignore # pylint: disable=no-member
        )
    )).all()
    return {public_id: file_id for public_id, file_id in rows}  # type: ignore


async def get_public_ids_by_file_ids(
    async_session: AsyncSession,
    file_ids: list[int],
) -> dict[int, str]:
    """批量换算 file_id → public_id（API 层响应装配用：内部 id → 对外标识）

    Returns:
        {file_id: public_id}（仅含存在的记录）
    """
    if not file_ids:
        return {}

    unique_ids = list(dict.fromkeys(file_ids))
    rows = (await async_session.exec(
        select(FileInDB.file_id, FileInDB.public_id).where(
            FileInDB.file_id.in_(unique_ids)  # type: ignore # pylint: disable=no-member
        )
    )).all()
    return {file_id: public_id for file_id, public_id in rows}  # type: ignore


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
    primary = col(column).asc() if order == "asc" else col(column).desc() # pylint: disable=no-member
    return (primary, col(FileInDB.file_id).desc()) # pylint: disable=no-member


# 精确匹配（IN）的筛选字段：FileInDB 上的单值列，多值语义即为"命中任一值"
FILE_EXACT_FILTER_FIELDS = (
    "experiment_type",
    "organism",
    "organism_part",
    "condition",
    "sample_stabilization",
    "sample_growth_conditions",
    "tissue_modification",
    "maldi_matrix",
    "maldi_matrix_application",
    "solvent",
    "polarity",
    "ionisation_source",
    "analyzer",
)

# 模糊匹配（LIKE %v%）的筛选字段：多值语义为"任一关键词命中"
FILE_FUZZY_FILTER_FIELDS = (
    "filename",
    "first_uploaded_by",
)


def build_file_filters(filters: FileFilter) -> list[Any]:
    """FileFilter → SQL 谓词列表（同字段多值 OR，不同字段之间由调用方 AND）

    供个人文件列表与公共文件列表共用，口径与 build_file_order 一致：
    空值不产生谓词（归一化已在 Filter 模型内完成，空值一律为 None）。

    模糊字段为 OR 连接的 LIKE，精确字段为 IN；大小写由列排序规则
    （utf8mb4_unicode_ci）保证不敏感，无需额外处理。

    注意：username 筛选需要 join 用户-文件关联表，不在此处生成，由调用方单独处理。
    """
    clauses: list[Any] = []

    for field in FILE_FUZZY_FILTER_FIELDS:
        values = getattr(filters, field)
        if values:
            clauses.append(
                or_(*[col(getattr(FileInDB, field)).like(f"%{value}%") for value in values])
            )

    for field in FILE_EXACT_FILTER_FIELDS:
        values = getattr(filters, field)
        if values:
            clauses.append(col(getattr(FileInDB, field)).in_(values))

    return clauses


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


# 允许签发上传 STS / 允许上传失败回滚清理的文件状态：初始预检记录（""）与上传中断后重试的 UPLOADING
UPLOADABLE_FILE_STATUSES = ("", FileStatus.UPLOADING)


async def validate_upload(
    async_session: AsyncSession,
    current_user: UserInDB,
    filename: str | None,
    pre_record: FileInDB,
) -> None:
    """
    验证签发上传凭证前的条件：归属、状态、名字

    存储类型由上传入口按预检记录的 storage_type 分发，此处不再比对

    Raises:
        file_not_found_exception: 预检记录不属于当前用户（对外隐藏存在性）
        HTTPException: 其他验证失败时抛出异常
    """
    # 归属校验：当前用户必须拥有该预检记录，防止越权签发 STS 覆盖他人对象
    # 使用 404 而非 403，避免泄露该 pre_public_id 是否存在
    if not await assert_file_owner(async_session, pre_record, current_user):
        logger.warning(
            f"unauthorized upload attempt user={current_user.id} file_id={pre_record.file_id}"
        )
        raise file_not_found_exception

    # 状态校验：仅允许初始预检记录（""）或上传中断后重试的 UPLOADING 状态重新签发 STS
    if pre_record.status not in UPLOADABLE_FILE_STATUSES:
        logger.warning(
            f"invalid upload state user={current_user.id} file_id={pre_record.file_id} status={pre_record.status}"
        )
        raise invalid_request_parameters_exception

    # 文件名检查
    if not filename:
        logger.error(f"filename missing user={current_user.id}")
        raise invalid_request_parameters_exception


async def upload_oss_file(
    current_user: UserInDB,
    async_session: AsyncSession,
    pre_record: FileInDB,
    filename: str,
) -> FileOssUploadResponse:
    """
    【OSS 上传实现】

    校验预检记录并返回限定对象路径的 STS 临时凭证，文件内容由前端
    通过 OSS SDK 直传。自行提交事务，失败时由 rollback_upload 补偿。

    OSS 对象清理策略：
    - 回调在路径提交后失败时，失败实验定时任务会按已落库路径尝试删除
      source/imzML/ibd 对象。
    - 回调在路径提交前失败或 FC 完全没有回调时，数据库中没有可靠对象键；
      未写入 persistent=true 标签的对象由 OSS 生命周期规则回收。
    """
    # 预检记录校验放在 try 之外：校验失败直接返回对应错误，不触发回滚。
    # 回滚会解除关联并删除预检记录，对他人的 pre_public_id 或已完成的文件执行时，
    # 会造成配额虚减或误删已完成文件
    logger.info("oss upload start user=%s filename=%s", current_user.id, filename)
    await validate_upload(async_session, current_user, filename, pre_record)

    try:
        # OSS 对象键使用唯一标识 data/{file_verify_code}/{file_id}.{file_type}，
        # 不再使用文件名；verify_code 前置打散 OSS 分区，FC 从文件名解析 file_id 后回调后端
        assert pre_record.file_id is not None and pre_record.file_verify_code is not None
        oss_path = OssStorage.prepare_storage_path(
            pre_record.file_id, pre_record.file_verify_code, pre_record.file_type,
        )
        oss_sts_token = await OssStorage.get_upload_sts_token(file_path=oss_path)
        if not oss_sts_token or not oss_path:
            raise internal_server_error_exception

        await update_file_record(async_session, pre_record, status=FileStatus.UPLOADING)
        await async_session.commit()
        await async_session.refresh(pre_record)

    except Exception as exc:
        logger.error(
            "oss upload failed user=%s filename=%s error=%s",
            current_user.id, filename, exc,
        )
        await rollback_upload(async_session, current_user, pre_record)
        raise file_persist_failed_exception from exc

    response = FileOssUploadResponse.model_validate(pre_record)
    response.oss_path = oss_path
    response.oss_sts_token = oss_sts_token
    response.oss_bucket = OSS_BUCKET_NAME
    response.oss_region_id = OSS_REGION_ID
    return response


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


async def rollback_upload(
    async_session: AsyncSession,
    current_user: UserInDB,
    pre_record: FileInDB | None = None,
) -> None:
    """
    上传失败时的统一回滚清理
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
    - 不触碰 file_record.path —— 避免被 cleanup_source_zips 误删 origin 永久文件。
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


async def validate_preflight_request(
    async_session: AsyncSession,
    current_user: UserInDB,
    file_data: FileCreate,
) -> bool:
    """
    统一验证预检请求参数

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

    # 4. 单文件大小上限校验（max_file_size_gb 未配置时不限制）
    if MAX_SINGLE_FILE_SIZE_BYTES and file_data.size and file_data.size > MAX_SINGLE_FILE_SIZE_BYTES:
        logger.error(f"file size exceeds limit user={current_user.id} size={file_data.size} limit={MAX_SINGLE_FILE_SIZE_BYTES}")
        return False

    # 5. 用户配额检查
    if not check_user_quota(current_user, file_data.size):
        raise user_file_limit_exception

    # 6. 重复文件名检查
    if await valid_duplicate_filename(async_session, current_user, file_data.filename):
        raise duplicate_file_exception

    # 8. 重复文件哈希检查
    assert file_data.file_verify_code is not None
    if await valid_user_duplicate_file_hash(async_session, current_user, file_data.file_verify_code):
        raise duplicate_file_exception

    return True
