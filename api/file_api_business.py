#########################
# File API Business - 业务逻辑层
# 职责:业务逻辑处理,数据库操作,但不直接暴露API
# 包含:用户文件关联管理、文件记录查询、权限校验、清理逻辑、分片合并
#########################
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import HTTPException
from sqlmodel import select, exists, and_, not_, func
from sqlmodel.ext.asyncio.session import AsyncSession
from celery.result import AsyncResult

from database.db import get_async_engine
from database.redis_client import redis_client
from api.user_api_helper import get_user_by_id
from api.experiment_api_business import get_origin_imzml_by_file_id
from api.experiment_api_helper import validate_experiment_type, packup_oss_file
from api.file_api_helper import (
    check_user_quota,
    get_file_record_by_id,
    valid_duplicate_filename,
    valid_user_duplicate_file_hash,
    validate_file_metadata,
    validate_total_parts,
    merge_files_and_get_hash,
    save_upload_file,
    delete_file_by_storage,
)
from storage_backend.storage import OssStorage, LocalStorage
from storage_backend.oss_log_helper import get_fc_invoke_logs_by_request_id_in_background

from celery_runtime.result import wait_for_result
from background.file_background_helper import build_zip_file_background
from constants.file_constants import (
    ALLOWED_STORAGE_TYPES,
    LOCAL_STORAGE_ROOT,
    OSS_PACKUP_ZIP_STORAGE_ROOT,
    FC_PACKUP_TIMEOUT_SECONDS,
    DOWNLOAD_ZIP_TTL_MINUTES,
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
from models.file_models import FileCreate, FileInDB, PartInDB, PackupStatus
from models.user_models import UserInDB
from models.link_models import UserFileLink
from models.experiment_models import ExperimentStatus

from boot.config import get_config

if TYPE_CHECKING:
    from fastapi import UploadFile
    from models.file_models import PartCreate

logger = get_logger("api.file_api_business")

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time
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

    完整的文件关联流程:
    1. 添加文件和用户的 UserFileLinker 关联
    2. 增加用户的文件计数
    3. 增加用户的总存储空间
    4. 将用户对象添加到会话(准备提交)

    Raises:
        invalid_user_for_file_list_sync_exception: 用户ID无效
        user_file_limit_exception: 超过用户文件数量限制
    """
    # 1. 将文件添加到用户的文件列表
    new_linker = await append_file_link(async_session, user, file)

    # 2. 更新用户的文件统计信息
    if new_linker:
        user.file_count = (user.file_count or 0) + 1
        user.total_file_size = (user.total_file_size or 0) + file_size

    # 3. 将用户对象和链接器对象添加到会话
    async_session.add(new_linker)
    async_session.add(user)

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
        2026.3.3 Neo checked
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
    注意:调用此函数前,应先完成上传预检校验(配额/参数/去重)

    处理逻辑:
    1. 查询相同哈希的文件记录
    2. 跳过用户已拥有的文件

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

    权限判断逻辑:
    1. 管理员拥有所有文件的权限
    2. 普通用户需要满足:
       - UserFileLink 含有该用户与该文件的链接

    Args:
        file_record: 文件记录对象(FileInDB)
        current_user: 当前用户对象(UserInDB)

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
    zip_size = None
    zip_path = None
    zip_download_url = None
    fc_request_id = None
    should_pack = False

    oss_packup_key = f"oss_packup-{file_record.filename}"
    oss_packup_lock_key = f"oss_packup_lock-{file_record.filename}"

    try:
        # 先检查redis是否存在正在进行或已完成打包流程
        packup = await redis_client.hgetall(oss_packup_key) # type: ignore

        # 没有打包流程
        if not packup:
            # 检查源ZIP文件是否存在，不存在则进入常规打包流程
            if not await OssStorage.check_file_exist(file_record.path):
                should_pack = True
                # 检查是否有其他进程正在打包
                lock = await redis_client.get(oss_packup_lock_key)
                if lock:
                    logger.warning("packup is in progress by other user, now wait for it to finish, filename=%s", file_record.filename)
                    zip_download_url = "<PACKING>"
                else:
                    # 加锁，防止其他进程同时打包
                    await redis_client.set(oss_packup_lock_key, "1", ex=10)
            else:
                # 源ZIP文件存在，直接返回下载URL
                zip_download_url = await OssStorage.sign_download_url(file_record.path, expires=DOWNLOAD_ZIP_TTL_MINUTES * 60)

                # 写入redis，标记为已完成
                await redis_client.hset(oss_packup_key, mapping={
                    "status": PackupStatus.COMPLETED,
                    "file_path": file_record.path,
                    "download_url": zip_download_url}) # type: ignore

        elif packup.get("status") == PackupStatus.PACKING: # 正在打包，等待打包完成
            logger.info("packup is in progress, file_id=%s", file_record.file_id)
            zip_download_url = "<PACKING>"

        elif packup.get("status") == PackupStatus.COMPLETED:  # 打包完成，返回打包后的文件路径
            zip_path = packup.get("file_path")
            fc_request_id = packup.get("fc_request_id")
            zip_download_url = packup.get("download_url")
            if not zip_path:
                logger.warning("packup completed but not file_path, now start new packup, filename=%s", file_record.filename) # filename
                should_pack = True
            elif not await OssStorage.check_file_exist(zip_path):
                logger.warning(f"packup completed but zip has been cleaned up, now start new packup, filename={file_record.filename}")
                should_pack = True
            elif zip_download_url is not None:
                # 直接返回已生成的下载URL
                logger.info("packup has completed and signed, return download_url directly, filename=%s", file_record.filename)
            else:
                zip_size = await OssStorage.get_file_size(zip_path)
                if zip_size is None:
                    raise file_content_missing_exception

                # 生成下载URL
                zip_download_url = await OssStorage.sign_download_url(zip_path, expires=DOWNLOAD_ZIP_TTL_MINUTES * 60)

                # 将生成的URL写入redis
                await redis_client.hset(oss_packup_key, mapping={"download_url": zip_download_url}) # type: ignore

                # 后台获取fc打包日志（只触发不等待）
                if fc_request_id is not None:
                    get_fc_invoke_logs_by_request_id_in_background.delay(fc_request_id) # type: ignore

                # 更新文件记录
                file_record.path = zip_path
                file_record.size = zip_size# type: ignore

                logger.info(
                    "prepared download zip file_id=%s path=%s size=%s",
                    file_record.file_id,
                    zip_path,
                    file_record.size,
                )

        elif packup.get("status") == PackupStatus.FAILED:  # 打包失败，返回错误信息
            logger.error("packup failed, filename=%s", file_record.filename)
            fc_request_id = packup.get("fc_request_id")
            await redis_client.delete(oss_packup_key) # 删除失败的打包记录
            # 后台获取fc打包日志（只触发不等待）
            if fc_request_id is not None:
                get_fc_invoke_logs_by_request_id_in_background.delay(fc_request_id) # type: ignore
            raise file_content_missing_exception

        else:
            logger.warning("invalid packup status in redis, now start new packup, filename=%s", file_record.filename) # filename
            should_pack = True

        # 常规打包流程
        if should_pack:
            # 处理 origin 文件(imzML) # TODO： 封装函数
            if file_record.experiment_type == "imzML":
                assert file_record.file_id is not None
                origin = await get_origin_imzml_by_file_id(session, file_record.file_id)

                if not origin or origin.status not in [ExperimentStatus.COMPLETED, ExperimentStatus.PROCESSING]:
                    logger.warning(
                        "origin not found or not completed file_id=%s", file_record.file_id
                    )
                    raise file_content_missing_exception

                # 获取 origin 文件路径并校验是否存在
                imzml_path = origin.imzml_path if origin.imzml_path else None
                ibd_path = origin.ibd_path if origin.ibd_path else None
                if (
                    not imzml_path or not ibd_path or
                    not await OssStorage.check_file_exist(imzml_path) or
                    not await OssStorage.check_file_exist(ibd_path)
                ):
                    logger.warning("origin files missing file_id=%s", file_record.file_id)
                    raise file_content_missing_exception

                origin_path = f"{Path(imzml_path).parent}/"

                # 异步触发FC文件打包（只触发不等待，返回函数调用ID，打包完成会由FC端调用回调通知）
                fc_request_id = await packup_oss_file(
                    filename=file_record.filename,
                    packup_folder = origin_path, # type: ignore # 理论上origin_path存在，直接用即可
                    zip_file_path = f"{OSS_PACKUP_ZIP_STORAGE_ROOT.strip('/')}/{Path(file_record.filename).stem}.zip", # type: ignore
                )

                if fc_request_id is None:
                    logger.error("packup_oss_file trigger failed, filename=%s", file_record.filename)
                    raise internal_server_error_exception

                # Redis 标记正在打包状态
                await redis_client.hset(oss_packup_key, mapping={"status": PackupStatus.PACKING, "fc_request_id": fc_request_id})  # type: ignore
                await redis_client.expire(oss_packup_key, DOWNLOAD_ZIP_TTL_MINUTES * 60 or 360)

                zip_download_url = "<PACKING>"

        if not zip_download_url:
            logger.error("zip_download_url is None, filename=%s", file_record.filename)
            raise file_content_missing_exception

        # 只在不需要打包 / 打包已完成 / 打包失败时删锁，正在打包时保留单请求锁
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
    验证简单上传的所有条件：
    包含：名字、存储类型
    确保上传的文件信息与预检记录一致

    Raises:
        HTTPException: 验证失败时抛出异常
    2026.3.3 Neo checked
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
    调用时机：文件落盘后
    ### Args:
        - expected_hash: 预期的哈希值
        - expected_size: 预期的文件大小
        - file_path: 文件路径(本地或oss路径)
        - storage_type: 存储类型(local/oss)
    ### Returns:
        - act_hash: 实际的哈希值
        - act_size: 实际的文件大小
    ### Raises:
        - file_validation_failed_exception: 验证失败时抛出异常
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
async def update_download_stats(
    session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
) -> None:
    """
    更新下载统计

    """
    now = utc_time()
    file_record.download_count += 1
    file_record.last_downloaded_at = now
    current_user.download_used = (current_user.download_used or 0) + 1
    session.add(current_user)
    session.add(file_record)


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

    删除流程:
    1. 查询文件记录,同时预加载对应的分片记录
    2. 删除分片物理文件(数据库记录会在删除文件时自动级联删除)
    3. 删除文件的物理文件
    4. 从所有关联用户的 file_ids 中移除该文件ID
    5. 更新所有关联用户的统计信息(file_count、total_file_size)
    6. 删除文件数据库记录(数据库会自动级联删除分片记录)
    Raises:
        ValueError: 文件不存在
        PermissionError: 用户无权限删除该文件
        OSError: 物理文件删除失败时

        2026.3.3 Neo checked
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


async def unlink_user_from_file( #TODO 风险，后面加锁
    session: AsyncSession,
    user: UserInDB,
    file_record: FileInDB,
) -> bool:
    """
    解除用户与文件的关联

    Returns:
        True 表示文件还有其他用户持有(只解除关联)
        False 表示文件没有其他用户持有(需要完全删除)
        2026.3.3 Neo checked
    """
    # 1. 更新当前用户的文件关联和统计信息
    assert file_record.file_id is not None
    await remove_file_link(session, user, file_record)
    session.add(file_record)

    # 2. 更新统计信息
    user.file_count = max(0, (user.file_count or 0) - 1)
    user.total_file_size = max(0, (user.total_file_size or 0) - file_record.size)
    session.add(user)

    # 3. 刷新事务
    await session.flush()
    await session.refresh(file_record)
    # 4. 返回是否还有其他用户持有该文件
    owner_counts = (await session.exec(
        select(func.count()).where(UserFileLink.file_id == file_record.file_id) # pylint: disable=not-callable
    )).one()

    return bool(owner_counts)


async def safe_cleanup_preflight_record(
    async_session: AsyncSession, current_user: UserInDB, pre_record: FileInDB
) -> None:
    """
    安全清理预检记录

    处理逻辑:
    - 使用 unlink_user_from_file 解除用户关联并更新统计信息
    - 如果文件属于多个用户:只移除当前用户的关联,保留记录
    - 如果文件只属于当前用户:删除整条记录
        2026.3.3 Neo checked
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

    处理逻辑:
    1. 回滚事务,撤销数据库中的预检记录和关联关系
    2. 删除已写入的物理文件(如果有)
    3. 开启新事务，安全清理预检记录和用户关联
    4. 提交事务,使清理生效

        2026.3.3 Neo checked
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


async def save_and_persist_part(
    async_session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
    part_data: PartCreate,
    part: UploadFile,
    temp_path: Path,
) -> PartInDB:
    """
    保存分片临时文件并创建数据库记录

    流程:
    1. 保存分片到临时目录
    2. 创建 PartInDB 记录
    3. flush + refresh

    Returns:
        持久化后的 PartInDB 记录

    Raises:
        file_persist_failed_exception: 文件保存失败
    """
    await save_upload_file(part, temp_path)

    part_record = PartInDB(
        **part_data.model_dump(),
        temp_path=str(temp_path),
        owner_id=current_user.id, #type:ignore 使用的时候需要提前判断
        file_id=file_record.file_id, #type:ignore 使用的时候需要提前判断
        total_file_size=file_record.size,
        total_parts=file_record.total_parts, #type:ignore 使用的时候需要提前判断
    )
    async_session.add(part_record)
    await async_session.flush()

    return part_record


async def merge_parts_to_file(
    async_session: AsyncSession,
    file_record: FileInDB,
    parts: list[PartInDB],
    current_user: UserInDB,
) -> None:
    """
    执行分片合并,将所有分片写入最终文件

    使用 helper 层的 merge_files_with_hash 函数进行合并和哈希验证 
    如果哈希不匹配,删除错误文件和所有相关记录

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
        # if final_path:
            # LocalStorage.delete_file(final_path)
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

    2026.3.3 Neo checked
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

    每次上传分片后调用,检查是否所有分片都已上传完成
    如果完成则执行合并操作

    合并流程:
    1. 查询该文件的所有分片记录
    2. 检查分片完整性(数量和序列)
    3. 如果分片不完整,返回 False 等待更多分片
    4. 如果所有分片已上传,执行分片合并
    5. 合并成功后清理分片临时文件

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

    删除流程:
    1. 查询该文件的所有分片记录
    2. 删除每个分片的物理文件(调用 LocalStorage.delete_file)
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

    Args:
        session: 数据库会话
        current_user: 当前用户
        file_data: 上传预检请求模型

    Returns:
        True 表示验证通过,False 表示验证失败

        2026.3.3 Neo checked
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
