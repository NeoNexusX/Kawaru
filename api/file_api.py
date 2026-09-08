#########################
# File API - API入口层
#########################
from typing import Annotated, Any

from fastapi import Depends, Form, Query, HTTPException
from sqlmodel import select, and_, or_, col

from api.user_api import token_authen
from api_helpers.file_helper import check_user_quota
from api_services.composite_service import mark_file_for_deletion
from api_services.file_service import (
    assert_file_owner,
    get_user_file_ids,
    link_file_to_user,
    prepare_download_file_oss,
    prepare_download_raw_oss,
    rollback_simple_upload,
    get_reuse_file_by_hash,
    set_file_public_service,
    update_download_stats,
    update_file_metadata,
    validate_simple_upload,
    validate_upload_request,
    get_file_record_by_id,
    update_file_record,
)
from background.file_background import delete_objects_task
from api_services.user_service import count_items
from constants.file_constants import ALLOWED_STORAGE_TYPES, OSS_BUCKET_NAME, OSS_REGION_ID, PACKUP_CACHE_TTL_SECONDS
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from database.db import AsyncSessionDep
from database.redis_client import redis_client
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.file_exceptions import (
    file_not_found_exception,
    file_persist_failed_exception,
    invalid_request_parameters_exception,
    permission_denied_exception,
    user_download_limit_exception,
    user_file_limit_exception,
    download_not_implemented_exception,
)
from models.file_models import (
    FileCreate,
    FileDownloadMeta,
    FileInDB,
    FilePatch,
    FilePublic,
    FileOssUploadResponse,
    FileStatus,
    RawDownloadResponse,
)
from models.filter_models import ListDataResponse, FileFilter, PaginationMeta
from models.link_models import UserFileLink
from models.user_models import UserInDB
from storage.oss.oss_storage import OssStorage
from tools import get_logger

logger = get_logger("api.file_api")


async def preflight_create_file(
    file_data: FileCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> FilePublic:
    """
    【预检接口】

    功能:根据前端提供的元数据创建文件记录,但不写入用户 file_ids 与计数。
    时机:在实际文件上传之前调用
    目的:提前验证、预分配文件ID、支持秒传(哈希复用)

    流程:
    1. 参数验证(存储类型、文件名、文件类型、用户配额检查)
    2. 重复文件检查(文件名、哈希)
    3. 尝试复用已存在的相同哈希md5文件(秒传)
    4. 如果无法复用,创建新的文件记录(预检状态)

    注意:
    - 此时文件记录已创建,但用户的 file_ids 和统计信息尚未更新
    - 只有在后续上传接口中校验通过后,才会更新用户计数
    """
    # 参数验证
    if not await validate_upload_request(async_session, current_user, file_data):
        logger.error(f"validation failed user={current_user.id} filename={file_data.filename}")
        raise invalid_request_parameters_exception

    # 尝试复用同哈希文件,否则创建新记录
    assert file_data.file_verify_code is not None

    reused_file = await get_reuse_file_by_hash(async_session, current_user, file_data.file_verify_code) # type: ignore

    # 复用前确认 OSS 对象真实存在（对象可能已被清理任务删除）。
    # 对象丢失时降级为正常新上传，不产生"幽灵文件"秒传。
    # path 为哨兵时跳过检查：zip 虽已被定时清理，但 origin（imzML/ibd）仍在
    # （COMPLETED 蕴含 origin 完整，origin 不存在随 file 记录独立删除的路径），
    # 下载侧可从 origin 重新打包，不影响复用。
    if (reused_file and reused_file.path != SOURCE_FILE_PATH_CLEARED
            and not await OssStorage.check_file_exist(reused_file.path)):
        logger.warning(
            "reuse target object missing, fallback to new upload file_id=%s path=%s",
            reused_file.file_id, reused_file.path,
        )
        reused_file = None

    is_reuse = False
    if reused_file:
        # 秒传按被复用记录的实际 size 计费和核对配额（该 size 已经回调核对，可信），
        # 不能用客户端本次声明值——否则声明 1 字节即可秒传大文件绕过配额
        if not check_user_quota(current_user, reused_file.size):
            logger.error(
                "reuse quota exceeded user=%s file_id=%s size=%s",
                current_user.id, reused_file.file_id, reused_file.size,
            )
            raise user_file_limit_exception
        file_record = reused_file
        is_reuse = True
    else:
        file_record = FileInDB(
             **file_data.model_dump(),
            path="",  # 预检时设置为空字符串,上传成功后再更新
            first_uploaded_by=current_user.username,
        )
        async_session.add(file_record)
        await async_session.flush()  # 获取 file_id

    # 统一建立用户关联（秒传按记录实际 size 计费，新上传按声明值预占）
    billed_size = file_record.size if is_reuse else file_data.size
    await link_file_to_user(async_session, current_user, file_record, billed_size)
    await async_session.commit()
    await async_session.refresh(file_record)
    await async_session.refresh(current_user)


    logger.info(
        f"preflight start user={current_user.id} filename={file_data.filename} file_id={file_record.file_id}\r\n"
        f"storage={file_data.storage_type} size={file_data.size} parts={file_data.total_parts} experiment_type={file_data.experiment_type}")

    result = FilePublic.model_validate(file_record)
    result.is_reuse = is_reuse
    return result # type: ignore


async def upload_simple_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    storage_type: str = Form("oss"),
    pre_file_id: int = Form(...),
    filename: str | None = Form(None),
) -> FileOssUploadResponse:
    """
    【OSS 简单上传接口】

    查询预检记录并返回限定对象路径的 STS 临时凭证，文件内容由前端
    通过 OSS SDK 直传。必须先调用预检接口获取 pre_file_id。

    OSS 对象清理策略：
    - 回调在路径提交后失败时，失败实验定时任务会按已落库路径尝试删除
      source/imzML/ibd 对象。
    - 回调在路径提交前失败或 FC 完全没有回调时，数据库中没有可靠对象键；
      未写入 persistent=true 标签的对象由 OSS 生命周期规则回收。
    """

    pre_record = None

    try:
        if storage_type != "oss" or storage_type not in ALLOWED_STORAGE_TYPES:
            raise invalid_request_parameters_exception

        pre_record = await get_file_record_by_id(pre_file_id, async_session)
        if filename is None:
            raise HTTPException(status_code=400, detail="missing filename while storage_type is oss")

        logger.info(
            "oss upload start user=%s filename=%s storage=%s",
            current_user.id, filename, storage_type,
        )
        await validate_simple_upload(async_session, current_user, filename, pre_record, storage_type)

        # OSS 对象键使用唯一标识 data/{file_id}/{file_verify_code}.{file_type}，
        # 不再使用文件名；FC 解压函数从对象键解析 file_id 后回调后端
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
        await rollback_simple_upload(async_session, current_user, pre_record)
        raise file_persist_failed_exception from exc

    response = FileOssUploadResponse.model_validate(pre_record)
    response.oss_path = oss_path
    response.oss_sts_token = oss_sts_token
    response.oss_bucket = OSS_BUCKET_NAME
    response.oss_region_id = OSS_REGION_ID
    return response


async def list_user_files(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
    filters: FileFilter,
    page: int ,
    size: int
) -> ListDataResponse:
    """
    【个人文件列表查询接口】

    功能:查询当前用户的文件记录，支持分页和过滤
    返回:包含分页元数据和文件列表的JSON对象

    查询逻辑:
    - 从用户的 file_ids 列表中获取文件ID
    - 应用过滤器（文件名模糊匹配、实验类型等）
    - 分页查询文件记录

    注意:
    - 只返回用户拥有的文件，包括预检记录
    - 支持模糊查询文件名
    - 结果按 uploaded_at DESC, file_id DESC 排序，保证分页稳定
    """
    try:
        # 1. 获取用户文件ID列表
        assert current_user.id is not None
        user_file_ids = await get_user_file_ids(session=session, user_id=current_user.id)

        if not user_file_ids:
            pagination_meta = PaginationMeta(
                current_page=page,
                current_records=0,
                total_pages=0,
                total_records=0
            )
            return ListDataResponse(meta=pagination_meta, data=[])

        # 2. 构建查询语句
        file_query = select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))  # pylint: disable=no-member # type: ignore

        # 3. 应用过滤器
        file_query = file_query.filter(
            and_(
                col(FileInDB.filename).like(f"%{filters.filename}%") if filters.filename else True, # pylint: disable=no-member
                FileInDB.experiment_type == filters.experiment_type if filters.experiment_type else True,
                FileInDB.organism == filters.organism if filters.organism else True,
                FileInDB.organism_part == filters.organism_part if filters.organism_part else True,
                FileInDB.condition == filters.condition if filters.condition else True,
                FileInDB.sample_stabilization == filters.sample_stabilization if filters.sample_stabilization else True,
                FileInDB.tissue_modification == filters.tissue_modification if filters.tissue_modification else True,
                FileInDB.maldi_matrix == filters.maldi_matrix if filters.maldi_matrix else True,
                FileInDB.maldi_matrix_application == filters.maldi_matrix_application if filters.maldi_matrix_application else True,
                FileInDB.solvent == filters.solvent if filters.solvent else True,
            )
        )

        # 4. 计算总数
        total_records = await count_items(session, file_query, FileInDB)

        # 5. 计算总页数
        total_pages = (total_records + size - 1) // size

        # 6. 分页查询
        offset = (page - 1) * size
        data = (await session.exec(
            file_query
            .order_by(col(FileInDB.uploaded_at).desc(), col(FileInDB.file_id).desc()).offset(offset).limit(size))).all() or [] # pylint: disable=no-member

        # 7. 分页元数据
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(data),
            total_pages=total_pages,
            total_records=total_records
        )

        # 8. 构建响应
        list_file_response = ListDataResponse(
            meta=pagination_meta,
            data=[FilePublic(**file.model_dump()) for file in data]
        )

        logger.info(f"list user files user={current_user.id} page={page} size={size} total={total_records}")

        return list_file_response

    except Exception as e:
        logger.error(f"failed to list_user_files: {e}")
        raise internal_server_error_exception from e


async def delete_user_file(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> dict[str, Any]:
    """
    【文件删除接口】
    
    功能:删除当前用户的指定文件
    
    删除策略(多用户共享文件):
    1. 如果文件被多个用户共享:
       - 只解除当前用户与文件的关联
       - 更新当前用户的统计信息(file_count、total_file_size)
       - 从文件的 owners 列表中移除当前用户
       - 保留文件记录和物理文件
       - 返回状态:unlinked
    
    2. 如果文件只属于当前用户:
       - 删除 origin 相关文件(如果有,如 imzML、ibd)
       - 删除 OSS 源文件及关联的 packup 对象
       - 删除文件数据库记录
       - 更新当前用户的统计信息
       - 返回状态:deleted
    
    判断逻辑:
    - 通过 file_record.owners 列表判断是否有其他用户持有该文件
    - 移除当前用户后,如果 owners 为空,则执行完全删除

    删除状态位流程（OSS 先删、DB 后删、失败标记）:
    - 完全删除不再同步等待 OSS 删除完成，而是登记 DELETING 后立即返回
    - unlink + 扣配额（用户可见语义）在登记事务内完成并提交
    - worker 删完全部 OSS 对象后才删 DB 行（「要么都删除」）；任一失败则
      行置 DELETE_FAILED 保留（「要么都保留」），等重试（兜底任务待全部
      删除场景改造完成后统一上线，期间管理员重复删除即为手动重试）
    - 重复请求幂等:已处于 DELETING 的文件直接再次投递删除任务
    """
    # include_deleting=True: 对已处于 DELETING 的文件重复发起删除时走幂等分支而非 404
    file_record = await get_file_record_by_id(file_id, session, include_deleting=True)
    if not await assert_file_owner(session, file_record, current_user):
        raise permission_denied_exception

    # 登记删除:行锁抢占 → 置 DELETING → unlink + 扣配额 → 收集路径快照（同一事务）
    snapshot = await mark_file_for_deletion(session, current_user, file_record)

    if snapshot is None:
        # 还有其他用户持有,只解除关联
        await session.commit()
        await session.refresh(current_user)
        await session.refresh(file_record)
        logger.info(f"unlinked file reference only user={current_user.id} file_id={file_id}")
        return {"file_id": file_id, "status": "unlinked"}

    # 先 commit（配额已扣、关联已解除，用户可见语义立即生效），再投递删除任务。
    # 投递失败只留下 DELETING 行（管理员重复删除即可手动重投，兜底任务统一
    # 上线后自动收敛）;
    # 顺序不能反:先投后 commit 的崩溃会留下「对象已删、行未标记」的不可自愈形态
    await session.commit()
    delete_objects_task.delay( # type: ignore
        kind="file",
        resource_id=file_id,
        paths=list(snapshot.paths),
        redis_keys=list(snapshot.redis_keys),
    )

    await session.refresh(current_user)
    logger.info(f"marked file for deletion user={current_user.id} file_id={file_id}")

    return {"file_id": file_id, "status": "deleted"}


async def set_file_public(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict[str, Any]:
    """
    【文件公开设置接口】

    功能:将私有文件(is_public=False)改为公开(is_public=True)。
    权限:仅文件所有者或管理员可操作。

    流程:
    1. 查询文件记录
    2. 权限校验(文件所有者或管理员)
    3. 更新 is_public 字段为 True

    注意:
    - 操作是幂等的:重复调用不会产生副作用
    - 设为公开后,文件将出现在公共文件列表中
    """
    file_record = await get_file_record_by_id(file_id, async_session)
    if not await assert_file_owner(async_session, file_record, current_user):
        raise permission_denied_exception

    await set_file_public_service(async_session, file_record)
    await async_session.commit()
    await async_session.refresh(file_record)
    await async_session.refresh(current_user)

    logger.info(f"file set to public user={current_user.id} file_id={file_id}")
    return {"file_id": file_id, "is_public": file_record.is_public}


async def patch_file_metadata(
    file_id: int,
    patch: FilePatch,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> FilePublic:
    """
    【文件元数据修改接口】

    功能:部分更新文件元数据,只改请求中出现的字段
    可改:样本属性、spectrum_mode、storage_mode
    禁改:is_public(走 set_public 专用接口,公开不可逆)、experiment_type、filename 及系统字段

    权限:任一持有者即可修改(UserFileLink 存在记录即持有,admin 旁路),
    与现有 assert_file_owner 语义一致——共享文件的任一持有者修改,其余持有者可见

    流程:
    1. 查询文件记录
    2. 权限校验(持有者或管理员)
    3. 部分更新元数据

    注意:
    - spectrum_mode/storage_mode 非法取值由请求模型约束,直接返回 422
    - 请求中出现 is_public 等白名单外字段会被静默忽略(不报错、不生效)
    """
    file_record = await get_file_record_by_id(file_id, async_session)
    if not await assert_file_owner(async_session, file_record, current_user):
        raise permission_denied_exception

    await update_file_metadata(async_session, file_record, patch)
    await async_session.commit()
    await async_session.refresh(file_record)

    logger.info(f"patch file metadata user={current_user.id} file_id={file_id}")
    return FilePublic.model_validate(file_record)


async def get_download_file_metadata(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> FileDownloadMeta:
    """
    【文件下载元数据接口】
    
    功能:返回文件元数据和路径,供下载使用
    时机:在实际下载文件之前调用
    
    流程:
    1. 下载次数限制检查(非管理员用户)
    2. 准备下载文件(处理 origin 文件ZIP打包)
    3. 更新下载统计(文件下载次数、用户下载次数)

    origin 文件处理:
    - 如果原始文件不存在,但 origin 文件(imzML、ibd)存在
    - 动态创建 ZIP 包,包含 imzML 和 ibd 文件
    - ZIP 包保存在临时目录,供下载使用
    
    下载限制:
    - 管理员:无限制
    - 普通用户:受用户下载配额限制
    
    注意:
    - 此接口不做文件归属权限验证:公开页面允许任意登录用户下载文件,因此只校验登录态(token_authen)而不校验文件归属
    - 此接口只返回元数据,不返回文件流
    - 实际文件下载由前端或其他接口处理
    - 实验文件的 ZIP 包是临时生成的,需要定期清理
    """

    file_record = await get_file_record_by_id(file_id, session)
    logger.debug(f"download request user={current_user.id} file_id={file_id}")

    # 下载次数限制检查
    if current_user.identity != "admin":
        used = current_user.download_used or 0
        if used >= current_user.max_download_count:
            raise user_download_limit_exception

    if file_record.storage_type != "oss":
        raise download_not_implemented_exception

    oss_download_url = await prepare_download_file_oss(session, file_record)

    if oss_download_url != "<PACKING>":
        await update_download_stats(session, file_record, current_user)

    await session.commit()
    await session.refresh(current_user)
    await session.refresh(file_record)

    if oss_download_url == "<PACKING>":
        logger.info(f"download packup file in progress, user={current_user.id} file_id={file_id} filename={file_record.filename}")
    else:
        logger.info(f"download success user={current_user.id} file_id={file_id} filename={file_record.filename}")

    file_download_meta = FileDownloadMeta.from_record(file_record)
    file_download_meta.oss_download_url = oss_download_url

    return file_download_meta


async def get_raw_download_metadata(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> RawDownloadResponse:
    """
    【RAW 直链下载元数据接口（需鉴权）】

    功能：不打包，直接为 origin 的 imzML/ibd 各签一个预签名 URL 返回。
    时机：前端请求 RAW 直链下载时调用。

    需要登录态：校验用户下载配额，更新用户维度和文件维度下载次数。

    与旧下载接口的区别：
    - 旧接口 GET /files/{id}/download：走 FC ZIP 打包，可能返回 <PACKING>
    - 新接口 GET /files/{id}/download_raw：跳过打包，返回两个直链 URL

    流程：
    1. 下载次数限制检查（非管理员用户）
    2. 校验文件类型（仅支持 OSS + imzML）
    3. Redis 缓存命中 → 直接返回；未命中 → 校验 origin 就绪 → 各签 URL → 写入缓存
    4. 更新下载统计（文件维度 + 用户维度）

    Redis 缓存：TTL 取 PACKUP_CACHE_TTL_SECONDS，避免重复点击反复调 OSS 签名。

    下载限制：
    - 管理员：无限制
    - 普通用户：受用户下载配额限制

    注意：
    - 不走 ZIP 打包，不产生中间产物，与被清理任务无关
    - 不写 file_record.path，不会误删永久 origin 文件
    """
    file_record = await get_file_record_by_id(file_id, session)

    logger.debug(f"raw download request user={current_user.id} file_id={file_id}")

    # 下载次数限制检查
    if current_user.identity != "admin":
        used = current_user.download_used or 0
        if used >= current_user.max_download_count:
            raise user_download_limit_exception

    # 只支持 OSS + imzML
    if file_record.storage_type != "oss" or file_record.experiment_type != "imzML":
        raise download_not_implemented_exception

    # Redis 缓存检查
    cache_key = f"raw_dl:{file_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        response = RawDownloadResponse.model_validate_json(cached)

        # 缓存命中仍然计一次下载
        await update_download_stats(session, file_record, current_user)
        await session.commit()
        await session.refresh(current_user)
        await session.refresh(file_record)

        logger.info(
            f"raw download (cached) user={current_user.id} file_id={file_id} filename={file_record.filename}"
        )
        return response

    # 缓存未命中 → 签名
    files = await prepare_download_raw_oss(session, file_record)
    response = RawDownloadResponse(files=files)

    # 写入缓存
    await redis_client.set(cache_key, response.model_dump_json(), ex=PACKUP_CACHE_TTL_SECONDS)

    # 更新下载统计
    await update_download_stats(session, file_record, current_user)

    await session.commit()
    await session.refresh(current_user)
    await session.refresh(file_record)

    logger.info(
        f"raw download success user={current_user.id} file_id={file_id} filename={file_record.filename}"
    )

    return response


async def get_raw_download_metadata_noauth(
    file_id: int,
    session: AsyncSessionDep,
) -> RawDownloadResponse:
    """
    【RAW 直链下载元数据接口（免登录）】

    功能：不打包，直接为 origin 的 imzML/ibd 各签一个预签名 URL 返回。
    时机：免登录下载场景调用。

    公开接口，无需鉴权：任何人都可直接获取 imzML/ibd 直链下载 URL，因此不做登录态与
    用户下载配额校验，也不更新用户维度的下载次数（仅更新文件维度统计）。

    与鉴权版下载接口的区别：
    - 鉴权版 GET /files/{id}/download_raw：需要登录，校验用户配额
    - 免登录版 GET /files/{id}/download_raw_noauth：无需登录，无限制

    流程：
    1. 校验文件类型（仅支持 OSS + imzML）
    2. Redis 缓存命中 → 直接返回；未命中 → 校验 origin 就绪 → 各签 URL → 写入缓存
    3. 更新下载统计（仅文件维度）

    Redis 缓存：TTL 取 PACKUP_CACHE_TTL_SECONDS，避免重复点击反复调 OSS 签名。

    注意：
    - 不走 ZIP 打包，不产生中间产物，与被清理任务无关
    - 不写 file_record.path，不会误删永久 origin 文件
    """
    file_record = await get_file_record_by_id(file_id, session)

    logger.debug(f"raw download noauth request file_id={file_id}")

    # 免登录下载仅服务公开文件（404 不暴露存在性）。
    # 该接口是集合公开页下载的免登录入口，任意 file_id 均可触达且无登录态，
    # 必须以 is_public 为闸——否则私有文件可被匿名直链遍历下载
    if not file_record.is_public:
        logger.warning(f"raw download noauth denied, file not public file_id={file_id}")
        raise file_not_found_exception

    # 只支持 OSS + imzML
    if file_record.storage_type != "oss" or file_record.experiment_type != "imzML":
        raise download_not_implemented_exception

    # Redis 缓存检查
    cache_key = f"raw_dl:{file_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        response = RawDownloadResponse.model_validate_json(cached)

        # 缓存命中仍然计一次下载
        await update_download_stats(session, file_record)
        await session.commit()
        await session.refresh(file_record)

        logger.info(
            f"raw download noauth (cached) file_id={file_id} filename={file_record.filename}"
        )
        return response

    # 缓存未命中 → 签名
    files = await prepare_download_raw_oss(session, file_record)
    response = RawDownloadResponse(files=files)

    # 写入缓存
    await redis_client.set(cache_key, response.model_dump_json(), ex=PACKUP_CACHE_TTL_SECONDS)

    # 更新下载统计
    await update_download_stats(session, file_record)

    await session.commit()
    await session.refresh(file_record)

    logger.info(
        f"raw download noauth success file_id={file_id} filename={file_record.filename}"
    )

    return response


async def get_file_list(
    filters: FileFilter,
    async_session: AsyncSessionDep,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> ListDataResponse:
    """
    【公共文件列表接口】

    功能:根据查询参数返回文件列表
    时机:在前端查询文件列表时调用

    流程:
    1. 按上传时间倒序分页查询文件列表(根据查询参数)
    2. 返回文件列表和分页元数据

    注意:
    - 分页参数 page≥1、1≤size≤100，越界由 FastAPI 返回 422
    - 结果按 uploaded_at DESC, file_id DESC 排序，保证分页稳定
    - 查询参数(filters)默认值为空字典

    """
    try:
        # 首先针对username模糊匹配数据库中的用户id列表
        user_ids = []
        if filters.username:
            user_query = select(UserInDB.id).where(col(UserInDB.username).like(f"%{filters.username}%"))
            user_ids = (await async_session.exec(user_query)).all() or []

        # 构建select语句
        file_query = select(FileInDB)

        # 如果有username过滤,则内连接UserFileLink表,确保关联用户的文件会被列出来
        if user_ids:
            file_query = file_query.join(UserFileLink, FileInDB.file_id == UserFileLink.file_id) # type:ignore

        # 构建其余的筛选条件
        file_query = file_query.filter(
            and_(
                col(FileInDB.filename).like(f"%{filters.filename}%") if filters.filename else True, # pylint: disable=no-member
                FileInDB.experiment_type == filters.experiment_type if filters.experiment_type else True,
                FileInDB.organism == filters.organism if filters.organism else True,
                FileInDB.organism_part == filters.organism_part if filters.organism_part else True,
                FileInDB.condition == filters.condition if filters.condition else True,
                FileInDB.sample_stabilization == filters.sample_stabilization if filters.sample_stabilization else True,
                FileInDB.tissue_modification == filters.tissue_modification if filters.tissue_modification else True,
                FileInDB.maldi_matrix == filters.maldi_matrix if filters.maldi_matrix else True,
                FileInDB.maldi_matrix_application == filters.maldi_matrix_application if filters.maldi_matrix_application else True,
                FileInDB.solvent == filters.solvent if filters.solvent else True,
                FileInDB.is_public == True, # noqa: E712 # pylint: disable=singleton-comparison

                # 用户关联条件：没有username过滤或user_id在匹配列表中
                or_(
                    not filters.username,
                    col(UserFileLink.user_id).in_(user_ids) if user_ids else False # pylint: disable=no-member
                )
            )
        ).distinct()

        # 先根据filter条件查询文件数量
        total_records = await count_items(async_session, file_query, FileInDB, distinct_on_pk=True)

        # 计算总页数
        total_pages = (total_records + size - 1) // size

        # 分页查询文件列表
        offset = (page - 1) * size

        data = (await async_session.exec(
            file_query
            .order_by(col(FileInDB.uploaded_at).desc(), col(FileInDB.file_id).desc()) # pylint: disable=no-member
            .offset(offset)
            .limit(size)
        )).all() or None

        # 分页元数据
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(data) if data else 0,
            total_pages=total_pages,
            total_records=total_records
        )

        # 构建响应模型
        list_file_response = ListDataResponse(
            meta=pagination_meta,
            data=[FilePublic(**file.model_dump()) for file in data] if data else []
        )

        return list_file_response

    except Exception as e:
        logger.error(f"failed to get_file_list: {e}")
        raise internal_server_error_exception from e


async def get_file_metadata(
    file_id: int,
    async_session: AsyncSessionDep,
) -> FilePublic:
    """
    【文件元数据查询接口】

    功能:根据文件ID查询文件的元数据信息
    时机:在前端需要查看单个文件详情时调用

    流程:
    1. 根据 file_id 查询文件记录
    2. 返回文件元数据
    """
    try:
        file_record = await get_file_record_by_id(file_id, async_session)
        return FilePublic(**file_record.model_dump())

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed to get_file_metadata: {e}")
        raise internal_server_error_exception from e


async def oss_upload_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    pre_file_id: int = Form(...),
    filename: str = Form(...),
) -> FileOssUploadResponse:
    """OSS上传文件，底层调用upload_simple_file接口提高复用性"""
    storage_type = "oss"
    return await upload_simple_file(
        current_user,
        async_session,
        storage_type,
        pre_file_id,
        filename=filename,
    )


# 依赖注入类型别名
DeleteFileDep = Annotated[dict[str, Any], Depends(delete_user_file)]
SimpleUploadDep = Annotated[FileOssUploadResponse, Depends(upload_simple_file)]
ListFilesDep = Annotated[ListDataResponse, Depends(list_user_files)]
DownloadFileDep = Annotated[FileDownloadMeta, Depends(get_download_file_metadata)]
RawDownloadDep = Annotated[RawDownloadResponse, Depends(get_raw_download_metadata)]
RawDownloadNoauthDep = Annotated[RawDownloadResponse, Depends(get_raw_download_metadata_noauth)]
PreflightDep = Annotated[FilePublic, Depends(preflight_create_file)]
ListPublicFilesDep = Annotated[ListDataResponse, Depends(get_file_list)]
OssUploadDep = Annotated[FileOssUploadResponse, Depends(oss_upload_file)]
FileMetadataDep = Annotated[FilePublic, Depends(get_file_metadata)]
SetFilePublicDep = Annotated[dict[str, Any], Depends(set_file_public)]
PatchFileMetadataDep = Annotated[FilePublic, Depends(patch_file_metadata)]
