#########################
# File API - API入口层
#########################
import json
from typing import Annotated, Any
from fastapi import Depends, File, Form, UploadFile
from sqlmodel import select, and_, or_, col

from background.experiment_background import trigger_experiment_extraction
from storage_backend.storage import LocalStorage, OssStorage
from api.file_api_business import (
    assert_file_owner,
    delete_file_and_parts,
    get_file_record_by_hash,
    get_user_file_ids,
    link_file_to_user,
    prepare_download_file,
    rollback_simple_upload,
    get_reuse_file_by_hash,
    save_and_persist_part,
    unlink_user_from_file,
    update_download_stats,
    validate_and_merge_parts,
    validate_part_upload,
    validate_simple_upload,
    rollback_part_upload,
    validate_upload_request,
    validate_hash_and_size,
)
from api.file_api_helper import (
    get_file_record_by_id,
    save_upload_file,
    prepare_part_temp_path
)
from api.experiment_api_business import delete_experiment_files, valid_file_uploaded
from api.user_api import token_authen
from api.user_api_helper import count_items, get_user_by_id
from constants.file_constants import MAX_DOWNLOADS_PER_USER, ALLOWED_STORAGE_TYPES
from database.db import AsyncSessionDep
from exceptions.file_exceptions import (
    file_persist_failed_exception,
    invalid_part_data_exception,
    invalid_request_parameters_exception,
    permission_denied_exception,
    user_download_limit_exception,
)
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.user_exceptions import user_not_found_exception
from models.file_models import (
    PartUploadStatus,
    FileCreate,
    FileDownloadMeta,
    FileInDB,
    FilePublic,
    PartCreate,
    PartInDB,
    PartPublic,
)
from models.user_models import UserInDB
from models.link_models import UserFileLink
from models.filter_models import ListDataResponse, FileFilter, PaginationMeta
from tools import utc_time,get_logger

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
    # 只有全局配置允许local存储时，才创建本地存储目录
    if "local" in ALLOWED_STORAGE_TYPES:
        LocalStorage.ensure_storage_dirs()

    # 参数验证
    if not await validate_upload_request(async_session, current_user, file_data):
        logger.error(f"validation failed user={current_user.id} filename={file_data.filename}")
        raise invalid_request_parameters_exception

    # 尝试复用同哈希文件,否则创建新记录
    assert file_data.file_verify_code is not None
    reused_file = await get_reuse_file_by_hash(async_session, current_user, file_data.file_verify_code)

    if reused_file:
        file_record = reused_file
    else:
        file_record = FileInDB(
            **file_data.model_dump(),
            path="",  # 预检时设置为空字符串,上传成功后再更新
            first_uploaded_by=current_user.username,
        )
        async_session.add(file_record)
        await async_session.flush()  # 获取 file_id

    # 统一建立用户关联
    await link_file_to_user(async_session, current_user, file_record, file_data.size)
    await async_session.commit()
    await async_session.refresh(file_record)
    await async_session.refresh(current_user)

    logger.info(
        f"preflight start user={current_user.id} filename={file_data.filename} file_id={file_record.file_id}\r\n"
        f"storage={file_data.storage_type} size={file_data.size} parts={file_data.total_parts} experiment_type={file_data.experiment_type}")

    return file_record # type: ignore


async def upload_file_part(
    part_data_json: Annotated[str, Form(alias="part_data_json")],
    part: Annotated[UploadFile, File()],
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> PartPublic:
    """
    【分片上传接口】

    功能:上传单个文件分片,每次上传后验证并尝试合并
    时机:在预检接口之后,逐个上传分片

    请求格式:multipart/form-data
    - part_data_json: JSON 字符串,包含分片元数据,对应模型为 PartCreate
    - part: 分片文件内容

    流程:
    1. 解析分片元数据
    2. 查询文件记录
    3. 验证分片(编号范围、重复、哈希大小)
    4. 保存分片并创建记录
    5. 尝试合并,合并成功则触发后台任务
    """
    part_record = None
    file_record = None
    temp_path = None
    # 1. 解析分片元数据
    try:
        # 只有全局配置允许local存储时，才创建本地存储目录
        if "local" in ALLOWED_STORAGE_TYPES:
            LocalStorage.ensure_storage_dirs()

        part_data = PartCreate(**json.loads(part_data_json))
        logger.info(f"part upload start user={current_user.id} file_hash={part_data.file_hash} part={part_data.part_number}")

        # 2. 查询文件记录(业务层)
        file_record = await get_file_record_by_hash(async_session, part_data.file_hash)

        # 3. 统一验证(业务层:编号范围 + 重复)
        await validate_part_upload(async_session, file_record, current_user, part_data)

        # 4 获取保存路径
        if not file_record.file_verify_code or not current_user.id or not file_record.file_id:
            raise invalid_request_parameters_exception

        temp_path = prepare_part_temp_path(current_user.id, file_record.file_verify_code, part_data.part_number)

        # 4. 保存分片、校验哈希并持久化记录(业务层)
        part_record = await save_and_persist_part(async_session, file_record, current_user, part_data, part, temp_path)
        _, act_part_size = await validate_hash_and_size(part_data.part_hash, part_data.part_size, str(temp_path), "local")

        logger.info(f"part saved user={current_user.id} file_id={file_record.file_id} "
                    f"part={part_data.part_number} size={act_part_size}")

        # 5. 尝试合并
        is_merged = await validate_and_merge_parts(async_session, file_record, current_user)
        if is_merged:
            await trigger_experiment_extraction(file_record, current_user, file_record.experiment_type)

        await async_session.commit()
        await async_session.refresh(current_user)
        await async_session.refresh(file_record)
        if not is_merged:
            await async_session.refresh(part_record)
        logger.info(f"part upload completed user={current_user.id} file_id={file_record.file_id}")

    except Exception as exc:
        await rollback_part_upload(async_session, temp_path, file_record)
        raise invalid_part_data_exception from exc

    return part_record  # type: ignore


async def get_part_upload_status(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> PartUploadStatus:
    """
    【分片上传状态查询接口】
    
    功能:查询分片上传的进度和状态
    时机:前端可以定期调用此接口查询上传进度
    
    返回信息:
    - file_hash: 文件哈希
    - filename: 文件名
    - total_parts: 总分片数
    - uploaded_parts: 已上传的分片编号列表
    - total_size: 文件总大小
    - is_complete: 是否已完成上传
    
    判断逻辑:
    - 如果文件ID在用户的 file_ids 中,说明已完成上传
    - 否则查询已上传的分片记录
    """
    file_record = await get_file_record_by_id(file_id, session)

    # 创建基础状态对象
    status = PartUploadStatus(
        file_hash=file_record.file_verify_code or "",
        filename=file_record.filename,
        total_parts=file_record.total_parts or 0,
        uploaded_parts=[],
        total_size=file_record.size,
        is_complete=False,
    )

    # 检查文件是否已完成上传(复用 valid_file_uploaded,对 imzML 类型额外验证实验状态)
    assert current_user.id is not None
    user_file_ids = await get_user_file_ids(session, current_user.id)
    if file_record.file_id in user_file_ids and await valid_file_uploaded(session, file_record):
        status.is_complete = True
        return status

    # 查询已上传的分片
    uploaded_parts = (
        await session.exec(
            select(PartInDB)
            .where(and_(PartInDB.file_id == file_id, PartInDB.owner_id == current_user.id))
            .order_by(PartInDB.part_number) # type: ignore
        )
    ).all()

    status.uploaded_parts = [part.part_number for part in uploaded_parts]
    return status


async def upload_simple_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    storage_type: str = Form("local"),
    pre_file_id: int = Form(...),
    file: UploadFile = File(...),
) -> FilePublic:
    """
    【简单上传接口】
    
    功能:一次性接收完整文件,不做分片
    时机:在预检接口之后调用
    
    流程:
    1. 查询预检记录
    2. 统一验证(存储类型、文件名、元数据、配额)
    3. 保存文件到存储目录
    4. 更新用户统计信息(file_ids、file_count、total_file_size)
    5. 如果是 ZIP 文件,触发后台任务提取实验文件
    
    后台任务:
    - 如果是 ZIP 文件,上传成功后会自动触发实验文件提取
    
    注意:
    - 必须先调用预检接口获取 pre_file_id
    - 文件哈希和大小必须与预检记录匹配,否则上传失败
    - 上传失败时会清理预检记录
    """
    logger.info(f"simple upload start user={current_user.id}, filename={file.filename},storage={storage_type}")

    pre_record = None
    final_path = None
    oss_path = None
    oss_sts_token = None

    try:
        # 只有全局配置允许local存储时，才创建本地存储目录
        if "local" in ALLOWED_STORAGE_TYPES:
            LocalStorage.ensure_storage_dirs()

        # 查询预检记录
        pre_record = await get_file_record_by_id(pre_file_id, async_session)

        # 统一验证(业务层)
        await validate_simple_upload(current_user, file, pre_record, storage_type)

        # 根据存储类型动态处理不同的存储逻辑
        assert file.filename is not None # 已经在validate_simple_upload 里面校验了
        assert pre_record.file_verify_code is not None # 已经在validate_simple_upload 里面校验了
        if storage_type == "local":
            # 如果是本地存储，保存文件并更新最终文件记录
            final_path = LocalStorage.prepare_storage_path(file.filename)
            await save_upload_file(file, final_path) # 保存文件 → 更新记录 → 提交,统一 try 保护

            # 校验文件哈希和大小
            assert pre_record.file_verify_code is not None
            _, act_size = await validate_hash_and_size(pre_record.file_verify_code, pre_record.size, str(final_path), storage_type)

            # 更新预检记录的路径和大小(预检时已建立用户关联)
            pre_record.path = str(final_path)
            pre_record.size = act_size
            pre_record.uploaded_at = utc_time()
            pre_record.last_downloaded_at = pre_record.uploaded_at
            async_session.add(pre_record)

            await async_session.commit()
            await async_session.refresh(pre_record)
            await async_session.refresh(current_user)
            logger.info(f"simple upload completed user={current_user.id} user name = {current_user.username}\n"
                        f"file_id={pre_record.file_id} filename={pre_record.filename} size={pre_record.size}")

        elif storage_type == "oss":
            # 如果是oss存储，向前端返回带oss_upload_path的URL，前端需要根据URL上传文件到oss
            # 前端上传完毕后，通知后端oss_upload_complete接口,更新文件记录并触发实验文件提取任务(或通过oss回调进行通知)
            oss_path = OssStorage.prepare_storage_path(file.filename) # oss 存储路径(如)
            oss_sts_token = await OssStorage.get_upload_sts_token(file_path = oss_path)
            if not oss_sts_token or not oss_path:
                raise internal_server_error_exception

    except Exception as exc:
        logger.error(f"simple upload failed user={current_user.id} filename={file.filename} error={exc}")
        await rollback_simple_upload(async_session, current_user, pre_record, final_path) #TODO 可能会传入空
        raise file_persist_failed_exception from exc

    # 如果是本地存储，在此处触发实验文件提取任务
    if storage_type == "local":
        await trigger_experiment_extraction(pre_record, current_user, pre_record.experiment_type)

    # 如果是oss存储，在此处将pre_record显式转换为FilePublic模型，并设置oss_path和oss_sts_token（仅供返回前端）
    if oss_path and oss_sts_token:
        pre_record = FilePublic.model_validate(pre_record)
        pre_record.oss_path = oss_path
        pre_record.oss_sts_token = oss_sts_token

    return pre_record # type: ignore


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
        data = (await session.exec(file_query.offset(offset).limit(size))).all() or []

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
       - 删除实验相关文件(如果有,如 imzML、ibd)
       - 删除文件物理文件
       - 删除所有分片记录和物理文件
       - 删除文件数据库记录
       - 更新当前用户的统计信息
       - 返回状态:deleted
    
    判断逻辑:
    - 通过 file_record.owners 列表判断是否有其他用户持有该文件
    - 移除当前用户后,如果 owners 为空,则执行完全删除

    """
    file_record = await get_file_record_by_id(file_id, session)
    if not await assert_file_owner(session, file_record, current_user):
        raise permission_denied_exception

    # 解除用户与文件的关联(业务层)
    has_other_owners = await unlink_user_from_file(session, current_user, file_record)

    if has_other_owners:
        # 还有其他用户持有,只解除关联
        await session.commit()
        await session.refresh(current_user)
        await session.refresh(file_record)
        logger.info(f"unlinked file reference only user={current_user.id} file_id={file_id}")
        return {"file_id": file_id, "status": "unlinked"}

    # 没有其他用户持有该文件,执行完全删除
    # 删除实验相关文件(如果有)
    if file_record.experiment_type == "imzML":
        await delete_experiment_files(session, file_id)

    # 删除文件、分片和数据库记录
    await delete_file_and_parts(session, file_id, current_user)

    await session.commit()
    await session.refresh(current_user)
    logger.info(f"deleted file completely user={current_user.id} file_id={file_id}")

    return {"file_id": file_id, "status": "deleted"}


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
    1. 权限验证(用户是否拥有该文件)
    2. 下载次数限制检查(非管理员用户)
    3. 准备下载文件(处理实验文件ZIP打包)
    4. 更新下载统计(文件下载次数、用户下载次数)
    
    实验文件处理:
    - 如果原始文件不存在,但实验文件(imzML、ibd)存在
    - 动态创建 ZIP 包,包含 imzML 和 ibd 文件
    - ZIP 包保存在临时目录,供下载使用
    
    下载限制:
    - 管理员:无限制
    - 普通用户:受 MAX_DOWNLOADS_PER_USER 限制
    
    注意:
    - 此接口只返回元数据,不返回文件流
    - 实际文件下载由前端或其他接口处理
    - 实验文件的 ZIP 包是临时生成的,需要定期清理
    """

    file_record = await get_file_record_by_id(file_id, session)
    if not await assert_file_owner(session, file_record, current_user):
        raise permission_denied_exception

    logger.debug(f"download request user={current_user.id} file_id={file_id}")

    # 下载次数限制检查
    if current_user.identity != "admin":
        used = current_user.download_used or 0
        if used >= MAX_DOWNLOADS_PER_USER:
            raise user_download_limit_exception

    # 准备下载文件(业务层处理)
    await prepare_download_file(session, file_record)

    # 更新下载统计(业务层处理)
    await update_download_stats(session, file_record, current_user)
    await session.commit()
    await session.refresh(current_user)
    await session.refresh(file_record)

    logger.info(f"download success user={current_user.id} file_id={file_id} filename={file_record.filename}")

    return FileDownloadMeta.from_record(file_record)


async def get_file_list(
    page: int,
    size: int,
    filters: FileFilter,
    async_session: AsyncSessionDep
) -> ListDataResponse:
    """
    【公共文件列表接口】

    功能:根据查询参数返回文件列表
    时机:在前端查询文件列表时调用

    流程:
    1. 分页查询文件列表(根据查询参数)
    2. 返回文件列表和分页元数据

    注意:
    - 分页参数(page, size)默认值为 1, 10
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
                FileInDB.is_public == True, # pylint: disable=singleton-comparison

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

        data = (await async_session.exec(file_query.offset(offset).limit(size))).all() or None

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


async def oss_upload_complete(
    file_id: int,
    current_user_id: int,
    fc_request_id: str,
    async_session: AsyncSessionDep
):
    """OSS上传完成时，前端调用该接口通知后端，更新文件状态为已完成上传"""
    # TODO: 需确认是通过前端调用还是oss上传完成的callback调用？
    # 如为前端调用，应有token依赖注入验证用户身份；如由callback调用，则直接传user_id即可），但需要限制该接口只能由aliyun oss调用
    pre_record = None
    current_user = None
    oss_path = None
    try:
        # 查询预检记录
        pre_record = await get_file_record_by_id(file_id, async_session)
        current_user = await get_user_by_id(current_user_id, async_session)
        if pre_record is None:
            raise FileNotFoundError(f"preflight_file_id={file_id} not found")
        if current_user is None:
            raise user_not_found_exception

        # 构建OSS文件路径
        oss_path = OssStorage.prepare_storage_path(pre_record.filename)

        # celery任务验证oss文件是否存在
        is_file_exist = await OssStorage.validate_file(oss_path, pre_record.size)
        if not is_file_exist:
            oss_path = None
            raise FileNotFoundError(f"file_id={file_id}, oss_file_name={pre_record.filename} not found")

        # 更新文件路径
        pre_record.path = oss_path
        await async_session.commit()
        await async_session.refresh(pre_record)

    except Exception as e:
        logger.error(f"oss_upload_complete failed: {e}")
        if current_user is not None:
            await rollback_simple_upload(async_session, current_user, pre_record, final_path=oss_path)
        raise internal_server_error_exception from e

    # 触发实验文件提取任务(传入fc_request_id，用于关联oss端自动提取结果)
    await trigger_experiment_extraction(pre_record, current_user, pre_record.experiment_type, fc_request_id)
    return pre_record


# 依赖注入类型别名
DeleteFileDep = Annotated[dict[str, Any], Depends(delete_user_file)]
SimpleUploadDep = Annotated[FilePublic, Depends(upload_simple_file)]
ListFilesDep = Annotated[ListDataResponse, Depends(list_user_files)]
DownloadFileDep = Annotated[FileDownloadMeta, Depends(get_download_file_metadata)]
PreflightDep = Annotated[FilePublic, Depends(preflight_create_file)]
PartUploadDep = Annotated[PartPublic, Depends(upload_file_part)]
PartStatusDep = Annotated[PartUploadStatus, Depends(get_part_upload_status)]
ListPublicFilesDep = Annotated[ListDataResponse, Depends(get_file_list)]
OssUploadCompleteDep = Annotated[FilePublic, Depends(oss_upload_complete)]
