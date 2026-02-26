#########################
# File API - API入口层
#########################
import json
from typing import Annotated, Any
from fastapi import BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlmodel import select, and_
from api.file_api_business import (
    assert_file_owner,
    delete_experiment_files,
    delete_file_and_parts,
    get_file_record_by_id,
    get_user_file_ids,
    link_file_to_user,
    prepare_download_file,
    rollback_simple_upload,
    get_reuse_file_by_hash,
    unlink_user_from_file,
    update_download_stats,
    validate_and_merge_chunks,
    validate_chunk_metadata,
    validate_simple_upload,
)
from api.file_api_helper import (
    compute_upload_file_metadata,
    delete_local_file,
    ensure_storage_dirs,
    prepare_local_storage_path,
    prepare_temp_chunk_path,
    save_upload_file,
    trigger_experiment_extraction,
    validate_upload_request,
)
from api.experiment_api_business import is_file_uploaded
from api.user_api import token_authen
from constants.file_constants import MAX_DOWNLOADS_PER_USER
from database.db import AsyncSessionDep
from exceptions.file_exceptions import (
    chunk_merge_failed_exception,
    download_not_implemented_exception,
    file_not_found_exception,
    file_persist_failed_exception,
    invalid_part_data_exception,
    invalid_request_parameters_exception,
    part_already_exists_exception,
    part_number_out_of_range_exception,
    part_validation_failed_exception,
    permission_denied_exception,
    user_download_limit_exception,
)
from models.file_models import (
    ChunkedUploadStatus,
    FileCreate,
    FileDownloadMeta,
    FileInDB,
    FileListResponse,
    FilePublic,
    PartCreate,
    PartInDB,
    PartPublic,
)
from models.user_models import UserInDB
from tools.logger import get_logger
logger = get_logger("api.file_api")


async def preflight_create_file(
    file_data: FileCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> FilePublic:
    """
    【预检接口 - 简单上传】
    
    功能:根据前端提供的元数据创建文件记录,但不写入用户 file_ids 与计数。
    时机:在实际文件上传之前调用
    目的:提前验证、预分配文件ID、支持秒传(哈希复用)
    
    流程:
    1. 清理过期的预检记录
    2. 参数验证(存储类型、文件名、文件类型)
    3. 用户配额检查
    4. 重复文件检查(文件名、哈希)
    5. 尝试复用已存在的相同哈希文件(秒传)
    6. 如果无法复用,创建新的文件记录(预检状态)
    
    注意:
    - 此时文件记录已创建,但用户的 file_ids 和统计信息尚未更新
    - 只有在后续上传接口中校验通过后,才会更新用户计数
    """
    ensure_storage_dirs()

    # 参数验证
    if not await validate_upload_request(async_session, current_user, file_data):
        logger.error(f"validation failed user={current_user.id} filename={file_data.filename}")
        raise invalid_request_parameters_exception

    # 尝试复用同哈希文件,否则创建新记录
    assert file_data.hash_sha256 is not None
    reused_file = await get_reuse_file_by_hash(async_session, current_user, file_data.hash_sha256)

    if reused_file:
        file_record = reused_file
    else:
        file_record = FileInDB(
            **file_data.model_dump(),
            path="",  # 预检时设置为空字符串,上传成功后再更新
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


async def upload_file_chunk(
    part_data_json: Annotated[str, Form(alias="part_data_json")],
    chunk: Annotated[UploadFile, File()],
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    background_tasks: BackgroundTasks,
) -> PartPublic:
    """
    【分片上传接口】
    
    功能:上传单个文件分片,每次上传后验证并尝试合并
    时机:在预检接口之后,逐个上传分片
    
    请求格式:multipart/form-data
    - part_data: JSON 字符串,包含分片元数据,对应模型为PartCreate
    - chunk: 分片文件内容
    
    流程:
    1. 解析分片元数据(从 JSON 字符串)
    2. 根据 file_id 查询文件记录
    3. 验证分片编号范围
    4. 检查分片是否已上传(防止重复)
    5. 验证分片哈希和大小
    6. 保存分片到临时目录
    7. 创建分片记录
    8. 尝试合并分片(如果所有分片都已上传)
    9. 如果合并成功,更新用户统计信息并触发后台任务
    
    合并时机:
    - 每次上传分片后都会检查是否所有分片都已上传
    - 如果是最后一个分片,会自动触发合并
    
    后台任务:
    - 如果是 ZIP 文件,合并成功后会自动触发实验文件提取
    
    注意:
    - 分片保存在临时目录,合并成功后会被删除
    - 合并失败时会清理所有分片和文件记录
    - part_data 必须是有效的 JSON 字符串
    """

    ensure_storage_dirs()

    # 1. 解析 JSON 字符串为 PartCreate 对象
    try:
        part_data_dict = json.loads(part_data_json)
        part_data = PartCreate(**part_data_dict)

    except Exception as exc:
        logger.error(f"invalid part_data user={current_user.id} error={exc}")
        raise invalid_part_data_exception from exc

    logger.info(f"chunk upload start user={current_user.id} file_hash={part_data.file_hash} part={part_data.part_number}")

    # 2. 根据 file_hash 查询文件记录
    file_record = (
        await async_session.exec(
            select(FileInDB)
            .where(FileInDB.hash_sha256 == part_data.file_hash))
            ).first()
    if not file_record:
        raise file_not_found_exception

    # 3. 验证分片编号范围:1 <= part_number <= total_parts
    assert part_data.part_number is not None and file_record.total_parts is not None
    if not 1 <= part_data.part_number <= file_record.total_parts:
        logger.error(f"part_number out of range:\n"
                     f"{part_data.part_number} not in [1, {file_record.total_parts}] file_id={file_record.file_id} user={current_user.id}")
        raise part_number_out_of_range_exception

    # 4. 检查分片是否已上传(防止重复)- 调用业务层
    assert file_record.file_id is not None and current_user.id is not None
    if await validate_chunk_metadata(async_session, file_record.file_id, part_data.part_number, current_user.id):
        logger.warning(f"chunk already exists user={current_user.id} file_id={file_record.file_id} part={part_data.part_number}")
        raise part_already_exists_exception

    # 5. 验证分片哈希和大小
    act_part_hash, act_part_size = await compute_upload_file_metadata(chunk)
    if act_part_hash != part_data.part_hash or act_part_size != part_data.part_size:
        logger.error(
            f"chunk validation:\n"
            f"failed user={current_user.id} file_id={file_record.file_id} part={part_data.part_number} \n"
            f"expected_hash={part_data.part_hash} actual_hash={act_part_hash} \n"
            f"expected_size={part_data.part_size} actual_size={act_part_size}")
        raise part_validation_failed_exception

    # 6. 保存分片到临时目录
    assert file_record.hash_sha256 is not None
    temp_path = prepare_temp_chunk_path(current_user.id, file_record.hash_sha256, part_data.part_number)
    try:
        await save_upload_file(chunk, temp_path)
    except Exception as exc:
        logger.error(f"save chunk failed user={current_user.id} file_id={file_record.file_id} part={part_data.part_number} error={exc}")
        delete_local_file(temp_path)
        raise file_persist_failed_exception from exc

    part_record = PartInDB(
        **part_data.model_dump(),
        temp_path=str(temp_path),
        owner_id=current_user.id,
        file_id=file_record.file_id,
        total_file_size=file_record.size,
        total_parts=file_record.total_parts,
    )
    async_session.add(part_record)
    await async_session.commit()
    await async_session.refresh(part_record)
    await async_session.refresh(current_user)
    await async_session.refresh(file_record)
    logger.info(f"chunk saved user={current_user.id} file_id={file_record.file_id} part={part_data.part_number} size={act_part_size}")

    try:
        merge_success = await validate_and_merge_chunks(async_session, file_record, current_user)
        if merge_success:
            # 合并成功,路径已在 merge_chunks_to_file 中更新,预检时已建立用户关联
            await async_session.commit()
            await async_session.refresh(current_user)
            await async_session.refresh(file_record)
            logger.info(f"chunked upload completed user={current_user.id} file_id={file_record.file_id}")

            trigger_experiment_extraction(
                background_tasks, file_record, current_user,
                file_record.experiment_type,
            )

            # 合并成功,返回分片信息
            return part_record # type: ignore

    except HTTPException:
        raise

    except Exception as exc:
        logger.error(f"unexpected error during chunk upload:\n"
                     f"user={current_user.id} file_id={file_record.file_id} part={part_data.part_number}\n"
                     f"error={exc}")
        raise chunk_merge_failed_exception from exc

    # 上传成功但未合并,返回分片信息
    return part_record # type: ignore


async def get_chunked_upload_status(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> ChunkedUploadStatus:
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
    status = ChunkedUploadStatus(
        file_hash=file_record.hash_sha256 or "",
        filename=file_record.filename,
        total_parts=file_record.total_parts or 0,
        uploaded_parts=[],
        total_size=file_record.size,
        is_complete=False,
    )

    # 检查文件是否已完成上传
    assert current_user.id is not None
    user_file_ids = await get_user_file_ids(session, current_user.id)
    if file_record.file_id in user_file_ids:
        status.total_parts = 0
        status.is_complete = True
        return status

    # 查询已上传的分片
    uploaded_chunks = (
        await session.exec(
            select(PartInDB)
            .where(and_(PartInDB.file_id == file_id, PartInDB.owner_id == current_user.id))
            .order_by(PartInDB.part_number) # type: ignore
        )
    ).all()

    status.uploaded_parts = [chunk.part_number for chunk in uploaded_chunks]
    return status


async def upload_simple_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    background_tasks: BackgroundTasks,
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

    # 查询预检记录
    pre_record = await get_file_record_by_id(pre_file_id, async_session)

    # 统一验证(业务层)
    _, size = await validate_simple_upload(async_session, current_user, file, pre_record)

    # 文件名校验(显式检查,避免 assert 在 -O 模式下被跳过)
    if not file.filename:
        raise invalid_request_parameters_exception

    final_path = prepare_local_storage_path(file.filename)

    try:
        # 保存文件 → 更新记录 → 提交,统一 try 保护
        await save_upload_file(file, final_path)

        # 更新预检记录的路径和大小(预检时已建立用户关联)
        pre_record.path = str(final_path)
        pre_record.size = size
        async_session.add(pre_record)

        await async_session.commit()
        await async_session.refresh(pre_record)
        await async_session.refresh(current_user)

    except Exception as exc:
        logger.error(f"simple upload failed user={current_user.id} filename={file.filename} error={exc}")
        await rollback_simple_upload(async_session, current_user, pre_record, final_path)
        raise file_persist_failed_exception from exc

    logger.info(f"simple upload completed user={current_user.id} file_id={pre_record.file_id} filename={pre_record.filename} size={pre_record.size}")

    trigger_experiment_extraction(background_tasks, pre_record, current_user, pre_record.experiment_type)

    return pre_record # type: ignore


async def list_user_files(
    current_user: Annotated[UserInDB, Depends(token_authen)], session: AsyncSessionDep
) -> FileListResponse:
    """
    【文件列表查询接口】
    
    功能:查询当前用户的全部文件记录
    返回:包含文件总数、文件列表和用户ID的JSON对象
    
    查询逻辑:
    - 从用户的 file_ids 列表中获取文件ID
    - 批量查询文件记录
    
    注意:
    - 只返回用户拥有的文件(file_id 在 user.file_ids 中)
    - 不包括预检记录(未完成上传的文件)
    """
    # 1. 快速返回:用户没有任何文件
    assert current_user.id is not None
    user_file_ids = await get_user_file_ids(session=session, user_id=current_user.id)
    if not user_file_ids:
        return FileListResponse(
            total=0,
            files=[],
            user_id=current_user.id
        )

    # 2. 批量查询文件记录(使用 IN 查询,效率高)
    statement = select(FileInDB).where(FileInDB.file_id.in_(user_file_ids))  # pylint: disable=no-member # type: ignore
    all_results = (await session.exec(statement)).all()

    # 只保留真正上传的文件
    filtered_results = []
    for file_record in all_results:
        if await is_file_uploaded(session, file_record):
            filtered_results.append(file_record)

    logger.debug("list user files user=%s count=%s", current_user.id, len(filtered_results))

    return FileListResponse(
        total=len(filtered_results),
        files=list(filtered_results), # type: ignore
        user_id=current_user.id
    )


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
        logger.info(
            "unlinked file reference only user=%s file_id=%s ",
            current_user.id,
            file_id,
        )
        return {"file_id": file_id, "status": "unlinked"}

    # 没有其他用户持有该文件,执行完全删除
    # 删除实验相关文件(如果有)
    if file_record.experiment_type == "imzML":
        await delete_experiment_files(session, file_id)

    # 删除文件、分片和数据库记录
    await delete_file_and_parts(session, file_id, current_user)

    await session.commit()
    await session.refresh(current_user)
    logger.info(
        "deleted file completely user=%s file_id=%s", current_user.id, file_id
    )
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

    logger.debug("download request user=%s file_id=%s", current_user.id, file_id)

    # 下载次数限制检查
    if current_user.identity != "admin":
        used = current_user.download_used or 0
        if used >= MAX_DOWNLOADS_PER_USER:
            raise user_download_limit_exception

    # 存储类型检查
    if file_record.storage_type != "local":
        raise download_not_implemented_exception

    # 准备下载文件(业务层处理)
    await prepare_download_file(session, file_record)

    # 更新下载统计(业务层处理)
    await update_download_stats(session, file_record, current_user)
    await session.commit()
    await session.refresh(current_user)
    await session.refresh(file_record)

    logger.info(
        "download success user=%s file_id=%s filename=%s",
        current_user.id,
        file_id,
        file_record.filename,
    )

    return FileDownloadMeta.from_record(file_record)

# 依赖注入类型别名
DeleteFileDep = Annotated[dict[str, Any], Depends(delete_user_file)]
SimpleUploadDep = Annotated[FilePublic, Depends(upload_simple_file)]
ListFilesDep = Annotated[FileListResponse, Depends(list_user_files)]
DownloadFileDep = Annotated[FileDownloadMeta, Depends(get_download_file_metadata)]
PreflightDep = Annotated[FilePublic, Depends(preflight_create_file)]
ChunkUploadDep = Annotated[PartPublic, Depends(upload_file_chunk)]
ChunkedStatusDep = Annotated[ChunkedUploadStatus, Depends(get_chunked_upload_status)]
