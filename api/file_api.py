#########################
# File API - API入口层
#########################
from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any
from fastapi import BackgroundTasks, Depends, File, Form, HTTPException, UploadFile
from sqlmodel import select
from api.experiment_api import process_imzml_experiment_background
from api.file_api_business import (
    cleanup_stale_preflights,
    delete_experiment_files,
    delete_file_and_parts,
    get_file_record,
    link_file_to_user,
    prepare_download_file,
    safe_cleanup_preflight_record,
    unlink_user_from_file,
    update_download_stats,
    validate_and_merge_chunks,
    validate_and_try_reuse_file,
    validate_chunk_metadata,
    validate_simple_upload,
    validate_upload_request,
)
from api.file_api_helper import (
    assert_file_owner,
    compute_upload_file_metadata,
    delete_local_file,
    ensure_storage_dirs,
    prepare_local_storage_path,
    prepare_temp_chunk_path,
    save_upload_file,
)
from api.user_api import token_authen
from constants import (
    MAX_DOWNLOADS_PER_USER,
)
from database.db import SessionDep
from exceptions import (
    chunk_merge_failed_exception,
    download_not_implemented_exception,
    file_not_found_exception,
    file_persist_failed_exception,
    file_reused_exception,
    invalid_part_data_exception,
    invalid_request_parameters_exception,
    part_already_exists_exception,
    part_number_out_of_range_exception,
    part_validation_failed_exception,
    permission_denied_exception,
    user_download_limit_exception,
)

from logger import get_logger
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
logger = get_logger("main.file_api")


async def preflight_create_file(
    file_data: FileCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> FilePublic:
    """
    【预检接口 - 简单上传】
    
    功能：根据前端提供的元数据创建文件记录，但不写入用户 file_ids 与计数。
    时机：在实际文件上传之前调用
    目的：提前验证、预分配文件ID、支持秒传（哈希复用）
    
    流程：
    1. 清理过期的预检记录
    2. 参数验证（存储类型、文件名、文件类型）
    3. 用户配额检查
    4. 重复文件检查（文件名、哈希）
    5. 尝试复用已存在的相同哈希文件（秒传）
    6. 如果无法复用，创建新的文件记录（预检状态）
    
    注意：
    - 此时文件记录已创建，但用户的 file_ids 和统计信息尚未更新
    - 只有在后续上传接口中校验通过后，才会更新用户计数与 file_ids
    - 如果上传失败，预检记录会在下次预检时被清理
    """
    ensure_storage_dirs()

    try:
        await cleanup_stale_preflights(session, current_user)
    except (OSError, IOError) as exc:#TODO
        # 清理失败不影响主流程，记录日志即可
        logger.warning(
            "cleanup stale preflights failed user=%s error=%s",
            current_user.id,
            exc,
        )

    logger.info(
        "preflight start user=%s filename=%s storage=%s size=%s",
        current_user.id,
        file_data.filename,
        file_data.storage_type,
        file_data.size,
    )

    # 参数验证
    if not validate_upload_request(file_data.storage_type, file_data.filename):
        logger.warning(
            "validation failed user=%s filename=%s storage=%s size=%s",
            current_user.id,
            file_data.filename,
            file_data.storage_type,
            file_data.size
        )
        raise invalid_request_parameters_exception

    # 验证文件并尝试复用（配额检查、重复检查、哈希复用）
    reused_file = await validate_and_try_reuse_file(session, current_user, file_data)
    if reused_file:
        # 检查文件是否真实存在（物理文件）
        if Path(reused_file.path).exists():
            # 文件存在，提交事务后抛出复用异常
            await session.commit()
            await session.refresh(reused_file)
            await session.refresh(current_user)
            logger.info(
                "preflight reused existing file user=%s file_id=%s",
                current_user.id,
                reused_file.file_id,
            )
            raise file_reused_exception
        else:
            # 文件不存在，返回记录等待上传
            await session.commit()
            await session.refresh(reused_file)
            await session.refresh(current_user)
            logger.info(
                "preflight reused record but file missing user=%s file_id=%s",
                current_user.id,
                reused_file.file_id,
            )
            return reused_file

    # 创建新的文件记录
    final_path = prepare_local_storage_path(file_data.filename)

    file_data_dict = file_data.model_dump()
    file_record = FileInDB(
        **file_data_dict,
        path=str(final_path),
        owners=[current_user.id],
    )
    session.add(file_record)
    await session.commit()
    await session.refresh(file_record)
    await session.refresh(current_user)
    logger.info(
        "preflight created new file record user=%s file_id=%s filename=%s",
        current_user.id,
        file_record.file_id,
        file_record.filename,
    )
    return file_record


async def preflight_create_chunked_file(
    file_data: FileCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> FilePublic:
    """
    【预检接口 - 分片上传】
    
    功能：为分片上传创建文件记录，准备接收分片
    时机：在分片上传之前调用
    目的：提前验证、预分配文件ID、支持秒传（哈希复用）
    
    流程：
    1. 清理过期的预检记录
    2. 参数验证（存储类型、文件名、文件类型、分片数量）
    3. 用户配额检查
    4. 重复文件检查（文件名、哈希）
    5. 尝试复用已存在的相同哈希文件（秒传）
    6. 如果无法复用，创建新的文件记录（预检状态）
    
    与简单上传的区别：
    - 需要验证 total_parts 参数
    - 复用文件时需要更新 total_parts
    
    注意：
    - 此时文件记录已创建，但用户的 file_ids 和统计信息尚未更新
    - 只有在所有分片上传并合并成功后，才会更新用户计数与 file_ids
    """
    ensure_storage_dirs()

    try:
        await cleanup_stale_preflights(session, current_user)
    except (OSError, IOError) as exc:#TODO
        # 清理失败不影响主流程，记录日志即可
        logger.warning(
            "cleanup stale records failed user=%s error=%s",
            current_user.id,
            exc,
        )
    await session.refresh(current_user)
    logger.info(
        "chunked preflight start user=%s filename=%s storage=%s size=%s parts=%s",
        current_user.id,
        file_data.filename,
        file_data.storage_type,
        file_data.size,
        file_data.total_parts,
    )

    # 参数验证
    if not validate_upload_request(file_data.storage_type, file_data.filename, file_data.total_parts):
        logger.warning(
            "validation failed user=%s filename=%s storage=%s size=%s parts=%s",
            current_user.id,
            file_data.filename,
            file_data.storage_type,
            file_data.size,
            file_data.total_parts
        )
        raise invalid_request_parameters_exception

    # 验证文件并尝试复用（配额检查、重复检查、哈希复用）
    reused_file = await validate_and_try_reuse_file(session, current_user, file_data)
    if reused_file:
        # 更新 total_parts（分片上传特有）
        reused_file.total_parts = file_data.total_parts
        session.add(reused_file)
        
        # 检查文件是否真实存在（物理文件）
        if Path(reused_file.path).exists():
            # 文件存在，提交事务后抛出复用异常
            await session.commit()
            await session.refresh(reused_file)
            await session.refresh(current_user)
            logger.info(
                "chunked preflight reused existing file user=%s file_id=%s parts=%s",
                current_user.id,
                reused_file.file_id,
                file_data.total_parts,
            )
            raise file_reused_exception
        else:
            # 文件不存在，返回记录等待上传
            await session.commit()
            await session.refresh(reused_file)
            await session.refresh(current_user)
            logger.info(
                "chunked preflight reused record but file missing user=%s file_id=%s parts=%s",
                current_user.id,
                reused_file.file_id,
                file_data.total_parts,
            )
            return reused_file

    # 创建新的文件记录
    final_path = prepare_local_storage_path(file_data.filename)

    # 将 FileCreate 转换为 FileInDB
    file_data_dict = file_data.model_dump()
    file_record = FileInDB(
        **file_data_dict,
        path=str(final_path),
        owners=[current_user.id],
    )
    session.add(file_record)
    await session.commit()
    await session.refresh(file_record)
    await session.refresh(current_user)
    logger.info(
        "chunked preflight created new record user=%s file_id=%s filename=%s parts=%s",
        current_user.id,
        file_record.file_id,
        file_record.filename,
        file_data.total_parts,
    )
    return file_record


async def upload_file_chunk(
    part_data_json: Annotated[str, Form(alias="part_data_json")],
    chunk: Annotated[UploadFile, File()],
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    background_tasks: BackgroundTasks,
) -> PartPublic:
    """
    【分片上传接口】
    
    功能：上传单个文件分片，每次上传后验证并尝试合并
    时机：在预检接口之后，逐个上传分片
    
    请求格式：multipart/form-data
    - part_data: JSON 字符串，包含分片元数据
    - chunk: 分片文件内容
    
    流程：
    1. 解析分片元数据（从 JSON 字符串）
    2. 根据 file_id 查询文件记录
    3. 验证分片编号范围
    4. 检查分片是否已上传（防止重复）
    5. 验证分片哈希和大小
    6. 保存分片到临时目录
    7. 创建分片记录
    8. 尝试合并分片（如果所有分片都已上传）
    9. 如果合并成功，更新用户统计信息并触发后台任务
    
    合并时机：
    - 每次上传分片后都会检查是否所有分片都已上传
    - 如果是最后一个分片，会自动触发合并
    
    后台任务：
    - 如果是 ZIP 文件，合并成功后会自动触发实验文件提取
    
    注意：
    - 分片保存在临时目录，合并成功后会被删除
    - 合并失败时会清理所有分片和文件记录
    - part_data 必须是有效的 JSON 字符串
    """

    ensure_storage_dirs()

    # 1. 解析 JSON 字符串为 PartCreate 对象
    try:
        part_data_dict = json.loads(part_data_json)
        part_data = PartCreate(**part_data_dict)
    except Exception as exc:
        logger.error(
            "invalid part_data user=%s error=%s",
            current_user.id,
            exc,
        )
        raise invalid_part_data_exception from exc

    logger.info(
        "chunk upload start user=%s file_hash=%s part=%s",
        current_user.id,
        part_data.file_hash,
        part_data.part_number,
    )

    # 2. 根据 file_hash 查询文件记录
    file_record = (
        await session.exec(
            select(FileInDB).where(FileInDB.hash_sha256 == part_data.file_hash)
        )
    ).first()
    if not file_record:
        raise file_not_found_exception

    # 3. 验证分片编号范围：1 <= part_number <= total_parts
    if not 1 <= part_data.part_number <= file_record.total_parts:
        logger.error(
            "part_number out of range: %s not in [1, %s] file_id=%s user=%s",
            part_data.part_number,
            file_record.total_parts,
            file_record.file_id,
            current_user.id,
        )
        raise part_number_out_of_range_exception

    # 4. 检查分片是否已上传（防止重复）- 调用业务层
    if await validate_chunk_metadata(session, file_record.file_id, part_data.part_number, current_user.id):
        logger.warning(
            "chunk already exists user=%s file_id=%s part=%s",
            current_user.id,
            file_record.file_id,
            part_data.part_number,
        )
        raise part_already_exists_exception

    # 5. 验证分片哈希和大小
    chunk_hash, chunk_size = await compute_upload_file_metadata(chunk)
    if chunk_hash != part_data.part_hash or chunk_size != part_data.part_size:
        logger.error(
            "chunk validation failed user=%s file_id=%s part=%s expected_hash=%s actual_hash=%s expected_size=%s actual_size=%s",
            current_user.id,
            file_record.file_id,
            part_data.part_number,
            part_data.part_hash,
            chunk_hash,
            part_data.part_size,
            chunk_size,
        )
        raise part_validation_failed_exception

    # 6. 保存分片到临时目录
    temp_path = prepare_temp_chunk_path(current_user.id, file_record.hash_sha256, part_data.part_number)
    try:
        await save_upload_file(chunk, temp_path)
    except Exception as exc:
        logger.error(
            "save chunk failed user=%s file_id=%s part=%s error=%s",
            current_user.id,
            file_record.file_id,
            part_data.part_number,
            exc,
        )
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
    session.add(part_record)
    await session.commit()
    await session.refresh(part_record)
    await session.refresh(current_user)
    await session.refresh(file_record)
    logger.info(
        "chunk saved user=%s file_id=%s part=%s size=%s",
        current_user.id,
        file_record.file_id,
        part_data.part_number,
        chunk_size,
    )

    try:
        merge_success = await validate_and_merge_chunks(
            session, file_record, current_user
        )
        if merge_success:
            link_file_to_user(session, current_user, file_record.file_id, file_record.size)
            await session.commit()
            await session.refresh(current_user)
            await session.refresh(file_record)
            logger.info(
                "chunked upload completed user=%s file_id=%s",
                current_user.id,
                file_record.file_id,
            )

            if file_record.file_type == "zip":
                experiment_name = Path(file_record.filename).stem
                background_tasks.add_task(
                    process_imzml_experiment_background,
                    current_user.id,
                    file_record.file_id,
                    experiment_name,
                )
                logger.info(
                    "auto extraction triggered for chunked ZIP user=%s file_id=%s",
                    current_user.id,
                    file_record.file_id,
                )

            # 合并成功，返回分片信息
            return part_record 
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(
            "unexpected error during chunk upload user=%s file_id=%s part=%s error=%s",
            current_user.id,
            file_record.file_id,
            part_data.part_number,
            exc,
        )
        raise chunk_merge_failed_exception from exc

    # 上传成功但未合并，返回分片信息
    return part_record 


async def get_chunked_upload_status(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> ChunkedUploadStatus:
    """
    【分片上传状态查询接口】
    
    功能：查询分片上传的进度和状态
    时机：前端可以定期调用此接口查询上传进度
    
    返回信息：
    - file_hash: 文件哈希
    - filename: 文件名
    - total_parts: 总分片数
    - uploaded_parts: 已上传的分片编号列表
    - total_size: 文件总大小
    - is_complete: 是否已完成上传
    
    判断逻辑：
    - 如果文件ID在用户的 file_ids 中，说明已完成上传
    - 否则查询已上传的分片记录
    """
    file_record = await get_file_record(file_id, session)
    if not file_record:
        raise file_not_found_exception

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
    user_file_ids = set(current_user.file_ids or [])
    if file_record.file_id in user_file_ids:
        status.total_parts = 0
        status.is_complete = True
        return status

    # 查询已上传的分片
    uploaded_chunks = (
        await session.exec(
            select(PartInDB)
            .where(PartInDB.file_id == file_id, PartInDB.owner_id == current_user.id)
            .order_by(PartInDB.part_number)
        )
    ).all()

    status.uploaded_parts = [chunk.part_number for chunk in uploaded_chunks]
    return status


async def upload_simple_file(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
    background_tasks: BackgroundTasks,
    storage_type: str = Form("local"),
    pre_file_id: int = Form(...),
    file: UploadFile = File(...),
) -> FilePublic:
    """
    【简单上传接口】
    
    功能：一次性接收完整文件，不做分片
    时机：在预检接口之后调用
    适用场景：小文件上传（< 100MB）
    
    流程：
    1. 查询预检记录
    2. 统一验证（存储类型、文件名、元数据、配额）
    3. 保存文件到存储目录
    4. 更新用户统计信息（file_ids、file_count、total_file_size）
    5. 如果是 ZIP 文件，触发后台任务提取实验文件
    
    后台任务：
    - 如果是 ZIP 文件，上传成功后会自动触发实验文件提取
    
    注意：
    - 必须先调用预检接口获取 pre_file_id
    - 文件哈希和大小必须与预检记录匹配，否则上传失败
    - 上传失败时会清理预检记录
    """
    ensure_storage_dirs()
    logger.info(
        "simple upload start user=%s filename=%s storage=%s",
        current_user.id,
        file.filename,
        storage_type,
    )

    # 查询预检记录
    pre_record = await get_file_record(pre_file_id, session)
    if not pre_record:
        raise file_not_found_exception

    # 统一验证（业务层）
    _, size = await validate_simple_upload(
        session, current_user, file, storage_type, pre_record
    )

    # 保存文件
    final_path = prepare_local_storage_path(file.filename)
    try:
        await save_upload_file(file, final_path)
    except Exception as exc:
        logger.error(
            "save upload file failed user=%s filename=%s error=%s",
            current_user.id,
            file.filename,
            exc,
        )
        delete_local_file(final_path)
        await safe_cleanup_preflight_record(session, current_user, pre_record)
        await session.commit()
        raise file_persist_failed_exception from exc

    file_record = pre_record
    await session.flush()

    link_file_to_user(session, current_user, file_record.file_id, size)
    await session.commit()
    await session.refresh(file_record)
    await session.refresh(current_user)
    logger.info(
        "simple upload completed user=%s file_id=%s filename=%s size=%s",
        current_user.id,
        file_record.file_id,
        file_record.filename,
        file_record.size,
    )

    if file_record.file_type == "zip":
        experiment_name = Path(file_record.filename).stem
        background_tasks.add_task(
            process_imzml_experiment_background,
            current_user.id,
            file_record.file_id,
            experiment_name,
        )
        logger.info(
            "auto extraction triggered for ZIP user=%s file_id=%s",
            current_user.id,
            file_record.file_id,
        )

    return file_record


async def list_user_files(
    current_user: Annotated[UserInDB, Depends(token_authen)], session: SessionDep
) -> FileListResponse:
    """
    【文件列表查询接口】
    
    功能：查询当前用户的全部文件记录
    返回：包含文件总数、文件列表和用户ID的JSON对象
    
    查询逻辑：
    - 从用户的 file_ids 列表中获取文件ID
    - 批量查询文件记录
    
    注意：
    - 只返回用户拥有的文件（file_id 在 user.file_ids 中）
    - 不包括预检记录（未完成上传的文件）
    """
    # 1. 快速返回：用户没有任何文件
    if not current_user.file_ids:
        return FileListResponse(
            total=0,
            files=[],
            user_id=current_user.id
        )

    # 2. 获取用户的文件ID列表（转换为list以便后续操作）
    seen_ids = list(current_user.file_ids or [])
    if not seen_ids:
        return FileListResponse(
            total=0,
            files=[],
            user_id=current_user.id
        )

    # 3. 批量查询文件记录（使用 IN 查询，效率高）
    statement = select(FileInDB).where(FileInDB.file_id.in_(seen_ids))  # pylint: disable=no-member
    results = (await session.exec(statement)).all()

    logger.debug("list user files user=%s count=%s", current_user.id, len(results))

    return FileListResponse(
        total=len(results),
        files=list(results),
        user_id=current_user.id
    )


async def delete_user_file(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
) -> dict[str, Any]:
    """
    【文件删除接口】
    
    功能：删除当前用户的指定文件
    
    删除策略（多用户共享文件）：
    1. 如果文件被多个用户共享：
       - 只解除当前用户与文件的关联
       - 更新当前用户的统计信息（file_count、total_file_size）
       - 从文件的 owners 列表中移除当前用户
       - 保留文件记录和物理文件
       - 返回状态：unlinked
    
    2. 如果文件只属于当前用户：
       - 删除实验相关文件（如果有，如 imzML、ibd）
       - 删除文件物理文件
       - 删除所有分片记录和物理文件
       - 删除文件数据库记录
       - 更新当前用户的统计信息
       - 返回状态：deleted
    
    判断逻辑：
    - 通过 file_record.owners 列表判断是否有其他用户持有该文件
    - 移除当前用户后，如果 owners 为空，则执行完全删除

    """
    file_record = await get_file_record(file_id, session)
    if not file_record:
        raise file_not_found_exception
    if not assert_file_owner(file_record, current_user):
        raise permission_denied_exception

    # 解除用户与文件的关联（业务层）
    has_other_owners = await unlink_user_from_file(session, current_user, file_record)

    if has_other_owners:
        # 还有其他用户持有，只解除关联
        await session.commit()
        await session.refresh(current_user)
        await session.refresh(file_record)
        logger.info(
            "unlinked file reference only user=%s file_id=%s remaining_owners=%s",
            current_user.id,
            file_id,
            file_record.owners,
        )
        return {"file_id": file_id, "status": "unlinked"}

    # 没有其他用户持有该文件，执行完全删除
    # 删除实验相关文件（如果有）
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
    session: SessionDep,
) -> FileDownloadMeta:
    """
    【文件下载元数据接口】
    
    功能：返回文件元数据和路径，供下载使用
    时机：在实际下载文件之前调用
    
    流程：
    1. 权限验证（用户是否拥有该文件）
    2. 下载次数限制检查（非管理员用户）
    3. 准备下载文件（处理实验文件ZIP打包）
    4. 更新下载统计（文件下载次数、用户下载次数）
    
    实验文件处理：
    - 如果原始文件不存在，但实验文件（imzML、ibd）存在
    - 动态创建 ZIP 包，包含 imzML 和 ibd 文件
    - ZIP 包保存在临时目录，供下载使用
    
    下载限制：
    - 管理员：无限制
    - 普通用户：受 MAX_DOWNLOADS_PER_USER 限制
    
    注意：
    - 此接口只返回元数据，不返回文件流
    - 实际文件下载由前端或其他接口处理
    - 实验文件的 ZIP 包是临时生成的，需要定期清理
    """

    file_record = await get_file_record(file_id, session)
    if not file_record:
        raise file_not_found_exception
    if not assert_file_owner(file_record, current_user):
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

    # 准备下载文件（业务层处理）
    await prepare_download_file(session, file_record)

    # 更新下载统计（业务层处理）
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
ChunkedPreflightDep = Annotated[FilePublic, Depends(preflight_create_chunked_file)]
ChunkUploadDep = Annotated[PartPublic, Depends(upload_file_chunk)]
ChunkedStatusDep = Annotated[ChunkedUploadStatus, Depends(get_chunked_upload_status)]
