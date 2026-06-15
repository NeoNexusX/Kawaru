"""
Processing API — API 入口层
"""
import json
from typing import Annotated

from fastapi import Depends
from fastapi.exceptions import HTTPException

from api_services.processing_service import (
    create_process_run,
    delete_process_run,
    get_origin_for_processing,
    get_process_run_by_id,
    get_user_runs,
    list_public_runs_by_file_id,
    rollback_process_run,
    validate_process_request,
    get_storage_type_by_source_file_id,
)
from api_helpers.processing_helper import assert_run_ownership
from api.user_api import token_authen
from database.db import AsyncSessionDep
from database.redis_client import redis_client
from exceptions.experiment_exceptions import invalid_process_request_exception
from exceptions.common_exceptions import internal_server_error_exception
from constants.experiment_constants import OSS_PROCESSED_STORAGE_ROOT
from constants.file_constants import OSS_BUCKET_NAME, OSS_REGION_ID, SIGNED_URL_EXPIRE_SECONDS, PACKUP_CACHE_TTL_SECONDS
from models.file_models import ZarrFolderAccess
from models.filter_models import ListDataResponse, PaginationMeta
from models.processing_models import (
    ExperimentProcessRunCreate,
    ExperimentProcessRunPublic,
)
from api_services.processing_service import assert_zarr_folder_access
from storage_backend.storage import OssStorage
from constants.file_constants import OSS_BUCKET_NAME, OSS_REGION_ID, SIGNED_URL_EXPIRE_SECONDS, PACKUP_CACHE_TTL_SECONDS
from models.user_models import UserInDB
from storage_backend.storage import OssStorage
from background.processing_background import trigger_fc_algorithm_task
from tools.logger import get_logger

logger = get_logger("api.processing_api")


async def create_algorithm_task(
    body: ExperimentProcessRunCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunPublic:
    """创建算法处理任务 — 写 DB + 异步触发 FC"""
    run_record = None
    try:
        # 1. 纯校验（配额、算法配置）
        if not await validate_process_request(current_user, body.algorithms):
            raise invalid_process_request_exception

        # 2. 查文件归属 + 就绪 origin，同时获取文件名
        origin, filename = await get_origin_for_processing(async_session, body.file_id, current_user)

        assert current_user.id is not None
        run_record = await create_process_run(
            async_session, origin, body.algorithms, current_user.id, body.is_public,
        )

        # 构造 OSS 输出路径
        oss_output_path = f"{OSS_PROCESSED_STORAGE_ROOT.rstrip('/')}/run_{run_record.id}.zarr/" # type: ignore

        # 先提交数据库，让 run_id 生效
        await async_session.commit()
        await async_session.refresh(run_record)
        await async_session.refresh(origin)

        # 异步触发 FC 函数执行算法处理
        trigger_fc_algorithm_task.delay( # type: ignore
            run_id=run_record.id,
            imzml_path=origin.imzml_path,
            config_json=run_record.params_json,
            output_path=oss_output_path,
        )

        return ExperimentProcessRunPublic(**run_record.model_dump(), filename=filename)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("create algorithm task failed user=%s file_id=%s", current_user.id, body.file_id)
        await rollback_process_run(async_session, run_record)
        raise internal_server_error_exception from e


async def list_public_process_runs(
    file_id: int,
    async_session: AsyncSessionDep,
    page: int = 1,
    size: int = 10,
) -> ListDataResponse:
    """根据文件ID分页查询公开的运行记录"""
    try:
        runs, total_records = await list_public_runs_by_file_id(async_session, file_id, page, size)

        total_pages = (total_records + size - 1) // size if total_records > 0 else 0
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(runs),
            total_pages=total_pages,
            total_records=total_records,
        )
        return ListDataResponse(
            meta=pagination_meta,
            data=[ExperimentProcessRunPublic(**row) for row in runs],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("failed to list_public_process_runs: %s", e)
        raise internal_server_error_exception from e


async def list_my_process_runs(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
    page: int = 1,
    size: int = 10,
) -> ListDataResponse:
    """分页查询当前用户的所有运行记录"""
    try:
        assert current_user.id is not None
        runs, total_records = await get_user_runs(async_session, current_user.id, page, size)

        total_pages = (total_records + size - 1) // size if total_records > 0 else 0
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(runs),
            total_pages=total_pages,
            total_records=total_records,
        )
        return ListDataResponse(
            meta=pagination_meta,
            data=[ExperimentProcessRunPublic(**row) for row in runs],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("failed to list_my_process_runs: %s", e)
        raise internal_server_error_exception from e


async def delete_process_run_endpoint(
    run_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict[str, int | str]:
    """根据 run_id 删除算法处理运行记录"""
    run_record = await get_process_run_by_id(async_session, run_id)
    if not run_record:
        raise invalid_process_request_exception

    if not assert_run_ownership(run_record.user_id, current_user.id):  # type: ignore
        raise invalid_process_request_exception

    storage_type = await get_storage_type_by_source_file_id(async_session, run_record.source_file_id)
    await delete_process_run(async_session, run_record, storage_type)
    await async_session.commit()

    return {"run_id": run_id, "status": "deleted"}


async def get_zarr_folder_access(
    run_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ZarrFolderAccess:
    """
    【zarr 文件夹访问接口】

    返回文件夹路径和 OSS STS Token，前端凭此通过 OSS SDK 直连读取，
    无需逐个文件签名，支持 Range 请求和 zarr 随机分块读取。
    """

    run_record = await assert_zarr_folder_access(async_session, run_id, current_user)

    # Redis 缓存：同一 run_id 1 小时内复用同一个 STS Token
    cache_key = f"zarr_sts:{run_id}"
    cached = await redis_client.get(cache_key)  # type: ignore
    if cached:
        return ZarrFolderAccess(**json.loads(cached))

    expires = SIGNED_URL_EXPIRE_SECONDS
    sts_token = await OssStorage.get_zarr_read_sts_token(run_record.out_path, expires)  # type: ignore
    if sts_token is None:
        raise internal_server_error_exception

    access = ZarrFolderAccess(
        folder_path=run_record.out_path,  # type: ignore
        bucket=OSS_BUCKET_NAME,           # type: ignore
        region=OSS_REGION_ID,             # type: ignore
        sts_token=sts_token,
        expires_in=expires,
    )

    await redis_client.set(cache_key, access.model_dump_json(), ex=PACKUP_CACHE_TTL_SECONDS)  # type: ignore

    return access


# 依赖注入类型别名
CreateAlgorithmTaskDep = Annotated[ExperimentProcessRunPublic, Depends(create_algorithm_task)]
ListPublicRunsDep = Annotated[ListDataResponse, Depends(list_public_process_runs)]
ListMyRunsDep = Annotated[ListDataResponse, Depends(list_my_process_runs)]
DeleteProcessRunDep = Annotated[dict[str, int | str], Depends(delete_process_run_endpoint)]
ZarrFolderDep = Annotated[ZarrFolderAccess, Depends(get_zarr_folder_access)]
