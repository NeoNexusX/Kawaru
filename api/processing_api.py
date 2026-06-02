"""
Processing API — API 入口层
"""
from typing import Annotated

from fastapi import Depends
from fastapi.exceptions import HTTPException

from api.processing_api_business import (
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
from api.processing_api_helper import assert_run_ownership
from api.user_api import token_authen
from database.db import AsyncSessionDep
from exceptions.experiment_exceptions import invalid_process_request_exception
from exceptions.common_exceptions import internal_server_error_exception
from models.processing_models import (
    ExperimentProcessRunCreate,
    ExperimentProcessRunPublic,
)
from api.processing_api_business import assert_zarr_folder_access
from storage_backend.storage import OssStorage
from constants.file_constants import OSS_BUCKET_NAME, OSS_REGION_ID
from models.user_models import UserInDB
from models.file_models import ZarrFolderAccess
from constants.experiment_constants import OSS_PROCESSED_STORAGE_ROOT
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
) -> list[ExperimentProcessRunPublic]:
    """根据文件ID查询公开的运行记录"""
    rows = await list_public_runs_by_file_id(async_session, file_id)
    return [ExperimentProcessRunPublic(**row) for row in rows]


async def list_my_process_runs(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> list[ExperimentProcessRunPublic]:
    """查询当前用户的所有运行记录"""
    assert current_user.id is not None
    rows = await get_user_runs(async_session, current_user.id)
    return [ExperimentProcessRunPublic(**row) for row in rows]


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

    expires = 3600  # STS Token 1 小时有效
    sts_token = await OssStorage.get_zarr_read_sts_token(run_record.out_path, expires)  # type: ignore
    if sts_token is None:
        raise internal_server_error_exception

    return ZarrFolderAccess(
        folder_path=run_record.out_path,  # type: ignore
        bucket=OSS_BUCKET_NAME,           # type: ignore
        region=OSS_REGION_ID,             # type: ignore
        sts_token=sts_token,
        expires_in=expires,
    )


# 依赖注入类型别名
ZarrFolderDep = Annotated[ZarrFolderAccess, Depends(get_zarr_folder_access)]
