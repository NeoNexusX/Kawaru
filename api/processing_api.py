"""
Processing API — API 入口层
"""
from typing import Annotated

from fastapi import Depends

from api.processing_api_business import (
    create_process_run,
    delete_process_run,
    execute_process_run_pipeline,
    get_process_run_by_id,
    get_user_runs,
    list_public_runs_by_file_id,
    rollback_process_run,
    validate_process_request,
    get_storage_type_by_source_file_id,
)
from api.processing_api_helper import assert_run_ownership, create_run_output_dir
from api.user_api import token_authen
from database.db import AsyncSessionDep
from exceptions.experiment_exceptions import invalid_process_request_exception
from exceptions.common_exceptions import internal_server_error_exception
from models.processing_models import (
    ExperimentProcessRunCreate,
    ExperimentProcessRunPublic,
    ExperimentProcessRunInDB,
)
from models.user_models import UserInDB
from tools.logger import get_logger

logger = get_logger("api.processing_api")


async def create_algorithm_task(
    body: ExperimentProcessRunCreate,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunInDB:
    """创建算法处理任务"""
    run_record = None
    try:
        origin = await validate_process_request(
            async_session, body.file_id, body.algorithms, current_user,
        )

        assert current_user.id is not None
        run_record = await create_process_run(
            async_session, origin, body.algorithms, current_user.id, body.is_public,)

        output_dir = create_run_output_dir(run_record.id)  # type: ignore
        await execute_process_run_pipeline(
            async_session, run_record.id, origin.imzml_path, output_dir,  # type: ignore
        )

        await async_session.commit()
        await async_session.refresh(run_record)
        return run_record

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
    return [ExperimentProcessRunPublic(**row.model_dump()) for row in rows]


async def list_my_process_runs(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> list[ExperimentProcessRunPublic]:
    """查询当前用户的所有运行记录"""
    assert current_user.id is not None
    rows = await get_user_runs(async_session, current_user.id)
    return [ExperimentProcessRunPublic(**row.model_dump()) for row in rows]


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
