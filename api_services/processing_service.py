#########################
# Processing Service - 业务逻辑层
# 职责：算法处理运行记录的增删改查与磁盘结果清理。无 FastAPI 依赖。
#########################
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Sequence

from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from database.db import get_async_engine
from models.processing_models import (
    ExperimentProcessRunInDB,
    ProcessingAlgorithmsConfig,
    ProcessingStatus,
)
from models.experiment_models import OriginImzMLInDB
from models.file_models import FileInDB
from models.user_models import UserInDB
from api_services.file_service import assert_file_owner
from api_services.experiment_service import get_origin_imzml_by_file_id
from api_helpers.processing_helper import (
    assert_origin_ready,
    check_processing_quota,
    create_run_output_dir,
    validate_algorithms_config,
)
from storage_backend.storage import OssStorage
from exceptions.experiment_exceptions import (
    invalid_process_request_exception,
    process_run_not_found_exception,
    process_run_permission_denied_exception,
    process_run_not_completed_exception,
    zarr_folder_not_found_exception,
)
from tools import utc_time
from tools.logger import get_logger

logger = get_logger("api_services.processing_service")


#########################
# 增
#########################

async def create_process_run(
    async_session: AsyncSession,
    experiment: OriginImzMLInDB,
    algorithms_config: ProcessingAlgorithmsConfig,
    user_id: int,
    is_public: bool = False,
) -> ExperimentProcessRunInDB:
    """
    创建算法处理运行记录

    Returns:
        新创建的 ExperimentProcessRunInDB 记录
    """
    run_record = ExperimentProcessRunInDB(
        experiment_id=experiment.id,  # type: ignore
        source_file_id=experiment.source_file_id,
        user_id=user_id,
        params_json=algorithms_config.model_dump_json(),
        status=ProcessingStatus.PROCESSING,
        started_at=utc_time(),
        is_public=is_public,
    )
    async_session.add(run_record)
    await async_session.flush()
    return run_record


#########################
# 删
#########################

async def delete_process_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB,
    storage_type: str,
) -> None:
    """
    删除算法处理运行记录及磁盘结果文件
    """
    if run_record.out_path:
        await cleanup_output_dir(run_record.out_path, storage_type)

    # 扣除用户已处理配额
    if run_record.output_size and run_record.output_size > 0:
        user = (
            await async_session.exec(
                select(UserInDB).where(UserInDB.id == run_record.user_id))).first()
        if user is not None:
            user.processed_file_size = max(0, user.processed_file_size - run_record.output_size)
            async_session.add(user)

    await async_session.delete(run_record)
    logger.info("deleted process run id=%s", run_record.id)


async def cleanup_output_dir(out_path: str, storage_type: str) -> None:
    """清理输出目录及其文件"""
    try:
        abs_path = Path(out_path)
        if storage_type == "local" and abs_path.exists():
            shutil.rmtree(abs_path)
            logger.debug("cleaned up run directory path=%s", abs_path)
        elif storage_type == "oss":
            await OssStorage.delete_file(out_path)
    except OSError as e:
        logger.warning("failed to cleanup output path=%s error=%s", out_path, e)


#########################
# 改
#########################

async def mark_process_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB,
    *,
    status: str = ProcessingStatus.COMPLETED,
    out_path: str | None = None,
    output_size: int | None = None,
    error_message: str | None = None,
) -> None:
    """更新算法处理运行记录状态"""
    run_record.status = status
    run_record.finished_at = utc_time()

    run_record.out_path = out_path
    run_record.output_size = output_size
    run_record.error_message = error_message

    async_session.add(run_record)


#########################
# 查
#########################

async def get_process_run_by_id(
    async_session: AsyncSession,
    run_id: int,
) -> ExperimentProcessRunInDB | None:
    """根据 run ID 查询记录"""
    return (
        await async_session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.id == run_id))).first()


async def get_process_runs_by_experiment_id(
    async_session: AsyncSession,
    experiment_id: int,
) -> Sequence[ExperimentProcessRunInDB]:
    """根据 experiment ID 查询所有 process_run"""
    return (
        await async_session.exec(
            select(ExperimentProcessRunInDB)
            .where(ExperimentProcessRunInDB.experiment_id == experiment_id))).all()


async def list_public_runs(
    async_session: AsyncSession,
    page: int = 1,
    size: int = 10,
) -> tuple[Sequence[ExperimentProcessRunInDB], int]:
    """分页查询所有 is_public=True 且 status=COMPLETED 的 run"""
    base_query = select(ExperimentProcessRunInDB).where(
        ExperimentProcessRunInDB.is_public == True,       # pylint: disable=singleton-comparison
        ExperimentProcessRunInDB.status == ProcessingStatus.COMPLETED,
    )
    total = (await async_session.exec(
        select(func.count()).select_from(base_query.subquery()))).one()  # pylint: disable=not-callable
    rows = (await async_session.exec(
        base_query.offset((page - 1) * size).limit(size))).all()
    return rows, total


async def list_public_runs_by_file_id(
    async_session: AsyncSession,
    file_id: int,
) -> Sequence[dict]:
    """根据源文件 ID 查询所有公开且完成的 run（含源文件名）"""
    result = await async_session.exec(
        select(ExperimentProcessRunInDB, FileInDB.filename)
        .join(FileInDB, ExperimentProcessRunInDB.source_file_id == FileInDB.file_id) # type: ignore
        .where(
            ExperimentProcessRunInDB.source_file_id == file_id,
            ExperimentProcessRunInDB.is_public == True,       # pylint: disable=singleton-comparison
            ExperimentProcessRunInDB.status == ProcessingStatus.COMPLETED,
        )
    )
    return [
        {**row[0].model_dump(), "filename": row[1]}
        for row in result.all()
    ]


async def list_user_runs(
    async_session: AsyncSession,
    user_id: int,
    page: int = 1,
    size: int = 10,
) -> tuple[Sequence[ExperimentProcessRunInDB], int]:
    """分页查询当前用户的所有 run"""
    base_query = select(ExperimentProcessRunInDB).where(
        ExperimentProcessRunInDB.user_id == user_id,
    )
    total = (await async_session.exec(
        select(func.count()).select_from(base_query.subquery()))).one()  # pylint: disable=not-callable
    rows = (await async_session.exec(
        base_query.offset((page - 1) * size).limit(size))).all()
    return rows, total


async def get_user_runs(
    async_session: AsyncSession,
    user_id: int,
) -> Sequence[dict]:
    """查询当前用户的所有 run（无分页，含源文件名）"""
    result = await async_session.exec(
        select(ExperimentProcessRunInDB, FileInDB.filename)
        .join(FileInDB, ExperimentProcessRunInDB.source_file_id == FileInDB.file_id) # type: ignore
        .where(
            ExperimentProcessRunInDB.user_id == user_id,
        )
    )
    return [
        {**row[0].model_dump(), "filename": row[1]}
        for row in result.all()
    ]


#########################
# 校验
#########################

async def validate_process_request(
    current_user: UserInDB,
    algorithms_config: ProcessingAlgorithmsConfig,
) -> bool:
    """
    纯校验，不查 DB。校验通过返回 True。

    校验链：
    1. 用户处理配额未超限
    2. 算法配置至少选中一个算法
    """
    if not check_processing_quota(current_user):
        logger.warning("processing quota exceeded user=%s", current_user.id)
        return False

    if validate_algorithms_config(algorithms_config) is None:
        logger.warning("no algorithm selected user=%s", current_user.id)
        return False

    return True


async def get_origin_for_processing(
    async_session: AsyncSession,
    file_id: int,
    current_user: UserInDB,
) -> tuple[OriginImzMLInDB, str]:
    """
    校验文件归属 + 获取就绪的 origin 记录。

    Returns:
        (origin, filename)

    Raises:
        invalid_process_request_exception
    """
    file_record = (
        await async_session.exec(
            select(FileInDB).where(FileInDB.file_id == file_id)
        )
    ).first()
    if not file_record:
        logger.warning("file not found file_id=%s user=%s", file_id, current_user.id)
        raise invalid_process_request_exception
    if not await assert_file_owner(async_session, file_record, current_user):
        logger.warning("file permission denied file_id=%s user=%s", file_id, current_user.id)
        raise invalid_process_request_exception

    origin = await get_origin_imzml_by_file_id(async_session, file_id)
    if not origin:
        logger.warning("origin not found file_id=%s", file_id)
        raise invalid_process_request_exception
    if not assert_origin_ready(origin):
        logger.warning("origin not ready file_id=%s status=%s", file_id, origin.status)
        raise invalid_process_request_exception

    return origin, file_record.filename


async def get_storage_type_by_source_file_id(
    async_session: AsyncSession,
    source_file_id: int,
) -> str:
    """根据 source_file_id 查询存储类型，默认返回 local"""
    result = await async_session.exec(
        select(FileInDB).where(FileInDB.file_id == source_file_id)
    )
    file_record = result.first()
    return file_record.storage_type if file_record else "local"


def cleanup_output_files(run_id: int) -> None:
    """清理 run 输出目录及文件（磁盘操作，无 DB 依赖）"""
    output_dir = create_run_output_dir(run_id)
    if output_dir.exists():
        shutil.rmtree(output_dir)
        logger.debug("cleaned up run output dir path=%s", output_dir)


async def rollback_process_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB | None,
) -> None:
    """
    算法处理运行失败时的统一回滚

    1. 回滚当前 DB 事务
    2. 清理磁盘上的输出目录
    3. 开新 session 删除 PROCESSING 状态的 run 记录
    """
    if run_record is None:
        await async_session.rollback()
        return

    _run_id = run_record.id

    await async_session.rollback()

    if _run_id is not None:
        cleanup_output_files(_run_id)

    if _run_id is not None:
        async with AsyncSession(get_async_engine()) as cleanup_session:
            record = await get_process_run_by_id(cleanup_session, _run_id)
            if record and record.status == ProcessingStatus.PROCESSING:
                await cleanup_session.delete(record)
                await cleanup_session.commit()
                logger.info("rolled back process run run_id=%s", _run_id)


#########################
# zarr 文件夹访问
#########################

async def assert_zarr_folder_access(
    async_session: AsyncSession,
    run_id: int,
    current_user: UserInDB,
) -> ExperimentProcessRunInDB:
    """
    校验 zarr 文件夹访问权限。

    校验链：
    1. run 记录存在
    2. 权限：管理员 / 公开记录 / 记录所有者
    3. 状态必须为 COMPLETED
    4. 输出路径不为空

    Returns:
        通过校验的 run_record

    Raises:
        process_run_not_found_exception
        process_run_permission_denied_exception
        process_run_not_completed_exception
        zarr_folder_not_found_exception
    """

    # 1. 查询 run 记录
    run_record = await get_process_run_by_id(async_session, run_id)
    if not run_record:
        raise process_run_not_found_exception

    # 2. 权限校验
    is_authorized = (
        current_user.identity == "admin"
        or run_record.is_public
        or run_record.user_id == current_user.id
    )
    if not is_authorized:
        raise process_run_permission_denied_exception

    # 3. 状态校验
    if run_record.status != ProcessingStatus.COMPLETED:
        raise process_run_not_completed_exception

    # 4. 输出路径校验
    if not run_record.out_path:
        raise zarr_folder_not_found_exception

    return run_record
