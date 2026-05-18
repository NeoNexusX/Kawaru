"""
算法处理运行 — 业务逻辑层
"""
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
from api.file_api_business import assert_file_owner
from api.file_api_helper import delete_multiple_files_by_storage
from api.experiment_api_business import get_origin_imzml_by_file_id
from api.processing_api_helper import (
    assert_origin_ready,
    check_processing_quota,
    create_run_output_dir,
    validate_algorithms_config,
)
from exceptions.experiment_exceptions import invalid_process_request_exception
from tools import utc_time
from tools.logger import get_logger

logger = get_logger("api.processing_api_business")


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

    Args:
        async_session: 数据库会话
        experiment: origin 实验记录
        algorithms_config: 算法配置
        user_id: 提交用户 ID
        is_public: 是否公开运行结果

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

    Args:
        async_session: 数据库会话
        run_record: 要删除的运行记录
        storage_type: 存储类型（local/oss）
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
            await delete_multiple_files_by_storage([out_path], storage_type)
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
) -> Sequence[ExperimentProcessRunInDB]:
    """根据源文件 ID 查询所有公开且完成的 run"""
    return (await async_session.exec(
        select(ExperimentProcessRunInDB).where(
            ExperimentProcessRunInDB.source_file_id == file_id,
            ExperimentProcessRunInDB.is_public == True,       # pylint: disable=singleton-comparison
            ExperimentProcessRunInDB.status == ProcessingStatus.COMPLETED,
        )
    )).all()


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
) -> Sequence[ExperimentProcessRunInDB]:
    """查询当前用户的所有 run（无分页）"""
    return (await async_session.exec(
        select(ExperimentProcessRunInDB).where(
            ExperimentProcessRunInDB.user_id == user_id,
        )
    )).all()


#########################
# 校验
#########################

async def validate_process_request(
    async_session: AsyncSession,
    file_id: int,
    algorithms_config: ProcessingAlgorithmsConfig,
    current_user: UserInDB,
) -> OriginImzMLInDB:
    """
    整合创建流程的前置校验，全部通过则返回 origin

    校验链：
    1. 用户处理配额未超限
    2. 文件存在且属于当前用户
    3. origin 存在且状态为 COMPLETED
    4. 算法配置至少选中一个算法

    Raises:
        invalid_process_request_exception: 任一校验失败
    """
    if not check_processing_quota(current_user):
        raise invalid_process_request_exception

    file_record = (
        await async_session.exec(
            select(FileInDB).where(FileInDB.file_id == file_id))).first()
    if not file_record or not await assert_file_owner(async_session, file_record, current_user):
        raise invalid_process_request_exception

    origin = await get_origin_imzml_by_file_id(async_session, file_id)
    if not origin or not assert_origin_ready(origin):
        raise invalid_process_request_exception

    if validate_algorithms_config(algorithms_config) is None:
        raise invalid_process_request_exception

    return origin


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
