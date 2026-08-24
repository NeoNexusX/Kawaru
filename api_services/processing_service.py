#########################
# Processing Service - 业务逻辑层
# 职责：算法处理运行记录的增删改查与 OSS 结果清理。无 FastAPI 依赖。
#########################
from __future__ import annotations

from typing import Sequence

from sqlmodel import select, func, col
from sqlmodel.ext.asyncio.session import AsyncSession

from database.db import get_async_engine
from database.atomic_ops import atomic_decrement
from models.processing_models import (
    ExperimentProcessRunInDB,
    ProcessingAlgorithmsConfig,
    ProcessingStatus,
    RAW_CONVERT_MARKER,
)
from models.experiment_models import OriginImzMLInDB
from models.file_models import FileInDB
from models.user_models import UserInDB
from api_services.file_service import assert_file_owner
from api_services.experiment_service import get_origin_imzml_by_file_id
from api_helpers.processing_helper import (
    assert_origin_ready,
    check_processing_quota,
    validate_algorithms_config,
)
from storage.oss.oss_storage import OssStorage
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
    )
    async_session.add(run_record)
    await async_session.flush()
    return run_record


async def create_raw_convert_run(
    file_id: int,
    user_id: int,
    experiment_id: int,
    async_session: AsyncSession,
) -> tuple[ExperimentProcessRunInDB, bool]:
    """
    创建 raw_convert 类型的处理 run（无算法，直转 spectra_zarr）

    防重复：若同文件已有 processing 状态的 raw_convert run，直接返回已有 run（不限用户）。

    Returns:
        (run_record, is_existing) — is_existing=True 表示返回的是已有记录（防重复命中）
    """
    existing = (
        await async_session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.source_file_id == file_id,
                ExperimentProcessRunInDB.params_json == RAW_CONVERT_MARKER,
                ExperimentProcessRunInDB.status == ProcessingStatus.PROCESSING,
            )
        )
    ).first()
    if existing:
        logger.info("raw_convert run already exists for file_id=%s run_id=%s",
                     file_id, existing.id)
        return existing, True

    run = ExperimentProcessRunInDB(
        source_file_id=file_id,
        user_id=user_id,
        experiment_id=experiment_id,
        params_json=RAW_CONVERT_MARKER,
        status=ProcessingStatus.PROCESSING,
    )
    async_session.add(run)
    await async_session.flush()
    return run, False


#########################
# 删
#########################

async def delete_process_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB,
) -> None:
    """
    删除算法处理运行记录及磁盘结果文件

    并发安全：使用原子 SQL UPDATE 递减用户已处理配额，避免 read-modify-write
    在并发场景（同一用户并行删除多条 run 记录）下的丢失更新问题。
    """
    if run_record.out_path:
        await cleanup_output_dir(run_record.out_path)

    # 聚类结果写回 out_path 内部，随 out_path 一并清理，无需单独处理

    # 如果删除的是 raw_convert 直转 run（无算法处理），清除文件表的 default_run_id
    if run_record.params_json == RAW_CONVERT_MARKER:
        file_record = (
            await async_session.exec(
                select(FileInDB).where(FileInDB.file_id == run_record.source_file_id)
            )
        ).first()
        if file_record and file_record.default_run_id == run_record.id:
            file_record.default_run_id = None
            async_session.add(file_record)
            logger.info(
                "cleared default_run_id for file_id=%s (deleting raw_convert run_id=%s)",
                run_record.source_file_id, run_record.id,
            )

    # 扣除用户已处理配额（原子操作，防止并发丢失更新；GREATEST 保证不低于 0）
    if run_record.output_size and run_record.output_size > 0 and run_record.user_id is not None:
        await atomic_decrement(
            async_session, UserInDB, run_record.user_id,
            "processed_file_size", run_record.output_size,
        )

    await async_session.delete(run_record)

    logger.info("deleted process run id=%s", run_record.id)


async def cleanup_output_dir(out_path: str, storage_type: str = "oss") -> None:
    """清理 OSS 输出目录；storage_type 参数为后续扩展本地存储而保留。"""
    if storage_type != "oss":
        raise ValueError(f"invalid storage type: {storage_type}")
    try:
        await OssStorage.delete_file(out_path)
    except Exception as e:
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


async def mark_clustering_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB,
    *,
    status: str = ProcessingStatus.COMPLETED,
    error_message: str | None = None,
) -> None:
    """更新聚类分析运行状态（聚类结果写回 out_path，不单独记录路径/大小）"""
    run_record.clustering_status = status
    if error_message:
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


async def get_user_runs(
    async_session: AsyncSession,
    user_id: int,
    page: int = 1,
    size: int = 10,
) -> tuple[Sequence[dict], int]:
    """分页查询当前用户的所有 run（含源文件名）"""
    base_query = (
        select(ExperimentProcessRunInDB, FileInDB.filename)
        .join(FileInDB, ExperimentProcessRunInDB.source_file_id == FileInDB.file_id)  # type:ignore
        .where(ExperimentProcessRunInDB.user_id == user_id)
    )

    total = (await async_session.exec(
        select(func.count()).select_from(base_query.subquery()))).one()  # pylint: disable=not-callable
    offset = (page - 1) * size
    rows = (await async_session.exec(
        base_query
        .order_by(col(ExperimentProcessRunInDB.created_at).desc(), col(ExperimentProcessRunInDB.id).desc()) #pylint: disable=no-member
        .offset(offset)
        .limit(size)
    )).all()
    return [
        {**row[0].model_dump(), "filename": row[1]}
        for row in rows
    ], total


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


async def validate_clustering_request(
    async_session: AsyncSession,
    run_id: int,
    current_user: UserInDB,
) -> ExperimentProcessRunInDB:
    """
    校验聚类分析请求，返回通过校验的 run_record。

    校验链：
    1. run 记录存在
    2. 权限：管理员或记录所有者
    3. 算法处理已完成
    4. 算法输出路径不为空
    5. 聚类未在处理中

    Raises:
        invalid_process_request_exception
    """
    run_record = await get_process_run_by_id(async_session, run_id)
    if not run_record:
        logger.warning("run not found for clustering run_id=%s", run_id)
        raise invalid_process_request_exception

    if not (current_user.identity == "admin" or run_record.user_id == current_user.id):
        logger.warning("clustering permission denied run_id=%s user=%s", run_id, current_user.id)
        raise invalid_process_request_exception

    if run_record.status != ProcessingStatus.COMPLETED:
        logger.warning("clustering run not completed run_id=%s status=%s", run_id, run_record.status)
        raise invalid_process_request_exception

    if not run_record.out_path:
        logger.warning("clustering run out_path empty run_id=%s", run_id)
        raise invalid_process_request_exception

    if run_record.clustering_status == ProcessingStatus.PROCESSING:
        logger.info("clustering already in progress run_id=%s", run_id)
        return run_record

    return run_record


async def get_origin_for_processing(
    async_session: AsyncSession,
    file_id: int,
    current_user: UserInDB,
    allow_public: bool = False,
) -> tuple[OriginImzMLInDB, str, dict, int | None]:
    """
    校验文件归属 + 获取就绪的 origin 记录。

    Args:
        allow_public: 若为 True 且文件已公开，跳过所有权校验（用于公开文件的 raw_convert）

    Returns:
        (origin, filename, metadata, default_run_id)

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

    skip_ownership = allow_public and file_record.is_public
    if not skip_ownership:
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

    # 提取需要写入 Zarr 的元数据字段
    metadata = {
        "filename": file_record.filename,                       # Zarr: 原始文件名
        "ionisation_source": file_record.ionisation_source or "", # Zarr: 离子源类型（如 MALDI、ESI）
        "analyzer": file_record.analyzer or "",                 # Zarr: 质量分析器类型（如 TOF、Orbitrap）
        "pixel_size_horizontal": file_record.pixel_size_horizontal or 0.0,  # Zarr: 水平像素尺寸 (μm)
        "pixel_size_vertical": file_record.pixel_size_vertical or 0.0,      # Zarr: 垂直像素尺寸 (μm)
        "polarity": file_record.polarity or "",                 # Zarr: 极性（positive / negative）
    }
    return origin, file_record.filename, metadata, file_record.default_run_id


async def rollback_process_run(
    async_session: AsyncSession,
    run_record: ExperimentProcessRunInDB | None,
) -> None:
    """
    算法处理运行失败时的统一回滚

    1. 回滚当前 DB 事务
    2. 开新 session 删除 PROCESSING 状态的 run 记录

    FC 未正常回调时无法从 run 记录获得可靠对象键，未持久化的 OSS 输出由
    bucket 生命周期规则清理。
    """
    if run_record is None:
        await async_session.rollback()
        return

    _run_id = run_record.id

    await async_session.rollback()

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

    聚类结果写回 out_path 内部，统一按算法结果 zarr 校验。

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

    # 2. 权限校验：管理员 / 记录所有者 / 公开文件的 raw_convert run
    is_authorized = current_user.identity == "admin" or run_record.user_id == current_user.id
    if not is_authorized and run_record.params_json == RAW_CONVERT_MARKER:
        # 公开文件的 raw_convert run 允许任何登录用户查看
        file_record = (
            await async_session.exec(
                select(FileInDB).where(FileInDB.file_id == run_record.source_file_id)
            )
        ).first()
        if file_record and file_record.is_public:
            is_authorized = True

    if not is_authorized:
        raise process_run_permission_denied_exception

    # 3. 状态校验
    if run_record.status != ProcessingStatus.COMPLETED:
        raise process_run_not_completed_exception

    # 4. 输出路径校验
    if not run_record.out_path:
        raise zarr_folder_not_found_exception

    return run_record
