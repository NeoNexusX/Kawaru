#########################
# Processing Service - 业务逻辑层
# 职责：算法处理运行记录的增删改查与 OSS 结果清理。无 FastAPI 依赖。
#########################
from __future__ import annotations

from typing import Any, Sequence

from sqlmodel import select, func, col, or_
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
from models.filter_models import RunFilter
from models.file_models import FileInDB, FileStatus
from models.user_models import UserInDB
from api_services.file_service import assert_file_owner, get_file_record_by_public_id
from api_services.experiment_service import get_origin_imzml_by_file_id
from api_helpers.processing_helper import (
    assert_origin_ready,
    check_processing_quota,
    validate_algorithms_config,
)
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
# 删除流程已切换到删除状态位架构（docs/zh/删除状态位改造方案.md 场景 2）：
# 登记（mark_run_for_deletion）在 composite 层，执行/收尾在 delete_objects_task。
# 旧 delete_process_run 已退役；文件级删除同样走删除状态位（run 输出目录进入文件删除快照）。
#########################


async def refund_processed_quota(
    async_session: AsyncSession,
    user_id: int,
    output_size: int,
) -> None:
    """
    退还单个 run 占用的用户处理配额

    原子操作（UPDATE SET processed_file_size = GREATEST(processed_file_size - N, 0)），
    防止并发丢失更新；GREATEST 保证结果不低于 0。
    """
    await atomic_decrement(
        async_session, UserInDB, user_id,
        "processed_file_size", output_size,
    )


async def refund_processed_quota_for_file(
    async_session: AsyncSession,
    file_id: int,
) -> int:
    """
    退还指定文件所有 process_run 占用的处理配额（文件级删除路径专用）。

    finalize_file_deletion / cleanup_failed_experiments 删除文件记录时，
    run 行由 FK 级联删除、out_path 物理清理；若不在删除前按行退还，
    user_id / output_size 随行消失，processed_file_size 将永久虚占。
    必须在 session.delete(file_record) 之前调用。

    Returns:
        实际退还配额的 run 数量
    """
    runs = (await async_session.exec(
        select(ExperimentProcessRunInDB.user_id, ExperimentProcessRunInDB.output_size).where(
            ExperimentProcessRunInDB.source_file_id == file_id
        )
    )).all()

    refunded = 0
    for run_user_id, output_size in runs:
        if run_user_id is None or not output_size or output_size <= 0:
            continue
        await refund_processed_quota(async_session, run_user_id, output_size)
        refunded += 1

    if refunded:
        logger.info("refunded processed quota for file_id=%s runs=%s", file_id, refunded)

    return refunded


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
    # 无条件写入（成功时为 None 清空），避免重跑成功后旧错误残留
    run_record.clustering_error_message = error_message
    if status == ProcessingStatus.PROCESSING:
        # 聚类由用户在处理完成后手动触发，created_at/finished_at 均不能代表聚类开始时间
        run_record.clustering_started_at = utc_time()

    async_session.add(run_record)


#########################
# 查
#########################

async def get_process_run_by_id(
    async_session: AsyncSession,
    run_id: int,
    *,
    include_deleting: bool = False,
) -> ExperimentProcessRunInDB | None:
    """根据 run ID 查询记录。

    默认拒绝删除流程中的 run：DELETING / DELETE_FAILED 一律按不存在处理
    （返回 None，调用方已有的 not-found 分支天然拦截），删除登记与回调
    检查用 include_deleting=True 显式放行。
    """
    record = (
        await async_session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.id == run_id))).first()

    if (not include_deleting
            and record is not None
            and record.status in (ProcessingStatus.DELETING, ProcessingStatus.DELETE_FAILED)):
        logger.warning(
            "run is being deleted, treat as not found run_id=%s status=%s",
            run_id, record.status,
        )
        return None

    return record


def build_run_filters(filters: RunFilter) -> list[Any]:
    """RunFilter → SQL 谓词列表（同一字段多值 OR，即任一关键词命中）

    两个字段均为模糊匹配，空值不产生谓词（归一化已在 Filter 模型内完成）。
    """
    clauses: list[Any] = []
    fuzzy_fields = (
        (FileInDB.filename, filters.filename),
        (ExperimentProcessRunInDB.params_json, filters.params),
    )
    for column, values in fuzzy_fields:
        if values:
            clauses.append(or_(*[col(column).like(f"%{value}%") for value in values]))
    return clauses


async def get_user_runs(
    async_session: AsyncSession,
    user_id: int,
    filters: RunFilter,
    page: int = 1,
    size: int = 10,
) -> tuple[Sequence[dict], int]:
    """分页查询当前用户的所有 run（含源文件名与源文件 public_id），支持文件名/参数模糊搜索。

    删除流程中的 run（DELETING / DELETE_FAILED）不展示：删除登记后立即从
    列表消失，接口语义与文件删除一致（前端零改动）。
    """
    base_query = (
        select(ExperimentProcessRunInDB, FileInDB.filename, FileInDB.public_id)
        .join(FileInDB, ExperimentProcessRunInDB.source_file_id == FileInDB.file_id)  # type:ignore
        .where(ExperimentProcessRunInDB.user_id == user_id)
        .where(ExperimentProcessRunInDB.status.notin_(( # pylint: disable=no-member # type: ignore
            ProcessingStatus.DELETING, ProcessingStatus.DELETE_FAILED,
        )))
    )
    # 模糊过滤（file_api 惯例：传了才匹配，没传恒真不过滤；同字段多值 OR）；
    # 直转 run 的 params_json 为 __RAW_CONVERT__ 标记，天然参与 LIKE 匹配
    for clause in build_run_filters(filters):
        base_query = base_query.where(clause)

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
        {**row[0].model_dump(), "filename": row[1], "source_file_public_id": row[2]}
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
) -> tuple[ExperimentProcessRunInDB, str]:
    """
    校验聚类分析请求，返回通过校验的 run_record 及源文件 public_id。

    校验链：
    1. run 记录存在
    2. 权限：管理员或记录所有者
    3. 算法处理已完成
    4. 算法输出路径不为空
    5. 聚类未在处理中

    Returns:
        (run_record, source_file_public_id)

    Raises:
        invalid_process_request_exception
    """
    run_record = await get_process_run_by_id(async_session, run_id)
    if not run_record:
        logger.warning("run not found for clustering run_id=%s", run_id)
        raise invalid_process_request_exception

    # 源文件状态与 public_id 同一次查询取出（public_id 供响应的 source_file_public_id）
    file_row = (await async_session.exec(
        select(FileInDB.status, FileInDB.public_id).where(FileInDB.file_id == run_record.source_file_id)
    )).first()
    if file_row is None:
        # 外键级联删除保证 run 存在时源文件必然存在，此处仅防御
        logger.warning("source file not found for clustering run_id=%s file_id=%s",
                       run_id, run_record.source_file_id)
        raise invalid_process_request_exception
    file_status, source_file_public_id = file_row

    # 删除流程中的文件不接受聚类请求：聚类结果写回源 zarr，而快照删除可能
    # 已清掉该目录，迟到的 FC 写入会成为孤儿对象
    if file_status in (FileStatus.DELETING, FileStatus.DELETE_FAILED):
        logger.warning("file is being deleted, reject clustering run_id=%s file_id=%s",
                       run_id, run_record.source_file_id)
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
        return run_record, source_file_public_id

    return run_record, source_file_public_id


async def get_origin_for_processing(
    async_session: AsyncSession,
    file_public_id: str,
    current_user: UserInDB,
    allow_public: bool = False,
) -> tuple[OriginImzMLInDB, FileInDB, dict]:
    """
    校验文件归属 + 获取就绪的 origin 记录。

    Args:
        file_public_id: 源文件 public_id（前端不再持有内部 file_id）
        allow_public: 若为 True 且文件已公开，跳过所有权校验（用于公开文件的 raw_convert）

    Returns:
        (origin, file_record, metadata)——文件名、public_id、default_run_id 均从 file_record 取

    Raises:
        file_not_found_exception: 文件不存在或处于删除流程（404）
        invalid_process_request_exception: 无权限、origin 不存在或未就绪
    """
    # 删除流程中的文件不接受新处理请求（按不存在处理）：run 的 out_path 不会
    # 进入删除快照，登记后创建的 run 输出会成为孤儿对象
    file_record = await get_file_record_by_public_id(file_public_id, async_session)
    file_id = file_record.file_id
    assert file_id is not None

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
    return origin, file_record, metadata


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
