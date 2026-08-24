#########################
# Composite Service - 编排层
# 职责：跨 file / experiment / processing 三个 service 的编排逻辑。
#
# 为什么需要这一层：
# file_service 与 processing_service 都在顶层 import experiment_service，
# experiment_service 处于依赖最底层，它反向引用 file_service / processing_service
# 会形成循环，原先靠函数内延迟导入（import-outside-toplevel）绕开。
# 把这类"同时操作多个 service 记录"的编排函数上移到本层后，依赖方向始终
# 自上而下（composite -> file/experiment/processing），不再有反向引用，可全部
# 顶层 import，消除延迟导入。
#
# 约定：本层只做编排（组合多个 service 调用 + 事务控制），不新增单表 CRUD 逻辑；
#      单表的增删改查仍归各自的 service。
#########################
from __future__ import annotations

from pathlib import Path

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from storage.oss.oss_storage import OssStorage
from api_services.file_service import (
    get_file_record_by_id,
    update_file_record,
)
from api_services.experiment_service import (
    get_origin_imzml_by_file_id,
    update_origin,
)
from api_services.processing_service import cleanup_output_dir
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.processing_models import ExperimentProcessRunInDB
from models.file_models import FileInDB, FileStatus

from tools.logger import get_logger

logger = get_logger("api_services.composite_service")


async def delete_origin_files(
    session: AsyncSession,
    file_id: int,
    storage_type: str,
) -> None:
    """
    删除与文件关联的 origin 文件(imzML、ibd 等)及数据库记录

    用于用户主动删除文件时,完整删除 origin 相关的所有内容

    处理逻辑:
    1. 查询与 source_file_id 关联的 origin 记录
    2. 清理所有 process_run 的输出目录（DB 记录由 FK CASCADE 自动删除）
    3. 删除 origin 相关的物理文件(imzml_path、ibd_path)及目录
    4. origin 数据库记录由 file 级联删除自动处理

    Raises:
        OSError: 物理文件删除失败时
    """
    if storage_type != "oss":
        raise ValueError(f"invalid storage type: {storage_type}")

    # 查询 imzML origin 记录
    origin = await get_origin_imzml_by_file_id(session, file_id)

    if not origin:
        logger.debug("no origin found for file_id=%s", file_id)
        return

    # 清理所有 process_run 的结果文件（DB 记录由 FK CASCADE 自动删除）
    process_runs = (await session.exec(
        select(ExperimentProcessRunInDB).where(ExperimentProcessRunInDB.experiment_id == origin.id))).all()

    for run in process_runs:
        if run.out_path:
            await cleanup_output_dir(run.out_path, storage_type)

    # 收集需要删除的 origin 文件及目录，一次 celery group 统一删除
    paths_to_delete = []
    if origin.imzml_path:
        paths_to_delete.append(origin.imzml_path)
    if origin.ibd_path:
        paths_to_delete.append(origin.ibd_path)
    if origin.imzml_path:
        paths_to_delete.append(f"{str(Path(origin.imzml_path).parent)}/")

    if paths_to_delete:
        if await OssStorage.delete_file(paths_to_delete):
            logger.info("deleted origin files for file_id=%s paths=%s", file_id, paths_to_delete)


async def failed_mark_origin_record_clean(
    async_session: AsyncSession,
    origin_record: OriginImzMLInDB | None,
    source_file: FileInDB | None,
    e: None | Exception = None,
    f_status: FileStatus = FileStatus.FAILED,
    o_status: ExperimentStatus = ExperimentStatus.FAILED,
) -> None:
    """
    标记对应的数据为失败状态，并不清理文件。

    同时更新 file 与 origin 两条记录为 FAILED，并在同一事务内提交、刷新。
    跨 file_service / experiment_service 的编排逻辑集中在此，避免在
    experiment_service 内反向 import file_service 造成循环依赖。

    当下架构中，三个 OSS 路径在成功分支的第一次 commit 时已落库；
    即使后续步骤（如打 persistent tag）失败触发 rollback，已提交的路径也不会被抹掉。
    因此兜底只需标 FAILED，无需再传路径参数，由 cleanup_failed_experiments
    按 DB 中已落库的路径删除 OSS 对象，避免孤儿。
    """
    # update file status to failed
    assert async_session is not None
    if source_file:
        await update_file_record(async_session, source_file, status=f_status)
    # update origin status to failed
    if origin_record:
        await update_origin(async_session, origin_record, status=o_status,
                            error_message=str(e))


async def delete_file_completely(
    session: AsyncSession,
    file_id: int,
    storage_type: str,
    current_user_id: int | None = None,
) -> None:
    """
    删除一个文件的全部资源（OSS 对象 + DB 记录）。

    编排逻辑：
    1. 清理 process_run 输出目录（无 origin 记录时需手动处理）
    2. 删除 origin 相关文件（imzML/ibd + 目录）
    3. 删除图像文件
    4. 删除主 OSS 文件
    5. 删除 DB 记录（级联删除 origin、UserFileLink 等）

    注意：不处理用户关联解除和所有权校验，由调用方负责。

    Raises:
        ValueError: 文件不存在或不支持的存储类型
        OSError: 物理文件删除失败时
    """
    if storage_type != "oss":
        raise ValueError(f"invalid storage type: {storage_type}")

    file_record = await get_file_record_by_id(file_id, session)

    # 1. 清理 process_run 输出目录（无 origin 记录时需手动处理，有 origin 时由 delete_origin_files 处理）
    origin = await get_origin_imzml_by_file_id(session, file_id)
    if not origin:
        run_out_paths = (await session.exec(
            select(ExperimentProcessRunInDB.out_path).where(
                ExperimentProcessRunInDB.source_file_id == file_id
            )
        )).all()
        for out_path in run_out_paths:
            if out_path:
                await cleanup_output_dir(out_path, storage_type)

    # 2. 删除 origin 相关文件（imzML/ibd + 目录）
    if file_record.experiment_type == "imzML":
        await delete_origin_files(session, file_id, storage_type)

    # 3. 删除图像文件
    if file_record.image_path:
        await OssStorage.delete_file(file_record.image_path)
        logger.info("deleted image file file_id=%s image_path=%s", file_id, file_record.image_path)

    # 4. 删除主 OSS 文件
    # TODO：删除与正在执行的 packup 仍有竞态，后续统一调整 packup 回调落库流程时处理。
    path = (file_record.path or "").strip()
    if path and path != SOURCE_FILE_PATH_CLEARED:
        ok = await OssStorage.delete_file(path)
        if ok is False:
            raise OSError(f"Failed to delete file: {path}")
    logger.debug("deleted file physical file path=%s file_id=%s", file_record.path, file_id)

    # 5. 删除 DB 记录（级联删除 origin、UserFileLink 等）
    await session.delete(file_record)
    logger.info("deleted file completely file_id=%s user=%s", file_id, current_user_id)
