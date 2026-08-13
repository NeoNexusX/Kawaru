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
from api_helpers.file_helper import delete_multiple_files_by_storage
from api_services.file_service import update_file_record
from api_services.experiment_service import (
    get_origin_imzml_by_file_id,
    update_origin,
)
from api_services.processing_service import cleanup_output_dir
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
    2. 删除 origin 相关的物理文件(imzml_path、ibd_path)
    3. 删除 origin 目录(如果为空)
    4. 删除 origin 数据库记录由对应的文件记录删除管理

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

    # 收集需要删除的 origin 文件
    files_to_delete = []
    if origin.imzml_path:
        files_to_delete.append(origin.imzml_path)
    if origin.ibd_path:
        files_to_delete.append(origin.ibd_path)

    # 批量删除 origin 文件
    if files_to_delete:
        deleted_count = await delete_multiple_files_by_storage(files_to_delete, storage_type)
        logger.info("deleted %s origin files for file_id=%s", deleted_count, file_id)

    # 删除对应的文件夹
    if origin.imzml_path:
        origin_dir = f"{str(Path(origin.imzml_path).parent)}/"
        if await OssStorage.delete_file(origin_dir):
            logger.debug("cleaned up origin directory path=%s", origin_dir)


async def failed_mark_origin_record_clean(
    async_session: AsyncSession,
    origin_record: OriginImzMLInDB | None,
    source_file: FileInDB | None,
    file_path: None | str = None,
    e: None | Exception = None,
    f_status: FileStatus = FileStatus.FAILED,
    o_status: ExperimentStatus = ExperimentStatus.FAILED,
    imzml_path: None | str = None,
    ibd_path: None | str = None,
) -> None:
    """
    标记对应的数据为失败状态，并不清理文件。

    同时更新 file 与 origin 两条记录为 FAILED，并在同一事务内提交、刷新。
    跨 file_service / experiment_service 的编排逻辑集中在此，避免在
    experiment_service 内反向 import file_service 造成循环依赖。

    三个 OSS 路径（file_path / imzml_path / ibd_path）在失败时也尽量落库：
    回调在 commit 之后打 tag 失败时会先 rollback，把正常流程里赋给 origin 的
    imzml/ibd 路径冲掉；这里显式补写，保证 cleanup_failed_experiments 能按路径
    删除 OSS 对象，避免已打 persistent tag 的对象成为孤儿。
    """
    # update file status to failed
    assert async_session is not None
    if source_file:
        await update_file_record(async_session, source_file, status=f_status, path=file_path)
    # update origin status to failed
    if origin_record:
        await update_origin(async_session, origin_record, status=o_status,
                            error_message=str(e),
                            imzml_path=imzml_path, ibd_path=ibd_path)
