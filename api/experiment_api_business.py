from __future__ import annotations

from pathlib import Path
import asyncio

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from models.experiment_models import ExperimentImzMLInDB, ExperimentStatus
from models.file_models import FileInDB
from api.file_api_helper import get_file_record_by_id,delete_files_async
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from tools.logger import get_logger

logger = get_logger("api.experiment_api_business")

#########################
# experiment  - 增
#########################
async def create_experiment_record(async_session: AsyncSession,
                                   experiment_name: str,
                                   source_file_id: int) -> ExperimentImzMLInDB:
    """
    创建实验记录并返回

    Return:
        ExperimentImzMLInDB
    """
    experiment_record = ExperimentImzMLInDB(
        experiment_name=experiment_name,
        source_file_id=source_file_id,
        status=ExperimentStatus.PROCESSING,
    )
    async_session.add(experiment_record)
    await async_session.flush()

    return experiment_record


#########################
# experiment  - 删
#########################
async def delete_experiment_files(
    session: AsyncSession,
    file_id: int,
) -> None:
    """
    删除与文件关联的实验文件(imzML、ibd 等)及数据库记录

    用于用户主动删除文件时,完整删除实验相关的所有内容

    处理逻辑:
    1. 查询与 source_file_id 关联的实验记录
    2. 删除实验相关的物理文件(imzml_path、ibd_path)
    3. 删除实验目录(如果为空)
    4. 删除实验数据库记录

    Raises:
        OSError: 物理文件删除失败时
    """
    # 查询 imzML 实验记录
    experiment = await get_imzml_experiment_by_file_id(session, file_id)

    if not experiment:
        logger.debug("no experiment found for file_id=%s", file_id)
        return

    # 收集需要删除的文件
    files_to_delete = []
    if experiment.imzml_path:
        files_to_delete.append(experiment.imzml_path)
    if experiment.ibd_path:
        files_to_delete.append(experiment.ibd_path)

    # 批量删除实验文件
    if files_to_delete:
        deleted_count = await delete_files_async(files_to_delete)
        logger.info("deleted %s experiment files for file_id=%s", deleted_count, file_id)

    # 删除实验目录(如果为空)
    if experiment.imzml_path:
        experiment_dir = Path(experiment.imzml_path).parent

        def _rmdir_if_empty(d: Path) -> None:
            if d.exists() and not any(d.iterdir()):
                d.rmdir()

        await asyncio.to_thread(_rmdir_if_empty, experiment_dir)
        logger.debug("cleaned up experiment directory path=%s", experiment_dir)

    # # 删除实验数据库记录
    # await session.delete(experiment)
    # logger.info(
    #     "deleted experiment record exp_id=%s file_id=%s", experiment.id, file_id
    # )


async def delete_source_file_and_clear_path(
        async_session: AsyncSession,
        source: int | FileInDB,
        is_successful: bool = True
        ):
    """
    删除源文件并清空数据库中的 path 字段
    Args:
        async_session: 数据库会话
        source_file: 文件记录对象，必须具有 path 属性
        is_successful: 是否成功处理实验文件，成功则保留占位符，失败则清空 path
    """
    source_file = await get_file_record_by_id(source,async_session) if isinstance(source, int) else source

    if source_file and source_file.path:
        # 删除物理文件
        Path(source_file.path).unlink(missing_ok=True)
        # 如果上传成功将 path 置为占位常量，否则清空path
        if is_successful:
            source_file.path = SOURCE_FILE_PATH_CLEARED
        else:
            source_file.path = ""
        async_session.add(source_file)
        logger.info(f"source file deleted and path cleared for file_id={source_file.file_id}") #type:ignore
    else:
        logger.error("source file not found or path is empty, cannot delete source file")
        raise ValueError("source file not found or path is empty")

#########################
# experiment  - 改
#########################

async def mark_experiment(
    async_session: AsyncSession,
    experiment: ExperimentImzMLInDB | int,
    final_imzml_path: Path | None = None,
    final_ibd_path: Path | None = None,
    error_message: str  = "",
    status = ExperimentStatus.COMPLETED
) -> None:
    """
    标记实验为完成状态，并更新其路径和大小信息
    注意:final_imzml_path 和 final_ibd_path 必须存在且有效，才能标记为完成状态

    """
    experiment_record = await find_experiment_by_id(async_session, experiment) if isinstance(experiment, int) else experiment

    if experiment_record:
        if status == ExperimentStatus.COMPLETED and final_imzml_path and final_ibd_path:
            experiment_record.imzml_path = str(final_imzml_path)
            experiment_record.imzml_size = final_imzml_path.stat().st_size
            experiment_record.ibd_path = str(final_ibd_path)
            experiment_record.ibd_size = final_ibd_path.stat().st_size
        experiment_record.status = status
        experiment_record.error_message = error_message

    else:
        logger.error("experiment record not found or final files do not exist, cannot mark experiment as completed")
        raise ValueError("experiment record not found or final files do not exist")

    async_session.add(experiment_record)


#########################
# experiment  - 查
#########################
async def get_imzml_experiment_by_file_id(
    async_session: AsyncSession,
    file_id: int
) -> ExperimentImzMLInDB | None:
    """
    根据文件ID查询imzml实验记录
    """
    return (
        await async_session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_id
            )
        )
    ).first()


async def valid_file_uploaded(session: AsyncSession, file_record) -> bool:
    """
    判断文件是否真正上传：
    - 对于 experiment_type==imzML,需有对应 experiment 且 status=COMPLETED
    - 其他类型可根据实际业务扩展
    """
    if getattr(file_record, 'experiment_type', None) == 'imzML':
        exp = (await session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_record.file_id,
                ExperimentImzMLInDB.status == ExperimentStatus.COMPLETED
            )
        )).first()
        return exp is not None
    # 其他类型直接视为已上传
    return True


async def find_experiment_by_id(async_session: AsyncSession, experiment_id: int) -> ExperimentImzMLInDB | None:
    """
    根据实验 ID 查找实验记录
    """
    statement = select(ExperimentImzMLInDB).where(ExperimentImzMLInDB.id == experiment_id)
    result = await async_session.exec(statement)
    return result.first()
