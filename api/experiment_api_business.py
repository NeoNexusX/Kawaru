from __future__ import annotations

from pathlib import Path

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from models.experiment_models import ExperimentImzMLInDB, ExperimentStatus
from models.file_models import FileInDB
from api.file_api_business import get_file_record_by_id
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from tools.logger import get_logger

logger = get_logger("api.experiment_api_business")

async def is_file_uploaded(session: AsyncSession, file_record) -> bool:
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
    await async_session.commit()
    await async_session.refresh(experiment_record)

    return experiment_record

async def delete_source_file_and_clear_path(
        async_session: AsyncSession,
        source: int | FileInDB
        ):
    """
    删除源文件并清空数据库中的 path 字段
    Args:
        source_file: 文件记录对象，必须具有 path 属性
    """
    source_file = await get_file_record_by_id(source,async_session) if isinstance(source, int) else source

    if source_file and source_file.path:
        # 删除物理文件
        Path(source_file.path).unlink(missing_ok=True)
        # 将 path 置为占位常量
        source_file.path = SOURCE_FILE_PATH_CLEARED
        async_session.add(source_file)
        logger.info(f"source file deleted and path cleared for file_id={source_file.id}") #type:ignore
    else:
        logger.error("source file not found or path is empty, cannot delete source file")
        raise ValueError("source file not found or path is empty")

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
