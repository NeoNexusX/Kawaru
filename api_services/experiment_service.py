#########################
# Experiment Service - 业务逻辑层
# 职责：origin 实验记录的增删改查与物理文件清理。无 FastAPI 依赖。
#########################
from __future__ import annotations

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from tools.logger import get_logger

logger = get_logger("api_services.experiment_service")


#########################
# origin  - 增
#########################
async def create_origin_record(async_session: AsyncSession,
                                   experiment_name: str,
                                   source_file_id: int) -> OriginImzMLInDB:
    """
    创建 origin 记录并返回

    Return:
        OriginImzMLInDB
    """
    origin_record = OriginImzMLInDB(
        experiment_name=experiment_name,
        source_file_id=source_file_id,
        status=ExperimentStatus.PROCESSING,
    )
    async_session.add(origin_record)
    await async_session.flush()

    return origin_record


#########################
# origin  - 改
#########################

async def update_origin(
    async_session: AsyncSession,
    origin_record: OriginImzMLInDB,
    *,
    status: ExperimentStatus | None = None,
    imzml_path: str | None = None,
    imzml_size: int | None = None,
    ibd_path: str | None = None,
    ibd_size: int | None = None,
    error_message: str | None = None,
) -> None:
    """
    更新 origin 记录的若干字段。

    仅传入需要更新的字段（None 表示保持原值），修改内存对象并加入会话，
    不 flush / commit，由调用方控制事务边界（与 update_file_record 一致）。
    """
    updates = {
        "status": status,
        "imzml_path": imzml_path,
        "imzml_size": imzml_size,
        "ibd_path": ibd_path,
        "ibd_size": ibd_size,
        "error_message": error_message,
    }
    for field, value in updates.items():
        if value is not None:
            setattr(origin_record, field, value)
    async_session.add(origin_record)


#########################
# origin  - 查
#########################
async def get_origin_imzml_by_file_id(
    async_session: AsyncSession,
    file_id: int
) -> OriginImzMLInDB | None:
    """
    根据文件ID查询 imzml origin 记录
    """
    return (
        await async_session.exec(
            select(OriginImzMLInDB).where(
                OriginImzMLInDB.source_file_id == file_id
            )
        )
    ).first()
