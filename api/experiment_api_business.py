from __future__ import annotations

from pathlib import Path

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from storage_backend.storage import OssStorage
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.processing_models import ExperimentProcessRunInDB
from models.file_models import FileInDB
from api.file_api_helper import get_file_record_by_id, delete_multiple_files_by_storage
from tools.logger import get_logger

logger = get_logger("api.experiment_api_business")

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
# origin  - 删
#########################
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
            from api.processing_api_business import cleanup_output_dir # type: ignore
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

    # 删除 origin 目录(如果为空)(仅在本地存储时需要手动删除目录)
    if origin.imzml_path and storage_type == "local":
        origin_dir = Path(origin.imzml_path).parent
        if origin_dir.exists() and not any(origin_dir.iterdir()):
            origin_dir.rmdir()
            logger.debug("cleaned up origin directory path=%s", origin_dir)
            
    if origin.imzml_path and storage_type == "oss":
        origin_dir = f"{str(Path(origin.imzml_path).parent)}/"
        if await OssStorage.delete_file(origin_dir):
            logger.debug("cleaned up origin directory path=%s", origin_dir)


async def delete_source_file_and_clear_path(
        async_session: AsyncSession,
        source: int | FileInDB,
        ):
    """
    删除源文件并清空数据库中的 path 字段，只用于实验处理失败时的清理
    Args:
        async_session: 数据库会话
        source_file: 文件记录对象，必须具有 path 属性
    """
    source_file = await get_file_record_by_id(source,async_session) if isinstance(source, int) else source

    if source_file and source_file.path:
        # 删除物理文件，根据存储类型选择不同的删除方式
        if source_file.storage_type == "local":
            Path(source_file.path).unlink(missing_ok=True)
        elif source_file.storage_type == "oss":
            await OssStorage.delete_file(source_file.path)
        else:
            logger.error(f"storage_type {source_file.storage_type} not supported")

        # 清空path
        source_file.path = ""
        async_session.add(source_file)
        logger.info(f"source file deleted and path cleared for file_id={source_file.file_id}") #type:ignore

    else:
        logger.error("source file not found or path is empty, cannot delete source file")
        raise ValueError("source file not found or path is empty")


#########################
# origin  - 改
#########################

async def mark_origin(
    async_session: AsyncSession,
    origin: OriginImzMLInDB | int,
    storage_type: str,
    final_imzml_path: Path | str | None = None,
    final_ibd_path: Path | str | None = None,
    error_message: str  = "",
    status = ExperimentStatus.COMPLETED,
) -> None:
    """
    标记 origin 为完成状态，并更新其路径和大小信息
    注意:final_imzml_path 和 final_ibd_path 必须存在且有效，才能标记为完成状态

    """
    origin_record = await find_origin_by_id(async_session, origin) if isinstance(origin, int) else origin

    if not origin_record:
        logger.error("origin record not found or final files do not exist, cannot mark origin as completed")
        raise ValueError("origin record not found or final files do not exist")

    if status == ExperimentStatus.COMPLETED and final_imzml_path and final_ibd_path:
        # 更新路径和大小信息
        imzml_size, ibd_size = None, None
        if storage_type == "local":
            # 确保路径是 Path 类型
            final_imzml_path = Path(final_imzml_path)
            final_ibd_path = Path(final_ibd_path)
            # 如果是本地存储，直接通过属性获取大小
            imzml_size = final_imzml_path.stat().st_size
            ibd_size = final_ibd_path.stat().st_size
        elif storage_type == "oss":
            # 确保路径是 str 类型
            final_imzml_path = str(final_imzml_path)
            final_ibd_path = str(final_ibd_path)
            # 如果是 OSS 存储，路径应该是 OSS 文件路径，需要需要通过 OSS API 获取文件大小
            imzml_size = await OssStorage.get_file_size(final_imzml_path)
            ibd_size = await OssStorage.get_file_size(final_ibd_path)
            if not imzml_size or not ibd_size:
                logger.error("oss file size not found for imzml_path=%s or ibd_path=%s", final_imzml_path, final_ibd_path)
                raise FileNotFoundError("oss file size not found for imzml_path or ibd_path")
        else:
            raise ValueError(f"invalid storage type: {storage_type}")

        origin_record.imzml_path = str(final_imzml_path)
        origin_record.imzml_size = imzml_size
        origin_record.ibd_path = str(final_ibd_path)
        origin_record.ibd_size = ibd_size
    origin_record.status = status
    origin_record.error_message = error_message

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


async def valid_file_uploaded(session: AsyncSession, file_record) -> bool:
    """
    判断文件是否真正上传：
    - 对于 experiment_type==imzML,需有对应 origin 且 status=COMPLETED
    - 其他类型可根据实际业务扩展
    """
    if getattr(file_record, 'experiment_type', None) == 'imzML':
        origin = (await session.exec(
            select(OriginImzMLInDB).where(
                OriginImzMLInDB.source_file_id == file_record.file_id,
                OriginImzMLInDB.status == ExperimentStatus.COMPLETED
            )
        )).first()
        return origin is not None
    # 其他类型直接视为已上传
    return True


async def find_origin_by_id(async_session: AsyncSession, origin_id: int) -> OriginImzMLInDB | None:
    """
    根据 origin ID 查找 origin 记录
    """
    statement = select(OriginImzMLInDB).where(OriginImzMLInDB.id == origin_id)
    result = await async_session.exec(statement)
    return result.first()
