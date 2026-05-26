from typing import Annotated
from fastapi import Depends
from zipfile import BadZipFile

from database.db import AsyncSessionDep, get_async_engine, AsyncSession
from database.redis_client import redis_client
from storage_backend.storage import OssStorage

from api.file_api_helper import get_file_by_filename
from api.experiment_api_business import create_origin_record, mark_origin, delete_source_file_and_clear_path

from models.file_models import FileInDB, FilePublic, PackupStatus
from models.experiment_models import ExperimentStatus
from models.config_models import Config
from models.callback_models import OssUploadCallback
from exceptions.file_exceptions import (
    file_content_missing_exception,
    storage_type_not_allowed_exception,
    hash_size_validation_failed_exception,
    oss_packup_callback_error_exception,
)
from exceptions.experiment_exceptions import invalid_zip_file_exception
from exceptions.common_exceptions import internal_server_error_exception

from boot.config import get_config
from tools.time_tools import utc_time
from tools.logger import get_logger

logger = get_logger("api.callback_api")

# TODO: 针对此处每个api，需要验证签名

async def get_expected_hash(
    filename: str,
    async_session: AsyncSessionDep
) -> str:
    """
    获取文件的理论哈希

    :param filename: 文件名
    :param async_session: 异步会话依赖
    :return: 文件的理论哈希
    """
    file_record = await get_file_by_filename(filename, async_session)
    if not file_record.file_verify_code:
        logger.warning("file record not found file_verify_code filename=%s", filename)
        raise file_content_missing_exception
    logger.info("get expected hash filename=%s, expected hash=%s", filename, file_record.file_verify_code)
    return file_record.file_verify_code


async def oss_upload_callback(
    callback_info: OssUploadCallback,
    async_session: AsyncSessionDep,
) -> FileInDB:
    """
    OSS上传完成回调
    作用是更新文件路径并创建实验记录(只针对数据库做操作，不涉及文件系统)

    :param filename: 文件名
    :param file_path: 文件路径
    :param async_session: 异步会话依赖

    :return: FileInDB
    """
    filename = callback_info.filename
    file_path = callback_info.file_path
    is_success = callback_info.is_success
    imzml_path = callback_info.imzml_path
    ibd_path = callback_info.ibd_path
    imzml_size = callback_info.imzml_size
    ibd_size = callback_info.ibd_size
    act_hash = callback_info.act_hash
    error_type = callback_info.error_type

    source_file = None
    source_file_id = None
    origin_record = None
    try:
        source_file = await get_file_by_filename(filename, async_session)
        source_file_id = source_file.file_id # type: ignore

        # 验证存储类型是否为oss
        if source_file.storage_type != "oss":
            logger.error("file record storage_type not match to oss, filename=%s, storage_type=%s", filename, source_file.storage_type)
            raise storage_type_not_allowed_exception

        # 创建实验记录
        origin_record = await create_origin_record(
            source_file_id=source_file.file_id, # type: ignore
            experiment_name=filename,
            async_session=async_session
        )

        # 验证上传是否成功
        if not is_success:
            if error_type == "BadZipFile":
                logger.error("oss upload not complete, filename = %s is not a valid zip file", filename)
                raise BadZipFile
            if error_type == "FileNotFoundError":
                logger.error("oss upload not complete, filename = %s ZIP file must contain both .imzML and .ibd files", filename)
                raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")
            else:
                logger.error("oss upload not complete, filename = %s, error_type=%s", filename, error_type)
                raise Exception(f"oss upload not complete, error_type={error_type}") # pylint: disable=W0719

        # 验证路径是否为空
        if not file_path or not imzml_path or not ibd_path:
            logger.error("file path or imzml path or ibd path is empty, filename=%s", filename)
            raise file_content_missing_exception

        # 验证实验大小是否为空或0
        if not imzml_size or not ibd_size or imzml_size <= 0 or ibd_size <= 0:
            logger.error("imzml size or ibd size is empty or <= 0, filename=%s", filename)
            raise file_content_missing_exception

        # 验证文件哈希是否为空
        if not act_hash:
            logger.error("act hash is empty, filename=%s", filename)
            raise hash_size_validation_failed_exception

        # 更新文件大小
        source_file.size = await OssStorage.get_file_size(file_path)
        if not source_file.size or source_file.size <= 0:
            logger.error("oss get file size failed, filename=%s", filename)
            raise internal_server_error_exception

        # 更新文件路径和实验路径
        source_file.path = file_path
        source_file.uploaded_at = utc_time()
        origin_record.status = ExperimentStatus.COMPLETED
        origin_record.imzml_path = imzml_path
        origin_record.ibd_path = ibd_path
        origin_record.imzml_size = imzml_size
        origin_record.ibd_size = ibd_size
        async_session.add(source_file)
        async_session.add(origin_record)

        # 标记OSS文件为持久化
        is_file_tagged = await OssStorage.tag_oss_file(file_path, {"persistent": "true", "file_verify_code": act_hash})
        is_imzml_tagged = await OssStorage.tag_oss_file(imzml_path, {"persistent": "true", "file_verify_code": act_hash})
        is_ibd_tagged = await OssStorage.tag_oss_file(ibd_path, {"persistent": "true", "file_verify_code": act_hash})

        if not all([is_file_tagged, is_imzml_tagged, is_ibd_tagged]):
            logger.error("oss tag file persistent failed, filename=%s", filename)
            raise internal_server_error_exception

        # 提交事务
        await async_session.commit()
        await async_session.refresh(source_file)
        await async_session.refresh(origin_record)

        return source_file

    except (BadZipFile, FileNotFoundError) as e:
        source_file.file_path = file_path # 此处更新文件路径，保持与oss记录一致，方便后续通过定时任务删除
        async_session.add(source_file)
        await mark_origin(
            async_session = async_session,
            origin=origin_record.id, # type:ignore
            storage_type="oss", # type:ignore
            error_message=str(e),
            status=ExperimentStatus.FAILED
        )
        await async_session.commit()
        await async_session.refresh(source_file)
        await async_session.refresh(origin_record)

        logger.error(f"invalid file ,origin id & file id is {origin_record.id} & {source_file.file_id}\nerror={e}")
        raise invalid_zip_file_exception from e

    except Exception as e:
        # 针对其他的异常，回滚实验记录的修改，保持数据库一致性
        await async_session.rollback()

        # 删除原始zip，清理记录的file的path信息
        if source_file_id:
            async with AsyncSession(get_async_engine()) as async_session:
                await delete_source_file_and_clear_path(async_session, source_file_id)
                await async_session.commit()

        logger.error("oss upload callback error, filename=%s, error=%s", filename, e)
        raise internal_server_error_exception from e


async def oss_packup_callback(
    filename: str,
    status: PackupStatus,
    zip_path: str | None = None,
) -> None:
    """
    处理OSS文件打包回调
    """
    r = redis_client
    try:
        # 打包成功，存储URL，更新状态为COMPLETED
        if status == PackupStatus.COMPLETED:
            await r.hset(f"oss_packup-{filename}", mapping={"status": status.value, "file_path": zip_path}) # type:ignore
            await r.expire(f"oss_packup-{filename}", 60 * 5) # 5分钟过期
        # 打包失败，更新状态为FAILED
        if status == PackupStatus.FAILED:
            await r.hset(f"oss_packup-{filename}", mapping={"status": status.value, "file_path": zip_path}) # type:ignore
            await r.expire(f"oss_packup-{filename}", 60 * 5) # 5分钟过期

    except Exception as e:
        logger.error("oss packup callback error, filename=%s, error=%s", filename, e)
        raise internal_server_error_exception from e



GetExpectHashDep = Annotated[str, Depends(get_expected_hash)]
GetConfigDep = Annotated[Config, Depends(get_config)]
OssUploadCallbackDep = Annotated[FilePublic, Depends(oss_upload_callback)]
OssPackupCallbackDep = Annotated[None, Depends(oss_packup_callback)]
