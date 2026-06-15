from zipfile import BadZipFile
from typing import Annotated

from fastapi import Depends, HTTPException

from sqlmodel import select

from database.db import AsyncSessionDep, get_async_engine, AsyncSession
from database.redis_client import redis_client
from storage_backend.storage import OssStorage

from api.file_api_helper import get_file_by_filename
from api.experiment_api_business import create_origin_record, mark_origin, delete_source_file_and_clear_path
from api.processing_api_business import get_process_run_by_id, mark_process_run
from background.processing_background import trigger_fc_tic_task, trigger_fc_feature_task
from constants.file_constants import PACKUP_CACHE_TTL_SECONDS
from models.callback_models import OssUploadCallback, AlgorithmCallback, TicCallback, FeatureCallback
from models.config_models import Config
from models.experiment_models import ExperimentStatus
from models.file_models import FileInDB, FilePublic, FileStatus, PackupStatus
from models.processing_models import ProcessingStatus, ExperimentProcessRunInDB, ExperimentProcessRunPublic
from models.user_models import UserInDB
from exceptions.file_exceptions import (
    file_content_missing_exception,
    storage_type_not_allowed_exception,
    hash_size_validation_failed_exception,
)
from exceptions.experiment_exceptions import invalid_zip_file_exception, invalid_process_request_exception
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
            logger.error("oss upload not complete, filename = %s, error_type=%s", filename, error_type)
            raise Exception(f"oss upload not complete, error_type={error_type}") # pylint: disable=broad-exception-raised

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
        source_file.status = FileStatus.COMPLETED
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

        # 根据 storage_mode 分发到不同的 FC 函数
        # continuous → fc-feature (UMAP/KMeans 特征分析，大内存)
        # processed  → fc-tic (TIC 图像生成，轻量)
        if source_file.storage_mode == "continuous":
            trigger_fc_feature_task.delay(file_id=source_file.file_id, imzml_path=imzml_path) # type: ignore
        else:
            trigger_fc_tic_task.delay(file_id=source_file.file_id, imzml_path=imzml_path) # type: ignore

        return source_file

    except (BadZipFile, FileNotFoundError) as e:
        source_file.file_path = file_path # 此处更新文件路径，保持与oss记录一致，方便后续通过定时任务删除 # type: ignore
        source_file.status = FileStatus.FAILED # type: ignore
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

        logger.error(f"invalid file ,origin id & file id is {origin_record.id} & {source_file.file_id}\nerror={e}") # type: ignore
        raise invalid_zip_file_exception from e

    except HTTPException:
        await async_session.rollback()
        raise

    except Exception as e:
        # 针对其他的异常，回滚实验记录的修改，保持数据库一致性
        await async_session.rollback()

        # 删除原始zip，清理记录的file的path信息
        if source_file_id:
            async with AsyncSession(get_async_engine()) as async_session:
                await delete_source_file_and_clear_path(async_session, source_file_id) # type: ignore
                await async_session.commit()

        logger.error("oss upload callback error, filename=%s, error=%s", filename, e)
        raise internal_server_error_exception from e


async def algorithm_callback(
    callback_info: AlgorithmCallback,
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunInDB:
    """
    FC 算法处理完成回调

    根据处理结果更新 process_run 状态和用户配额。

    :param callback_info: 算法回调信息
    :param async_session: 异步会话依赖
    :return: ExperimentProcessRunInDB
    """
    run_id = callback_info.run_id
    is_success = callback_info.is_success
    out_path = callback_info.out_path
    output_size = callback_info.output_size
    error_type = callback_info.error_type
    error_message = callback_info.error_message

    try:
        run_record = await get_process_run_by_id(async_session, run_id)
        if not run_record:
            logger.error("process run not found for algorithm callback run_id=%s", run_id)
            raise invalid_process_request_exception

        # 处理失败
        if not is_success:
            await mark_process_run(
                async_session, run_record,
                status=ProcessingStatus.FAILED,
                error_message=error_message or error_type or "FC algorithm processing failed",
            )
            await async_session.commit()
            await async_session.refresh(run_record)
            logger.info("algorithm callback failed run_id=%s error=%s", run_id, error_message)
            return run_record

        # 校验回调数据完整性
        if not out_path or not output_size or output_size <= 0:
            logger.error("algorithm callback data incomplete run_id=%s", run_id)
            raise file_content_missing_exception

        # 更新 run 状态
        await mark_process_run(
            async_session, run_record,
            status=ProcessingStatus.COMPLETED,
            out_path=out_path,
            output_size=output_size,
        )

        # 累加用户处理配额
        user = (await async_session.exec(
            select(UserInDB).where(UserInDB.id == run_record.user_id)
        )).first()
        if user is not None:
            user.processed_file_size += output_size
            async_session.add(user)

        await async_session.commit()
        await async_session.refresh(run_record)

        logger.info(
            "algorithm callback success run_id=%s out_path=%s output_size=%s",
            run_id, out_path, output_size,
        )
        return run_record

    except HTTPException:
        await async_session.rollback()
        raise
    except Exception as e:
        await async_session.rollback()
        logger.error("algorithm callback error run_id=%s error=%s", run_id, e)
        raise internal_server_error_exception from e


async def tic_callback(
    callback_info: TicCallback,
    async_session: AsyncSessionDep,
) -> FileInDB:
    """
    FC TIC 图像生成回调。

    更新文件的 image_path 字段。
    """
    file_id = callback_info.file_id
    is_success = callback_info.is_success
    image_path = callback_info.image_path

    try:
        file_record = (
            await async_session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )
        ).first()
        if not file_record:
            logger.error("tic callback: file not found file_id=%s", file_id)
            raise file_content_missing_exception

        if not is_success:
            logger.warning("tic generation failed file_id=%s error=%s",
                           file_id, callback_info.error_message)
            return file_record

        file_record.image_path = image_path
        async_session.add(file_record)
        await async_session.commit()
        await async_session.refresh(file_record)

        logger.info("tic callback success file_id=%s image_path=%s", file_id, image_path)
        return file_record

    except HTTPException:
        await async_session.rollback()
        raise
    except Exception as e:
        await async_session.rollback()
        logger.error("tic callback error file_id=%s error=%s", file_id, e)
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
            await r.expire(f"oss_packup-{filename}", PACKUP_CACHE_TTL_SECONDS) # 与签名URL过期时间一致
        # 打包失败，更新状态为FAILED
        if status == PackupStatus.FAILED:
            await r.hset(f"oss_packup-{filename}", mapping={"status": status.value, "file_path": zip_path}) # type:ignore
            await r.expire(f"oss_packup-{filename}", PACKUP_CACHE_TTL_SECONDS) # 与签名URL过期时间一致

    except Exception as e:
        logger.error("oss packup callback error, filename=%s, error=%s", filename, e)
        raise internal_server_error_exception from e


async def feature_callback(
    callback_info: FeatureCallback,
    async_session: AsyncSessionDep,
) -> FileInDB:
    """
    FC UMAP/KMeans 特征分析完成回调。

    更新文件的 image_path 字段。
    """
    file_id = callback_info.file_id
    is_success = callback_info.is_success
    image_path = callback_info.image_path

    try:
        file_record = (
            await async_session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id))).first()
        if not file_record:
            logger.error("feature callback: file not found file_id=%s", file_id)
            raise file_content_missing_exception

        if not is_success:
            logger.warning("feature analysis failed file_id=%s error=%s",
                           file_id, callback_info.error_message)
            return file_record

        file_record.image_path = image_path
        async_session.add(file_record)
        await async_session.commit()
        await async_session.refresh(file_record)

        logger.info("feature callback success file_id=%s image_path=%s", file_id, image_path)
        return file_record

    except HTTPException:
        await async_session.rollback()
        raise
    except Exception as e:
        await async_session.rollback()
        logger.error("feature callback error file_id=%s error=%s", file_id, e)
        raise internal_server_error_exception from e


GetExpectHashDep = Annotated[str, Depends(get_expected_hash)]
GetConfigDep = Annotated[Config, Depends(get_config)]
OssUploadCallbackDep = Annotated[FilePublic, Depends(oss_upload_callback)]
AlgorithmCallbackDep = Annotated[ExperimentProcessRunPublic, Depends(algorithm_callback)]
TicCallbackDep = Annotated[FileInDB, Depends(tic_callback)]
FeatureCallbackDep = Annotated[FileInDB, Depends(feature_callback)]
OssPackupCallbackDep = Annotated[None, Depends(oss_packup_callback)]
