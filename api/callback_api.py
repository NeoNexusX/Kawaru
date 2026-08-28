from zipfile import BadZipFile
import os
from typing import Annotated
from operator import attrgetter
from fastapi import Depends, Header, HTTPException

from sqlmodel import select

from api_services.composite_service import failed_mark_origin_record_clean
from database.db import AsyncSessionDep
from database.redis_client import redis_client
from database.atomic_ops import atomic_increment
from storage.oss.oss_storage import OssStorage

from api_services.file_service import get_file_record_by_id, update_file_record
from api_services.experiment_service import create_origin_record, get_origin_imzml_by_file_id, update_origin
from api_services.processing_service import get_process_run_by_id, mark_process_run, mark_clustering_run
from background.processing_background import trigger_fc_tic_task, trigger_fc_feature_task
from constants.file_constants import PACKUP_CACHE_TTL_SECONDS
from models.callback_models import OssUploadCallback, AlgorithmCallback, TicCallback, UmapCallback, ClusteringCallback
from models.config_models import Config
from models.experiment_models import ExperimentStatus
from models.file_models import FileInDB, FilePublic, FileStatus, PackupStatus
from models.processing_models import ProcessingStatus, ExperimentProcessRunInDB, ExperimentProcessRunPublic, RAW_CONVERT_MARKER
from models.user_models import UserInDB
from exceptions.file_exceptions import (
    file_content_missing_exception,
    storage_type_not_allowed_exception,
    hash_size_validation_failed_exception,
)
from exceptions.experiment_exceptions import invalid_process_request_exception
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.callback_exceptions import invalid_fc_callback_token_exception

from boot.config import get_config
from tools.time_tools import utc_time
from tools.logger import get_logger

logger = get_logger("api.callback_api")

# FC 回调验证 Token（从环境变量读取，与 FC 控制台配置一致）
FC_CALLBACK_TOKEN = os.environ["FC_CALLBACK_TOKEN"]

# TODO: 针对此处每个api，需要验证签名，目前使用的是token验证，后续可以考虑使用签名验证，签名验证需要FC端配合

async def get_expected_hash(
    file_id: int,
    async_session: AsyncSessionDep
) -> str:
    """
    获取文件的理论哈希

    :param file_id: 文件ID（FC 从 OSS 对象键 data/{file_id}/{file_verify_code}.zip 解析）
    :param async_session: 异步会话依赖
    :return: 文件的理论哈希
    """
    file_record = await get_file_record_by_id(file_id, async_session)
    if not file_record.file_verify_code:
        logger.warning("file record not found file_verify_code file_id=%s", file_id)
        raise file_content_missing_exception
    logger.info("get expected hash file_id=%s, expected hash=%s", file_id, file_record.file_verify_code)
    return file_record.file_verify_code


async def oss_upload_callback(
    callback_info: OssUploadCallback,
    async_session: AsyncSessionDep,
) -> FileInDB:
    """
    OSS上传完成回调
    作用是更新文件路径并创建实验记录(只针对数据库做操作，不涉及文件系统)

    :param callback_info: 回调信息（file_id 由 FC 从 OSS 对象键解析）
    :param async_session: 异步会话依赖

    :return: FileInDB
    """
    # TODO: 这里怎么自动同步更新？
    (file_id, file_path, is_success, imzml_path,
     ibd_path, imzml_size, ibd_size, act_hash, error_type) = attrgetter(
        "file_id", "file_path", "is_success", "imzml_path", "ibd_path",
        "imzml_size", "ibd_size", "act_hash", "error_type",)(callback_info)

    source_file = None
    origin_record = None
    try:
        source_file = await get_file_record_by_id(file_id, async_session)
        source_file_id = source_file.file_id
        experiment_name = source_file.filename

        # 已经存在不管什么状态直接返回
        existing = await get_origin_imzml_by_file_id(async_session, source_file_id) #type: ignore
        if existing:
            return source_file

        # 验证存储类型是否为oss
        if source_file.storage_type != "oss":
            logger.error("file record storage_type not match to oss, file_id=%s, storage_type=%s", file_id, source_file.storage_type)
            raise storage_type_not_allowed_exception

        ###以上为状态检查####

        # 创建实验记录
        origin_record = await create_origin_record(
            async_session=async_session,
            experiment_name=experiment_name,
            source_file_id=source_file_id, #type: ignore
        )

        # 验证上传是否成功
        if not is_success:
            if error_type == "BadZipFile":
                logger.error("oss upload not complete, file_id = %s is not a valid zip file", file_id)
                raise BadZipFile
            if error_type == "FileNotFoundError":
                logger.error("oss upload not complete, file_id = %s ZIP file must contain both .imzML and .ibd files", file_id)
                raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")
            logger.error("oss upload not complete, file_id = %s, error_type=%s", file_id, error_type)
            raise Exception(f"oss upload not complete, error_type={error_type}") # pylint: disable=broad-exception-raised

        # 校验回调数据完整性
        if (not file_path or not imzml_path or not ibd_path
                or not imzml_size or not ibd_size
                or imzml_size <= 0 or ibd_size <= 0):
            raise file_content_missing_exception

        # 哈希一致性校验
        if not act_hash or act_hash != source_file.file_verify_code:
            logger.error("hash check failed file_id=%s expected=%s actual=%s",
                         file_id, source_file.file_verify_code, act_hash)
            raise hash_size_validation_failed_exception

        # 更新文件大小
        source_file.size = await OssStorage.get_file_size(file_path)
        if not source_file.size or source_file.size <= 0:
            logger.error("oss get file size failed, file_id=%s", file_id)
            raise internal_server_error_exception

        # 更新文件路径和实验路径
        await update_file_record(async_session, source_file, status=FileStatus.COMPLETED,
                                 path=file_path, uploaded_at=utc_time())
        # 更新 origin 记录
        await update_origin(async_session, origin_record,
                            status=ExperimentStatus.COMPLETED,
                            imzml_path=imzml_path, imzml_size=imzml_size,
                            ibd_path=ibd_path, ibd_size=ibd_size)

        # 提交事务（路径已落库），再打 persistent tag
        await async_session.commit()
        await async_session.refresh(source_file)
        await async_session.refresh(origin_record)

        # 标记OSS文件为持久化。失败则抛异常进 except：三个路径已在上面第一次 commit 时落库，
        # rollback 不会抹掉，failed_mark_origin_record_clean 只需标 FAILED，
        # 由 cleanup_failed_experiments 按 DB 中已落库的路径删除对象，避免孤儿。
        is_file_tagged = await OssStorage.tag_oss_file(file_path, {"persistent": "true", "file_verify_code": act_hash})
        is_imzml_tagged = await OssStorage.tag_oss_file(imzml_path, {"persistent": "true", "file_verify_code": act_hash})
        is_ibd_tagged = await OssStorage.tag_oss_file(ibd_path, {"persistent": "true", "file_verify_code": act_hash})

        if not all([is_file_tagged, is_imzml_tagged, is_ibd_tagged]):
            logger.error("oss tag file persistent failed, file_id=%s", file_id)
            raise internal_server_error_exception

        # 根据 storage_mode 分发到不同的 FC 函数
        # continuous → fc-feature（UMAP 预览图，大内存）
        # 其他模式   → fc-tic（TIC 预览图，轻量）
        if source_file.storage_mode == "continuous":
            trigger_fc_feature_task.delay(file_id=source_file.file_id, imzml_path=imzml_path) # type: ignore
        else:
            trigger_fc_tic_task.delay(file_id=source_file.file_id, imzml_path=imzml_path) # type: ignore

        return source_file

    except Exception as e:
        #不直接删除的原因是考虑用户体验，他在时间段看到失败之后会主动删除，能看到失败。
        await async_session.rollback()
        await failed_mark_origin_record_clean(async_session,
                                              origin_record,
                                              source_file,
                                              e,
                                              f_status=FileStatus.FAILED,
                                              o_status=ExperimentStatus.FAILED)
        await async_session.commit()
        if origin_record:
            await async_session.refresh(origin_record)
            logger.error(f"origin id is {origin_record.id}")
        if source_file:
            await async_session.refresh(source_file)
            logger.error(f"file id is {source_file.file_id}")
        logger.error("oss upload callback error, file_id=%s, error=%s", file_id, e)

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

        # 累加用户处理配额（原子操作，防止并发丢失更新）
        # 同一用户的多个算法任务可能几乎同时完成，FC 回调几乎同时到达；
        # 改用 UPDATE SET processed_file_size = processed_file_size + N
        # 由数据库引擎在排他行锁内进行 current read，保证累加结果不丢失。
        if run_record.user_id is not None:
            await atomic_increment(
                async_session, UserInDB, run_record.user_id,
                "processed_file_size", output_size,
            )

        # raw_convert 成功回调：写入 fileindb.default_run_id
        if run_record.params_json == RAW_CONVERT_MARKER:
            file_record = (
                await async_session.exec(
                    select(FileInDB).where(
                        FileInDB.file_id == run_record.source_file_id,
                    )
                )
            ).first()
            if file_record:
                file_record.default_run_id = run_record.id
                async_session.add(file_record)
                logger.info(
                    "set default_run_id=%s for file_id=%s",
                    run_record.id, run_record.source_file_id,
                )

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

    更新文件的 image_path 字段（完整对象键，含 preview.jpg 文件名）。
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

        await update_file_record(async_session, file_record, image_path=image_path)
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


async def umap_callback(
    callback_info: UmapCallback,
    async_session: AsyncSessionDep,
) -> FileInDB:
    """
    FC UMAP 图生成完成回调。

    更新文件的 image_path 字段（完整对象键，含 preview.jpg 文件名）。
    """
    file_id = callback_info.file_id
    is_success = callback_info.is_success
    image_path = callback_info.image_path

    try:
        file_record = (
            await async_session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id))).first()
        if not file_record:
            logger.error("umap callback: file not found file_id=%s", file_id)
            raise file_content_missing_exception

        if not is_success:
            logger.warning("umap generation failed file_id=%s error=%s",
                           file_id, callback_info.error_message)
            return file_record

        await update_file_record(async_session, file_record, image_path=image_path)
        await async_session.commit()
        await async_session.refresh(file_record)

        logger.info("umap callback success file_id=%s image_path=%s", file_id, image_path)
        return file_record

    except HTTPException:
        await async_session.rollback()
        raise
    except Exception as e:
        await async_session.rollback()
        logger.error("umap callback error file_id=%s error=%s", file_id, e)
        raise internal_server_error_exception from e


async def clustering_callback(
    callback_info: ClusteringCallback,
    async_session: AsyncSessionDep,
) -> ExperimentProcessRunInDB:
    """
    FC 聚类分析完成回调。

    聚类结果写回源 zarr，回调只更新 run 记录的聚类状态。
    """
    run_id = callback_info.run_id
    is_success = callback_info.is_success
    error_type = callback_info.error_type
    error_message = callback_info.error_message

    try:
        run_record = await get_process_run_by_id(async_session, run_id)
        if not run_record:
            logger.error("process run not found for clustering callback run_id=%s", run_id)
            raise invalid_process_request_exception

        # 处理失败
        if not is_success:
            await mark_clustering_run(
                async_session, run_record,
                status=ProcessingStatus.FAILED,
                error_message=error_message or error_type or "FC clustering processing failed",
            )
            await async_session.commit()
            await async_session.refresh(run_record)
            logger.info("clustering callback failed run_id=%s error=%s", run_id, error_message)
            return run_record

        # 更新聚类状态
        await mark_clustering_run(
            async_session, run_record,
            status=ProcessingStatus.COMPLETED,
        )

        await async_session.commit()
        await async_session.refresh(run_record)

        logger.info("clustering callback success run_id=%s", run_id)
        return run_record

    except HTTPException:
        await async_session.rollback()
        raise
    except Exception as e:
        await async_session.rollback()
        logger.error("clustering callback error run_id=%s error=%s", run_id, e)
        raise internal_server_error_exception from e


async def verify_fc_callback_token(
    x_fc_token: str | None = Header(None, alias="X-FC-Token"),
) -> None:
    """
    验证 FC 回调请求的 Token。

    从请求头 X-FC-Token 中读取 Token，与环境变量 FC_CALLBACK_TOKEN 比对。
    如果 Token 缺失或不匹配，抛出 401 异常。
    """
    # 缺失/空 Token 或不匹配都视为未授权(401)，避免退化为 422 参数校验错误
    if not x_fc_token or x_fc_token != FC_CALLBACK_TOKEN:
        raise invalid_fc_callback_token_exception


GetExpectHashDep = Annotated[str, Depends(get_expected_hash)]
GetConfigDep = Annotated[Config, Depends(get_config)]
OssUploadCallbackDep = Annotated[FilePublic, Depends(oss_upload_callback)]
AlgorithmCallbackDep = Annotated[ExperimentProcessRunPublic, Depends(algorithm_callback)]
TicCallbackDep = Annotated[FileInDB, Depends(tic_callback)]
UmapCallbackDep = Annotated[FileInDB, Depends(umap_callback)]
ClusteringCallbackDep = Annotated[ExperimentProcessRunPublic, Depends(clustering_callback)]
OssPackupCallbackDep = Annotated[None, Depends(oss_packup_callback)]
