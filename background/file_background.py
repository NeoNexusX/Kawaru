from datetime import timedelta
import zipfile
from pathlib import Path

from fastapi import HTTPException
from sqlmodel import and_, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.user_api_business import check_user_login_status
from api.user_api_helper import get_user
from api.experiment_api_business import create_experiment_record, mark_experiment, delete_source_file_and_clear_path
from api.experiment_api_helper import create_experiment_directories, extract_and_validate_imzml_zip
from api.file_api_helper import get_file_record_by_id, delete_file_by_storage
from api.file_api_business import safe_cleanup_preflight_record
from constants.file_constants import PREFLIGHT_TTL_MINUTES, DOWNLOAD_ZIP_RETENTION_DAYS
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from database.db import get_async_engine
from exceptions.experiment_exceptions import invalid_zip_file_exception
from models.experiment_models import ExperimentStatus
from models.file_models import FileInDB, utc_time
from models.link_models import UserFileLink

from background.celery_app import celery_app, TaskManager
from tools.logger import get_logger
logger = get_logger("background.file_background")

async def process_imzml_experiment_background(
    user_id: int,
    source_file_id: int,
    experiment_name: str
):
    """
    后台处理 ImzML 实验的任务

    存储路径：
    - 临时目录:experiments/temp/exp_{id}/
    - 正式目录:experiments/processed/exp_{id}/
    """
    source_file = None
    async with AsyncSession(get_async_engine()) as async_session:
        try:
            logger.info(f"starting imzml experiment processing user={user_id} file={source_file_id}")

            # 阶段1: 查询文件记录并创建实验记录
            # 查询源文件
            source_file = await get_file_record_by_id(source_file_id, async_session)

            # 创建实验记录
            experiment_record = await create_experiment_record(async_session, experiment_name, source_file_id)

            #阶段2: 处理实验文件
            # 创建正式目录（helper 层）
            experiment_dir = create_experiment_directories(experiment_record.id)#type:ignore

            # 解压和验证
            final_imzml_path, final_ibd_path = extract_and_validate_imzml_zip(Path(source_file.path), experiment_dir)

            # 更新实验记录
            await mark_experiment(async_session,experiment_record,final_imzml_path,final_ibd_path)

            # 保留原始 ZIP 文件与数据库 path 字段，由定时任务统一清理
            await async_session.commit()
            await async_session.refresh(experiment_record)

            logger.info(f"completed imzml experiment processing exp_id={experiment_record.id} source_file_id={source_file_id}")

        except HTTPException as e:
            logger.error(e)
            raise e

        except (zipfile.BadZipFile, FileNotFoundError) as e:
            #此处理论上experiment_record一定存在，直接使用即可
            await mark_experiment(async_session = async_session,
                                    experiment=experiment_record.id, #type:ignore
                                    error_message=str(e),
                                    status=ExperimentStatus.FAILED)
            await async_session.commit()
            await async_session.refresh(experiment_record)
            logger.error(f"invalid file ,exp id & file id is {experiment_record.id} & {source_file_id}\nerror={e}") #type:ignore
            raise invalid_zip_file_exception from e

        except Exception as e:
            # 针对其他的异常，回滚实验记录的修改，保持数据库一致性
            await async_session.rollback()

            async with AsyncSession(get_async_engine()) as async_session:
                # 删除原始zip，清理记录的file的path信息
                if source_file:
                    await delete_source_file_and_clear_path(async_session, source_file)
                    await async_session.commit()
            logger.error("imzml experiment processing failed error=%s", e)
            raise


# TODO: 针对所有用户清理 做定时任务
@celery_app.task()
async def cleanup_stale_preflights(
    user_token: str,
    ttl_minutes: int = PREFLIGHT_TTL_MINUTES,
) -> int:
    """
    清理当前用户过期且未完成的预检记录(celery定时任务版本)

    清理条件(需同时满足):
    1. 记录 owners 中包含当前用户
    2. 文件物理路径不存在(说明为预检记录,未完成上传)
    3. file_id 不在用户 file_ids 中
    4. uploaded_at 超过 TTL

    更正清理条件:
    在多对多设计的情况下,简化为满足条件2和4即可清理

    Returns:
        清理的记录数量
    """
    username = await check_user_login_status(user_token)
    if username is False:
        TaskManager.remove_task(f"cleanup_stale_preflights_{user_token[0:8]}") # type:ignore
        logger.warning("Invalid user token provided for cleanup_stale_preflights task, " \
                        "task has been removed from schedule.")
        return 0

    # Create internal database session
    assert isinstance(username, str) # for type checker
    async with AsyncSession(get_async_engine()) as session:
        current_user = await get_user(username, session)
        assert current_user is not None # for type checker
        logger.info(f"starting cleanup stale preflights for user {current_user.id} with TTL {ttl_minutes} minutes...")

        # 代表 uploaded_at 超过 TTL 的时间点
        now = utc_time()
        cutoff_time = now - timedelta(minutes=ttl_minutes)
        cleaned = 0

        # 数据库层面先过滤出owners包含当前用户的所有文件记录
        records = (await session.exec(
            select(FileInDB)
            .join(UserFileLink, and_(FileInDB.file_id == UserFileLink.file_id))
            .where(UserFileLink.user_id == current_user.id)
        )).all()

        for rec in records:
            # 只要 path 不为空字符串,直接跳过(不再检查物理文件是否存在)
            if rec.path:
                continue

            # TTL 判断 - 所有时间都是UTC时间,直接比较
            if not rec.uploaded_at < cutoff_time:
                continue

            # 满足所有条件,清理预检记录
            await safe_cleanup_preflight_record(session, current_user, rec)
            cleaned += 1

        if cleaned:
            await session.commit()
            logger.info(f"completed cleanup of stale preflights for user {current_user.id}, cleaned {cleaned} records")

        return cleaned


@celery_app.task()
async def cleanup_old_download_zips(
    retention_days: int = DOWNLOAD_ZIP_RETENTION_DAYS,
    batch_size: int = 100,
) -> int:
    """
    定时清理符合条件的下载zip文件：
      - 走到数据库 file 表 path 不为空
      - last_downloaded_at 早于 当前时间 - retention_days
    物理文件删除后只置空 path，保留数据库记录
    """
    now = utc_time()
    cutoff_time = now - timedelta(days=retention_days)
    cleaned = 0

    async with AsyncSession(get_async_engine()) as session:
        records = (await session.exec(
            select(FileInDB).where(FileInDB.path != "", FileInDB.last_downloaded_at < cutoff_time).limit(batch_size))).all()
        for rec in records:
            file_path = rec.path.strip() if rec.path else ""
            if not file_path or rec.path == SOURCE_FILE_PATH_CLEARED:
                continue
            try:
                delete_file_by_storage(file_path, rec.storage_type)
                rec.path = SOURCE_FILE_PATH_CLEARED
                session.add(rec)
                cleaned += 1
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to delete old download zip for file_id=%s path=%s error=%s",rec.file_id,file_path,exc)

        if cleaned:
            await session.commit()

    logger.info("cleanup_old_download_zips removed %d old zip(s)", cleaned)
    return cleaned


@celery_app.task()
def heartbeat():
    """
    这是一个心跳任务，用于celery beat后台保活，
    理论上无需进行任何操作，也可以定期输出一些东西
    """
    pass # pylint: disable=unnecessary-pass
