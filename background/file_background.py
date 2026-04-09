from datetime import timedelta

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from api.file_api_helper import delete_file_by_storage
from api.file_api_business import safe_cleanup_preflight_record
from api.user_api_helper import get_user_by_id
from constants.file_constants import (
    PREFLIGHT_TTL_MINUTES, DOWNLOAD_ZIP_RETENTION_DAYS,
)

from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from database.db import get_async_engine
from models.file_models import FileInDB
from models.link_models import UserFileLink
from models.user_models import UserInDB

from background.celery_app import celery_app

from tools import get_logger,utc_time
logger = get_logger("background.file_background")

assert celery_app is not None, "celery_app is None"


@celery_app.task() # type: ignore
async def cleanup_stale_preflights_all_users(
    ttl_minutes: int = PREFLIGHT_TTL_MINUTES,
    batch_size: int = 100,
) -> int:
    """
    清理所有用户过期且未完成的预检记录(celery定时任务版本)

    清理条件(需同时满足):
    1. 文件物理路径为空（“”）(说明为预检记录,未完成上传)
    2. uploaded_at 超过 TTL

    Returns:
        清理的记录数量
    """
    async with AsyncSession(get_async_engine()) as session:
        logger.info(f"starting cleanup stale preflights for all users with TTL {ttl_minutes} minutes...")

        # 代表 uploaded_at 超过 TTL 的时间点
        now = utc_time()
        cutoff_time = now - timedelta(minutes=ttl_minutes)
        total_cleaned = 0

        # 直接查询所有过期预检记录（path 为空且 uploaded_at < cutoff_time）
        # 通过 UserFileLink 关联获取用户
        records = (await session.exec(
            select(FileInDB, UserFileLink.user_id)
            .join(UserFileLink, FileInDB.file_id == UserFileLink.file_id) # type: ignore[arg-type]
            .join(UserInDB, UserInDB.id == UserFileLink.user_id) # type: ignore[arg-type]
            .where(FileInDB.path == "", FileInDB.uploaded_at < cutoff_time)
            .limit(batch_size)
        )).all()

        if not records:
            return 0

        for file_record, user_id in records:
            # 清理预检记录
            assert user_id is not None
            user = await get_user_by_id(user_id, session)
            await safe_cleanup_preflight_record(session, user, file_record) # type: ignore[arg-type]
            total_cleaned += 1

        if total_cleaned:
            await session.commit()
            logger.info(f"completed cleanup of stale preflights for all users, total cleaned {total_cleaned} records")

        return total_cleaned


@celery_app.task() # type: ignore
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
                await delete_file_by_storage(file_path, rec.storage_type)
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
