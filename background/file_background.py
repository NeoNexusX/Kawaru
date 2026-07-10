from datetime import timedelta

from sqlmodel import select, func


from api_services.experiment_service import delete_origin_files
from api_services.user_service import get_user_by_id
from api_services.file_service import (
    get_file_record_by_id,
    safe_cleanup_preflight_record,
    delete_file_and_parts,
)
from api_helpers.file_helper import delete_file_by_storage
from constants.file_constants import PREFLIGHT_TTL_MINUTES, DOWNLOAD_ZIP_TTL_MINUTES
from constants.file_constants import STALE_PROCESS_RUN_TIMEOUT_MINUTES
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED,FAILED_EXPERIMENT_TTL_MINUTES
from database.db import get_standalone_async_session
from models.file_models import FileInDB
from models.link_models import UserFileLink
from models.user_models import UserInDB
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus

from celery_runtime.state import get_celery_app

from tools import get_logger,utc_time
logger = get_logger("background.file_background")

celery_app = get_celery_app()


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
    async with get_standalone_async_session() as session:
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
            .with_for_update(skip_locked=True)
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
    retention_minutes: int = DOWNLOAD_ZIP_TTL_MINUTES,
    batch_size: int = 100,
) -> int:
    """
    定时清理符合条件的下载zip文件：
      - 走到数据库 file 表 path 不为空
      - last_downloaded_at 早于 当前时间 - retention_days
    物理文件删除后只置空 path，保留数据库记录
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup old download zips for all users with TTL {retention_minutes} minutes...")

        now = utc_time()
        cutoff_time = now - timedelta(minutes=retention_minutes)
        cleaned = 0
        records = (await session.exec(
            select(FileInDB).where(FileInDB.path != "", FileInDB.last_downloaded_at < cutoff_time)
            .limit(batch_size).with_for_update(skip_locked=True))).all()
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


@celery_app.task()  # type: ignore
async def clean_user_all_task(
    file_ids: list[int],
    deleted_user_id: int | None = None,
    batch_size: int = 100,
) -> dict:
    """
    清理指定用户删除后保留的文件数据。

    传入 file_ids 后，按 delete_user_file 的逻辑处理每个 file_id:
    - 先解除已删除用户与文件的关联（如果存在）
    - 如果文件被多个用户共享，仅解除关联
    - 如果文件仅属于该用户时，完全删除文件和分片
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting clean_user_all_task for deleted_user_id={deleted_user_id} file_count={len(file_ids)}")

        total_unlinked = 0
        total_deleted = 0
        total_errors = 0

        for file_id in file_ids[:batch_size]:
            try:
                # 检查剩余所有者数量
                owner_count = (await session.exec(
                    select(func.count()).where(UserFileLink.file_id == file_id))).one() # pylint: disable=not-callable

                if owner_count > 0:
                    # 还有其他用户持有，只解除关联
                    total_unlinked += 1
                else:
                    # 没有其他用户持有，执行完全删除
                    file_record = await get_file_record_by_id(file_id, session)
                    if file_record.experiment_type == "imzML":
                        await delete_origin_files(session, file_id, file_record.storage_type)

                    # 创建一个临时的 user 对象用于删除
                    user_obj = UserInDB( # type: ignore
                        id=deleted_user_id,username="deleted_user",email="",hashed_password="",)
                    await delete_file_and_parts(session, file_id, user_obj) # TODO 因为这个函数需要这个user对象来记录日志，所以这里构造了一个临时的user对象传入，
                                                                            # 后续可以考虑优化这个函数的参数设计，避免需要传入一个无实际意义的user对象
                    total_deleted += 1

                await session.commit()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                total_errors += 1
                logger.error(
                    "clean_user_all_task failed for deleted_user_id=%s file_id=%s error=%s",deleted_user_id,file_id,exc,)
        result = {"deleted_user_id": deleted_user_id,"files_unlinked": total_unlinked,
                    "files_deleted": total_deleted,"errors": total_errors,}
        logger.info("clean_user_all_task completed %s", result)
    return result


@celery_app.task()  # type: ignore
async def cleanup_failed_experiments(
    ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES,
    batch_size: int = 100
) -> int:
    """
    清理过期的失败 origin 记录

    完整的清理流程:
    1. 查询过期的失败 origin 记录
    2. 手动更新所有相关用户的 file_count 和 total_file_size
    3. 删除 origin 文件(imzML、ibd)物理文件
    4. 删除文件表记录
    5. 数据库自动级联删除关联的 UserFileLink 和 OriginImzMLInDB 记录

    注意:
    - 源 ZIP 文件会删除
    - delete_origin_files 仅处理物理文件删除，不涉及数据库操作
    - 删除 file 记录时，数据库级联删除会处理 UserFileLink 和 OriginImzMLInDB
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup_failed_experiments with TTL {ttl_minutes} minutes... ")
        now = utc_time()
        threshold = now - timedelta(minutes=ttl_minutes)
        cleaned = 0
        # 第一步:查询过期的失败 origin 记录
        try:
            failed_origins = (await session.exec(select(OriginImzMLInDB).where(
                        OriginImzMLInDB.status == ExperimentStatus.FAILED,
                        OriginImzMLInDB.created_at < threshold,).limit(batch_size).with_for_update(skip_locked=True))).all()
        # 第二步:处理每个失败的 origin
            for exp in failed_origins:
                # 查询关联的文件记录
                file_record = (await session.exec(
                    select(FileInDB).where(FileInDB.file_id == exp.source_file_id))).first()

                # 1. 查询所有关联的用户，手动更新 file_count 和 total_file_size
                user_ids = (await session.exec(
                    select(UserFileLink.user_id).where(UserFileLink.file_id == exp.source_file_id))).all()

                for user_id in user_ids:
                    user_record = (await session.exec(select(UserInDB).where(UserInDB.id == user_id).with_for_update(skip_locked=True))).first()

                    if user_record:
                        user_record.file_count = max(0, (user_record.file_count or 0) - 1)  #TODO 风险，后面加锁
                        user_record.total_file_size = max(
                            0, (user_record.total_file_size or 0) - (file_record.size or 0)) # type: ignore[attr-defined]
                        session.add(user_record)
                        logger.debug("updated user stats: user_id=%s file_count=%s total_size=%s",user_id,user_record.file_count,
                            user_record.total_file_size,)

                # 2. 删除 origin 文件(imzML、ibd)物理文件， delete_origin_files 只处理物理文件删除，不操作数据库
                await delete_origin_files(session, exp.source_file_id, file_record.storage_type) # type: ignore

                # 3. 删除真实文件和文件表记录
                # 数据库级联删除会自动处理: 删除关联的 UserFileLink 记录.删除关联的 OriginImzMLInDB 记录
                file_path = file_record.path.strip() # type: ignore[attr-defined]
                if file_path and file_path != SOURCE_FILE_PATH_CLEARED :
                    await delete_file_by_storage(file_path, file_record.storage_type) # type: ignore[attr-defined]
                await session.delete(file_record)
                cleaned += 1
                logger.debug("cleanup failed origin: exp_id=%s", exp.id)

        # 第三步:提交事务
            await session.commit()
            logger.info("cleanup_failed_experiments completed %s", cleaned)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("failed to  cleanup changes error=%s", e)

    return cleaned


@celery_app.task()  # type: ignore
async def cleanup_stale_process_runs(
    timeout_minutes: int = STALE_PROCESS_RUN_TIMEOUT_MINUTES,
    batch_size: int = 100,
) -> int:
    """
    清理僵尸算法/转换 run：将超过 timeout 仍为 processing 的记录标记为 failed。

    FC 函数异步执行后通过回调更新状态，若 FC 超时/崩溃/回调丢失，
    run 会永久卡在 processing。此任务定期扫描并标记为 failed。
    """
    async with get_standalone_async_session() as session:
        now = utc_time()
        cutoff_time = now - timedelta(minutes=timeout_minutes)
        total_cleaned = 0

        records = (await session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.status == ProcessingStatus.PROCESSING,
                ExperimentProcessRunInDB.created_at < cutoff_time,).limit(batch_size)
            .with_for_update(skip_locked=True))).all()

        for run_record in records:
            run_record.status = ProcessingStatus.FAILED
            run_record.error_message = "FC task timeout: no callback received"
            run_record.finished_at = now
            session.add(run_record)
            total_cleaned += 1
            logger.info(
                "marked stale process run as failed run_id=%s created_at=%s",
                run_record.id, run_record.created_at,
            )

        if total_cleaned:
            await session.commit()

        logger.info("cleanup_stale_process_runs completed, marked %d as failed", total_cleaned)
    return total_cleaned


@celery_app.task()
def heartbeat():
    """
    这是一个心跳任务，用于celery beat后台保活，
    理论上无需进行任何操作，也可以定期输出一些东西
    """
    pass # pylint: disable=unnecessary-pass
