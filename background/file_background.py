from datetime import timedelta

from sqlmodel import select, or_


from api_services.composite_service import (
    finalize_file_deletion,
    finalize_run_deletion,
    mark_file_for_deletion,
)
from api_services.processing_service import refund_processed_quota_for_file
from api_services.user_service import get_user_by_id
from api_services.file_service import (
    get_file_record_by_id,
    safe_cleanup_preflight_record,
)
from storage.oss.oss_storage import OssStorage
from background.file_background_helper import delete_oss_objects
from constants.file_constants import (
    PREFLIGHT_TTL_MINUTES,
    DOWNLOAD_ZIP_TTL_MINUTES,
)
from constants.file_constants import STALE_PROCESS_RUN_TIMEOUT_MINUTES, STALE_CLUSTERING_RUN_TIMEOUT_MINUTES
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED,FAILED_EXPERIMENT_TTL_MINUTES
from database.db import get_standalone_async_session
from database.redis_client import redis_client
from database.atomic_ops import atomic_decrement
from models.file_models import FileInDB, FileStatus
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

    清理条件(uploaded_at 超过 TTL, 且满足以下任一):
    1. 文件物理路径为空（""）(说明为预检记录,未完成上传)
    2. status == FAILED (回调失败留下的脏记录, include_failed=True 时生效)

    失败记录的 path 可能为空(回调未回传路径)或有值(哈希校验/打 tag 失败等)，
    两种情况都仅清理数据库记录与用户额度，不主动删除 OSS 对象——未打persistent tag 的孤儿对象由 bucket 生命周期规则自动回收。

    Returns:
        清理的记录数量
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup stale preflights for all users with TTL {ttl_minutes} minutes...")

        # 代表 uploaded_at 超过 TTL 的时间点
        now = utc_time()
        cutoff_time = now - timedelta(minutes=ttl_minutes)
        total_cleaned = 0

        # 查询所有过期预检记录:
        #   path 为空(未完成上传) 或 status 为 FAILED(回调失败脏记录) 通过 UserFileLink 关联获取用户
        conditions = [or_(FileInDB.path == "", FileInDB.status == FileStatus.FAILED),
                          FileInDB.uploaded_at < cutoff_time]
        records = (await session.exec(
            select(FileInDB, UserFileLink.user_id)
            .join(UserFileLink, FileInDB.file_id == UserFileLink.file_id) # type: ignore[arg-type]
            .join(UserInDB, UserInDB.id == UserFileLink.user_id) # type: ignore[arg-type]
            .where(*conditions)
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
      - 走到数据库 file 表 path 不为空，且未被标记为已清理（哨兵 SOURCE_FILE_PATH_CLEARED）
      - last_downloaded_at 早于 当前时间 - retention_days
    物理文件删除后回写哨兵 path（非空但不再指向物理文件），保留数据库记录。

    注意：查询条件必须排除哨兵。已清理记录的 last_downloaded_at 不会随之更新，
    若不排除，它们会每轮被重复捞起又整批跳过，导致新文件永远得不到清理（任务"卡住"）。
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup old download zips for all users with TTL {retention_minutes} minutes...")

        now = utc_time()
        cutoff_time = now - timedelta(minutes=retention_minutes)
        cleaned = 0
        records = (await session.exec(
            select(FileInDB).where(
                FileInDB.path != "",
                FileInDB.path != SOURCE_FILE_PATH_CLEARED,
                FileInDB.last_downloaded_at < cutoff_time,
            )
            .order_by(FileInDB.file_id) # type: ignore[call-overload]
            .limit(batch_size).with_for_update(skip_locked=True))).all()
        for rec in records:
            file_path = rec.path.strip() if rec.path else ""
            if not file_path or rec.path == SOURCE_FILE_PATH_CLEARED:
                continue
            try:
                await OssStorage.delete_file(file_path)
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
) -> dict:
    """
    清理指定用户删除后遗留的文件数据。

    用户行删除时 userfilelink 已随之级联删除，这里逐个处理传入的全部 file_id（每个文件独立事务）:
    - 文件仍有其他持有者：不做处理（关联已随用户删除解除）
    - 文件已无持有者：与用户删除文件走同一条删除状态位流程——行锁内登记 DELETING 并收集路径快照
      （源文件、packup zip、预览图目录、origin、全部 run 输出），提交后投递 delete_objects_task，
      OSS 对象全部删除成功后才删 DB 行
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting clean_user_all_task for deleted_user_id={deleted_user_id} file_count={len(file_ids)}")

        total_unlinked = 0
        total_deleted = 0
        total_errors = 0

        for file_id in file_ids:
            try:
                # include_deleting=True：已处于删除流程的文件幂等重投删除任务
                file_record = await get_file_record_by_id(file_id, session, include_deleting=True)
                snapshot = await mark_file_for_deletion(session, None, file_record)
                # 先 commit 再投递（顺序理由见 file_api.delete_user_file）
                await session.commit()

                if snapshot is None:
                    # 还有其他用户持有，关联已随用户删除解除
                    total_unlinked += 1
                    continue

                delete_objects_task.delay(  # type: ignore
                    kind="file",
                    resource_id=file_id,
                    paths=list(snapshot.paths),
                    redis_keys=list(snapshot.redis_keys),
                )
                total_deleted += 1
            except Exception as exc:  # pylint: disable=broad-exception-caught
                await session.rollback()
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

    清理流程:
    1. 查询过期的失败 origin 记录
    2. 手动更新所有相关用户的 file_count 和 total_file_size
    3. 按数据库中已有路径尝试删除 OSS 对象
    4. 删除文件表记录
    5. 数据库自动级联删除关联的 UserFileLink 和 OriginImzMLInDB 记录

    注意:
    - 对数据库中已有的 file/imzML/ibd 路径逐个尝试显式删除
    - FC 未回调或路径未落库时无法可靠定位对象，未打 persistent tag 的对象由 OSS 生命周期清理
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
                if not file_record:
                    # 关联文件记录缺失（理论上被级联/手动删过），无法定位 OSS 对象，跳过
                    logger.warning(
                        "cleanup_failed_experiments: file record missing for origin exp_id=%s source_file_id=%s, skip",
                        exp.id, exp.source_file_id,
                    )
                    continue

                # 1. 查询所有关联的用户，使用 atomic_decrement 原子更新统计信息
                #    与 unlink_user_from_file 的语义一致，避免 skip_locked 或
                #    read-modify-write 导致的配额漂移。
                user_ids = (await session.exec(
                    select(UserFileLink.user_id).where(
                        UserFileLink.file_id == exp.source_file_id
                    )
                )).all()

                for user_id in user_ids:
                    assert user_id is not None
                    await atomic_decrement(session, UserInDB, user_id, "file_count")
                    await atomic_decrement(
                        session, UserInDB, user_id, "total_file_size", file_record.size or 0
                    )
                    logger.debug(
                        "updated user stats via atomic decrement: user_id=%s file_id=%s",
                        user_id, file_record.file_id,
                    )

                # 1.1 退还该文件所有 run 占用的处理配额。run 行随文件记录级联删除，
                #     必须在级联前按行退（口径同 delete_process_run），否则配额永久虚占；
                #     退还对象是 run.user_id（执行处理的用户），可能不同于文件 owner。
                await refund_processed_quota_for_file(session, exp.source_file_id)

                # 2. 收集所有 OSS 路径，一次批量删除，避免逐个 celery round-trip。
                #    回调失败时 failed_mark_origin_record_clean 已尽量把三个路径落库。
                #    同时补删该文件所有 run 的输出目录（zarr）：run 行随 file 级联删除，
                #    不提前收集 out_path 会成为孤儿对象。
                run_out_paths = (await session.exec(
                    select(ExperimentProcessRunInDB.out_path).where(
                        ExperimentProcessRunInDB.source_file_id == exp.source_file_id
                    )
                )).all()
                paths_to_delete = [
                    p for p in (
                        (file_record.path or "").strip(),
                        (exp.imzml_path or "").strip(),
                        (exp.ibd_path or "").strip(),
                        *((pp or "").strip() for pp in run_out_paths),
                    )
                    if p
                ]
                if paths_to_delete:
                    try:
                        await OssStorage.delete_file(paths_to_delete)
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.warning(
                            "cleanup_failed_experiments delete oss objects failed paths=%s error=%s",
                            paths_to_delete, exc,
                        )

                # 3. 删除文件表记录。
                # 数据库级联删除会自动处理: 删除关联的 UserFileLink 记录.删除关联的 OriginImzMLInDB 记录
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
    clustering_timeout_minutes: int = STALE_CLUSTERING_RUN_TIMEOUT_MINUTES,
    batch_size: int = 100,
) -> int:
    """
    清理僵尸算法/转换/聚类 run：将超过 timeout 仍为 processing 的记录标记为 failed。

    FC 函数异步执行后通过回调更新状态，若 FC 超时/崩溃/回调丢失，
    run 会永久卡在 processing。此任务定期扫描并标记为 failed。

    两个阶段独立判定超时，互不覆盖对方的错误字段：
    - 算法处理：status + error_message，以 created_at 为开始时间
    - 聚类：clustering_status + clustering_error_message，以 clustering_started_at
      为开始时间（聚类由用户在处理完成后手动触发，不能用 created_at/finished_at
      判定；历史数据该字段为 NULL，不参与扫描，由运维手动处理）
    """
    async with get_standalone_async_session() as session:
        now = utc_time()
        total_cleaned = 0

        # 阶段一：算法处理超时
        cutoff_time = now - timedelta(minutes=timeout_minutes)
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

        # 阶段二：聚类超时（处理阶段已 COMPLETED 的 run 才可能处于聚类 processing，
        # 只置 clustering_status/clustering_error_message，不动处理阶段字段；
        # 若 FC 回调迟到，真实回调会无条件覆盖此处标记）
        clustering_cutoff_time = now - timedelta(minutes=clustering_timeout_minutes)
        clustering_records = (await session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.clustering_status == ProcessingStatus.PROCESSING,
                ExperimentProcessRunInDB.clustering_started_at < clustering_cutoff_time,).limit(batch_size) # type: ignore[call-overload]
            .with_for_update(skip_locked=True))).all()

        for run_record in clustering_records:
            run_record.clustering_status = ProcessingStatus.FAILED
            run_record.clustering_error_message = "FC clustering task timeout: no callback received"
            session.add(run_record)
            total_cleaned += 1
            logger.info(
                "marked stale clustering run as failed run_id=%s clustering_started_at=%s",
                run_record.id, run_record.clustering_started_at,
            )

        if total_cleaned:
            await session.commit()

        logger.info("cleanup_stale_process_runs completed, marked %d as failed", total_cleaned)
    return total_cleaned


@celery_app.task()  # type: ignore
async def delete_objects_task(
    kind: str,
    resource_id: int,
    paths: list[str],
    redis_keys: list[str] | None = None,
) -> bool:
    """
    统一的 OSS 删除执行任务（② 执行，全系统删除主流程只此一处碰 OSS 删除）。

    kind 取值：file（场景 1，删文件）/ run（场景 2，删算法/聚类 run 输出）。
    幂等：底层 delete_oss_objects 删不存在的 key 返回 204 也算成功，可无限重试。
    流程：
    - 任一对象删除失败 → 行置 DELETE_FAILED 保留（「要么都保留」），等重试
    - 全部成功 → finalize[kind] 在同一 DB 事务内删行（「要么都删除」）
    finalize 容忍行已不在（重复投递时直接算成功）。
    """
    # 1. 清理 Redis 状态键（best-effort，键自身有 TTL，失败不影响主流程）
    for key in redis_keys or []:
        try:
            await redis_client.delete(key)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("delete_objects_task: delete redis key failed key=%s error=%s", key, exc)

    # 2. 删除 OSS 对象（幂等，可无限重试）
    ok = await delete_oss_objects(paths)
    if not ok:
        await _mark_delete_failed(kind, resource_id)
        return False

    # 3. finalize：同一 DB 事务内收尾
    async with get_standalone_async_session() as session:
        if kind == "file":
            await finalize_file_deletion(session, resource_id)
        elif kind == "run":
            await finalize_run_deletion(session, resource_id)
        else:
            logger.error("delete_objects_task: unknown kind=%s resource_id=%s", kind, resource_id)
            return False
        await session.commit()
    return True


async def _mark_delete_failed(kind: str, resource_id: int) -> None:
    """OSS 删除失败后，把对应资源行置 DELETE_FAILED（按 kind 分表，行保留等重试）。"""
    async with get_standalone_async_session() as session:
        if kind == "file":
            record = (await session.exec(
                select(FileInDB)
                .where(FileInDB.file_id == resource_id)
                .with_for_update()
            )).first()
            deleting_states = (FileStatus.DELETING, FileStatus.DELETE_FAILED)
        elif kind == "run":
            record = (await session.exec(
                select(ExperimentProcessRunInDB)
                .where(ExperimentProcessRunInDB.id == resource_id)
                .with_for_update()
            )).first()
            deleting_states = (ProcessingStatus.DELETING, ProcessingStatus.DELETE_FAILED)
        else:
            logger.error("_mark_delete_failed: unknown kind=%s resource_id=%s", kind, resource_id)
            return

        if record is not None and record.status in deleting_states:
            if kind == "file":
                record.status = FileStatus.DELETE_FAILED
            else:
                record.status = ProcessingStatus.DELETE_FAILED
            session.add(record)
            await session.commit()
            logger.error(
                "delete_objects_task: oss delete failed, marked DELETE_FAILED kind=%s resource_id=%s",
                kind, resource_id,
            )
        else:
            # 行已被并发 finalize 删除或状态异常：只记日志，不覆盖状态
            logger.info(
                "delete_objects_task: oss delete failed but row gone/skipped kind=%s resource_id=%s",
                kind, resource_id,
            )


@celery_app.task()
def heartbeat():
    """
    这是一个心跳任务，用于celery beat后台保活，
    理论上无需进行任何操作，也可以定期输出一些东西
    """
    pass # pylint: disable=unnecessary-pass
