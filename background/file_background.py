from datetime import timedelta

from sqlmodel import select, or_


from api_helpers.admin_notify import notify_delete_failed
from api_services.composite_service import (
    finalize_file_deletion,
    finalize_run_deletion,
    mark_file_for_deletion,
)
from api_services.user_service import get_user_by_id
from api_services.file_service import (
    get_file_record_by_id,
    safe_cleanup_preflight_record,
)
from storage.oss.oss_storage import OssStorage, delete_oss_objects
from constants.file_constants import (
    PREFLIGHT_TTL_MINUTES,
    DOWNLOAD_ZIP_TTL_MINUTES,
)
from constants.file_constants import STALE_PROCESS_RUN_TIMEOUT_MINUTES, STALE_CLUSTERING_RUN_TIMEOUT_MINUTES
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED,FAILED_EXPERIMENT_TTL_MINUTES
from database.db import get_standalone_async_session
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
async def cleanup_source_zips(
    retention_minutes: int = DOWNLOAD_ZIP_TTL_MINUTES,
    batch_size: int = 100,
) -> int:
    """
    定时清理源 zip：file.path 指向用户上传的原始 zip（存量数据中也可能指向历史打包 zip）。
    解压回调成功后下载只走 origin（raw / relay），源 zip 不再有读取方，留存只占存储；
    它在回调时被打了 persistent tag，生命周期规则不会回收，只能由本任务删除。
    清理条件：
      - path 不为空，且未被标记为已清理（哨兵 SOURCE_FILE_PATH_CLEARED）
      - last_downloaded_at 早于 当前时间 - retention_minutes
    对象删除成功后才回写哨兵 path（非空但不再指向物理文件），保留数据库记录；
    删除失败保留原 path，下一轮重试——先写哨兵会让对象再无引用、永久泄漏。

    注意：查询条件必须排除哨兵。已清理记录的 last_downloaded_at 不会随之更新，
    若不排除，它们会每轮被重复捞起又整批跳过，导致新文件永远得不到清理（任务"卡住"）。
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup source zips for all users with TTL {retention_minutes} minutes...")

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
                ok = await OssStorage.delete_file(file_path)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to delete source zip for file_id=%s path=%s error=%s",rec.file_id,file_path,exc)
                continue
            if not ok:
                logger.warning("Failed to delete source zip, keep path for retry file_id=%s path=%s", rec.file_id, file_path)
                continue
            rec.path = SOURCE_FILE_PATH_CLEARED
            session.add(rec)
            cleaned += 1

        if cleaned:
            await session.commit()

        logger.info("cleanup_source_zips removed %d source zip(s)", cleaned)
    return cleaned


@celery_app.task()  # type: ignore
async def clean_user_all_task(
    file_ids: list[int],
    deleted_user_id: int | None = None,
    pre_collected_run_paths: list[list[int | str]] | None = None,
) -> dict:
    """
    清理指定用户删除后遗留的文件数据。

    用户行删除时 userfilelink 已随之级联删除，这里逐个处理传入的全部 file_id（每个文件独立事务）:
    - 文件仍有其他持有者：不做处理（关联已随用户删除解除）
    - 文件已无持有者：与用户删除文件走同一条删除状态位流程——行锁内登记 DELETING 并收集路径快照
      （源文件、预览图目录、origin、全部 run 输出），提交后投递 delete_objects_task，
      OSS 对象全部删除成功后才删 DB 行

    pre_collected_run_paths：del_user 在删 user 行前预收集的 (source_file_id, out_path) 对
    （run.user_id 外键 ondelete=CASCADE，user 行删除后 run 行消失、out_path 事后查不到，
    见 docs/zh/删除状态位改造方案.md §4 场景 3）。按 source_file_id 合入对应文件的删除快照；
    对应文件已无记录或仍有其他持有者时无处合入，仅告警——这些输出未打 persistent tag，
    依赖 bucket 生命周期规则回收（方案 §7 已接受的取舍）。
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting clean_user_all_task for deleted_user_id={deleted_user_id} file_count={len(file_ids)}")

        # 预收集的 run 输出按文件分组，待各文件登记成功后合入其删除快照
        pending_run_paths: dict[int, list[str]] = {}
        for source_file_id, out_path in pre_collected_run_paths or []:
            if not out_path:
                continue
            pending_run_paths.setdefault(int(source_file_id), []).append(str(out_path).strip())

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

                paths = list(snapshot.paths)
                paths.extend(pending_run_paths.pop(file_id, []))
                delete_objects_task.delay(  # type: ignore
                    kind="file",
                    resource_id=file_id,
                    paths=list(dict.fromkeys(paths)),
                )
                total_deleted += 1
            except Exception as exc:  # pylint: disable=broad-exception-caught
                await session.rollback()
                total_errors += 1
                logger.error(
                    "clean_user_all_task failed for deleted_user_id=%s file_id=%s error=%s",deleted_user_id,file_id,exc,)
        if pending_run_paths:
            logger.warning(
                "clean_user_all_task: run outputs not dispatched (file gone or shared), "
                "rely on bucket lifecycle deleted_user_id=%s leftovers=%s",
                deleted_user_id, pending_run_paths,
            )
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
    清理过期的失败 origin 记录（场景 1 的批量版：删除状态位三件套）

    清理流程（每个失败 origin 的源文件独立事务，互不拖累）:
    1. 查询过期的失败 origin 记录（行锁 skip_locked，只护住本次查询事务）
    2. mark_file_for_deletion(unlink_all_owners=True)：行锁内解除全部持有者的关联并
       释放 storage 配额（用户可见语义在登记事务内生效），置 DELETING 并收集路径快照
       （源文件、预览图目录、origin、全部 run 输出）
    3. 先 commit 再投递 delete_objects_task：worker 幂等删净 OSS 对象后 finalize 删
       file 行（级联 UserFileLink / OriginImzMLInDB / run），processed 配额由 finalize
       退还；任一对象删除失败则行置 DELETE_FAILED 保留，重复触发本任务即幂等重投

    注意:
    - 行锁只在本次查询事务内有效：本任务刻意逐文件 commit（互不拖累），首个文件提交后
      同一批 origin 仍满足扫描条件，多 worker 并发会重复捞取同一批（重复处理由
      mark_file_for_deletion 的行锁 + DELETING 守卫幂等吸收：不重复 unlink / 扣配额，
      代价仅是多余的 OSS 调用与日志）
    - include_deleting=True：文件已处于删除流程（如用户已自行删除）时走幂等分支直接重投
    - FC 未回调或路径未落库时无法可靠定位对象，未打 persistent tag 的对象由 OSS 生命周期清理
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup_failed_experiments with TTL {ttl_minutes} minutes... ")
        now = utc_time()
        threshold = now - timedelta(minutes=ttl_minutes)
        cleaned = 0
        total_errors = 0
        # 第一步:查询过期的失败 origin 记录
        failed_origins = (await session.exec(select(OriginImzMLInDB).where(
                    OriginImzMLInDB.status == ExperimentStatus.FAILED,
                    OriginImzMLInDB.created_at < threshold,).limit(batch_size).with_for_update(skip_locked=True))).all()
        # 立刻抽出所需字段为纯元组：per-file commit/rollback 会 expire 全部 ORM 实例，
        # 跨事务再访问 exp.* 会在异步上下文触发隐式 IO 报错
        failed_pairs = [(exp.id, exp.source_file_id) for exp in failed_origins]

        for exp_id, source_file_id in failed_pairs:
            if not source_file_id:
                # 关联源文件缺失（级联一致性防御）：跳过，origin 行保持原样
                logger.warning(
                    "cleanup_failed_experiments: origin has no source file, skip exp_id=%s",
                    exp_id,
                )
                continue
            try:
                # include_deleting=True：已处于删除流程的文件幂等重投而非 404
                file_record = await get_file_record_by_id(source_file_id, session, include_deleting=True)
                snapshot = await mark_file_for_deletion(session, None, file_record, unlink_all_owners=True)
                # 先 commit 再投递（顺序理由同 file_api.delete_user_file / clean_user_all_task）
                await session.commit()

                if snapshot is None:  # 防御：unlink_all_owners 模式不会返回 None
                    continue

                delete_objects_task.delay(  # type: ignore
                    kind="file",
                    resource_id=source_file_id,
                    paths=list(snapshot.paths),
                )
                cleaned += 1
                logger.info(
                    "cleanup_failed_experiments registered deletion exp_id=%s file_id=%s paths=%s",
                    exp_id, source_file_id, snapshot.paths,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                await session.rollback()
                total_errors += 1
                logger.error(
                    "cleanup_failed_experiments failed exp_id=%s source_file_id=%s error=%s",
                    exp_id, source_file_id, exc,
                )

        if cleaned or total_errors:
            logger.info("cleanup_failed_experiments completed registered=%s errors=%s", cleaned, total_errors)
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
    # 1. 删除 OSS 对象（幂等，可无限重试）
    ok = await delete_oss_objects(paths)
    if not ok:
        await _mark_delete_failed(kind, resource_id)
        return False

    # 2. finalize：同一 DB 事务内收尾
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
        # 告警用的对外标识（仅 file 类有，run 的对外标识就是 run_id）：
        # 必须在 commit 之前取出——commit 会 expire 实例，之后再访问属性
        # 会在异步上下文触发隐式 IO 报错
        alert_public_id: str | None = None
        if kind == "file":
            record = (await session.exec(
                select(FileInDB)
                .where(FileInDB.file_id == resource_id)
                .with_for_update()
            )).first()
            deleting_states = (FileStatus.DELETING, FileStatus.DELETE_FAILED)
            if record is not None:
                alert_public_id = record.public_id
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
            # 告警只挂「DELETING → DELETE_FAILED」跃迁一次：重投时行已是
            # DELETE_FAILED，不重发，跃迁即天然去重（删除架构现状说明 §8）
            first_failure = record.status == deleting_states[0]
            if kind == "file":
                record.status = FileStatus.DELETE_FAILED
            else:
                record.status = ProcessingStatus.DELETE_FAILED
            session.add(record)
            await session.commit()
            if first_failure:
                # fire-and-forget 告警，失败不影响状态机
                notify_delete_failed(kind, resource_id, public_id=alert_public_id)
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
