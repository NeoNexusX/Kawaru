from datetime import timedelta

from sqlmodel import select, func


from api_helpers.admin_notify import notify_delete_failed
from api_helpers.deletion_helper import (
    DELETING_FILE_STATUSES,
    DELETING_RUN_STATUSES,
    file_is_deleting,
    run_is_deleting,
    visible_files_clause,
)
from api_services.composite_service import (
    begin_file_deletion,
    finalize_file_deletion,
    finalize_run_deletion,
    mark_run_for_deletion,
)
from api_services.processing_service import get_process_run_by_id
from api_services.user_service import get_user_by_id
from api_services.file_service import (
    get_file_record_by_id,
    safe_cleanup_preflight_record,
    unlink_user_from_file,
)
from storage.oss.oss_storage import OssStorage, delete_oss_objects
from constants.file_constants import (
    PREFLIGHT_TTL_MINUTES,
    DOWNLOAD_ZIP_TTL_MINUTES,
)
from constants.file_constants import STALE_PROCESS_RUN_TIMEOUT_MINUTES, STALE_CLUSTERING_RUN_TIMEOUT_MINUTES
from constants.file_constants import STALE_DELETION_TIMEOUT_MINUTES
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

    清理条件(uploaded_at 超过 TTL, 且 path 为空——预检后回调从未把路径落库)。

    刻意不看 status：回调失败留下的记录里，path 已有值的那条路（先 commit 落库再打
    persistent tag，tag 失败才回滚）必然是 file + origin 同事务双置 FAILED，由
    cleanup_failed_experiments 整份删除——它会连 OSS 对象一起删，把这条路径正好兜住
    （只删 DB 行会让**已打上 persistent tag 的部分对象永久泄漏**）；其余失败都回滚在
    回调首次 commit 之前，path 仍为空，上面的条件已覆盖。

    本任务只清数据库记录与用户额度，**不主动删除 OSS 对象**——未打 persistent tag 的
    孤儿对象由 bucket 生命周期规则回收。

    Returns:
        清理的记录数量
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup stale preflights for all users with TTL {ttl_minutes} minutes...")

        # 代表 uploaded_at 超过 TTL 的时间点
        now = utc_time()
        cutoff_time = now - timedelta(minutes=ttl_minutes)
        total_cleaned = 0

        # 查询所有过期预检记录：path 为空（回调从未落库）通过 UserFileLink 关联获取用户
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


async def dispatch_user_file_deletions(session, deleted_user: UserInDB) -> dict:
    """对被删用户名下**剩余**的文件逐个登记 + 投递删除任务（每个文件独立事务，互不拖累）。

    被两个入口共用：clean_user_all_task（即时加速）与 cleanup_deleted_users（每轮兜底）。
    因此必须幂等、可反复调用：
    - 已在删除流程的文件 → 走 begin_file_deletion 的幂等分支重收集快照重投
      （兜住卡在 DELETE_FAILED 的）
    - 尚未登记的文件 → 正常登记（兜住 clean_user_all_task 投递失败、文件从没进过流程的）
    - 还有别的持有者 → 只解除被删用户的关联（文件留给其他人）

    run 输出路径不需要预收集：user 行不删，run 行不会级联消失，快照按 source_file_id
    反查即可。

    Returns: {"files_unlinked": n, "files_deleted": n, "errors": n}
    """
    assert deleted_user.id is not None
    user_id = deleted_user.id
    file_ids = (await session.exec(select(UserFileLink.file_id).where(UserFileLink.user_id == user_id))).all()

    total_unlinked = 0
    total_deleted = 0
    total_errors = 0

    for file_id in file_ids:
        if file_id is None:
            continue
        try:
            # 回血：上一轮的 commit / rollback 都会把 deleted_user expire
            # 触发隐式懒加载，直接抛 MissingGreenlet
            await session.refresh(deleted_user)
            # include_deleting=True：已处于删除流程的文件走幂等分支而非 404
            file_record = await get_file_record_by_id(file_id, session, include_deleting=True)
            snapshot = await begin_file_deletion(session, file_record, current_user=deleted_user)
            # 先 commit 再投递（顺序理由见 file_api.delete_user_file）
            await session.commit()
            await session.refresh(file_record)
            await session.refresh(deleted_user)

            if snapshot is None:
                # 还有其他持有者：只解除被删用户的关联（文件留给其他人）
                await unlink_user_from_file(session, deleted_user, file_record)
                await session.commit()
                total_unlinked += 1
                continue

            delete_objects_task.delay(  # type: ignore
                kind="file",
                resource_id=file_id,
                paths=list(snapshot.paths),
            )
            total_deleted += 1
        except Exception as exc:  # pylint: disable=broad-exception-caught
            await session.rollback()
            total_errors += 1
            logger.error(
                "dispatch_user_file_deletions failed user_id=%s file_id=%s error=%s", user_id, file_id, exc)

    return {"files_unlinked": total_unlinked, "files_deleted": total_deleted, "errors": total_errors}


@celery_app.task()  # type: ignore
async def clean_user_all_task(deleted_user_id: int) -> dict:
    """管理员删除用户后的**即时**清理（加速路径）：立刻把该用户名下文件登记 + 投递。

    兜底由 cleanup_deleted_users 每轮重跑同一函数承担，所以本任务投递失败不致命。
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting clean_user_all_task for deleted_user_id={deleted_user_id}")
        # 被删用户仍需用于 unlink（按 id 查，不按 active 过滤）
        deleted_user = await get_user_by_id(deleted_user_id, session)
        if deleted_user is None:
            logger.warning("clean_user_all_task: user not found user_id=%s, abort", deleted_user_id)
            return {"deleted_user_id": deleted_user_id, "files_unlinked": 0, "files_deleted": 0, "errors": 0}
        result = await dispatch_user_file_deletions(session, deleted_user)
    result["deleted_user_id"] = deleted_user_id
    logger.info("clean_user_all_task completed %s", result)
    return result


@celery_app.task()  # type: ignore
async def cleanup_deleted_users(batch_size: int = 100) -> int:
    """收敛已删除用户：名下资源清空后物理删其 user 行（删除入口见 api/user_api.del_user）。

    **按 active=False 扫描**——本系统里 active=False 就是"已删除"（见 UserBase.active
    的注释），它不是"停用"；若将来引入停用功能并复用该字段，本任务会删光那些用户的
    数据，务必另加字段。

    独立定时任务而非挂在文件 finalize 上——两者的失败域必须隔离：用户收敛里要给
    「他人文件上的 run」投递删除任务，broker 不可达时不该把文件收尾一起回滚。

    每个用户独立事务，判定顺序:
    1. **先对名下剩余文件重跑一遍登记 + 投递**（dispatch_user_file_deletions，幂等）。
       这一步是兜底：`clean_user_all_task` 投递失败时文件从没进过删除流程，只靠"还有
       link 就跳过"会永久死锁；卡在 DELETE_FAILED 的也靠它重投
    2. 名下还有 file（UserFileLink 未消失）→ 说明文件刚投递、worker 还没删完，等下一轮
    3. 名下还有 run → 此时必然在**他人文件**上（自己的文件已删，其上的 run 已级联
       消失），走 A2 三段式后跳过
    4. 都没有 → 删 user 行（级联清掉残留 link）

    注意: 查询带 limit 且无 ORDER BY，长期卡在 2/3 的用户会占用批次额度。
    """
    async with get_standalone_async_session() as session:
        # 先抽成纯 id：循环内 commit 会 expire ORM 实例，之后再访问属性会触发隐式 IO
        user_ids = (await session.exec(
            select(UserInDB.id).where(UserInDB.active == False)  # type: ignore # pylint: disable=singleton-comparison,no-member
            .limit(batch_size))).all()

        finalized = 0
        for user_id in user_ids:
            if user_id is None:
                continue
            try:
                # ① 重跑登记 + 投递（按 id 查用户：已删用户不按 active 过滤）
                deleted_user = await get_user_by_id(user_id, session)
                if deleted_user is None or deleted_user.active:
                    continue
                await dispatch_user_file_deletions(session, deleted_user)

                # ② 重新取一次再判定残余：上一步逐文件 commit 会 expire 实例、也释放了行锁，
                #    这里用行锁把最终判定串行化
                deleted_user = (await session.exec(
                    select(UserInDB).where(UserInDB.id == user_id).with_for_update())).first()
                if deleted_user is None or deleted_user.active:
                    continue

                link_count = (await session.exec(
                    select(func.count()).where(UserFileLink.user_id == user_id))).one()  # pylint: disable=not-callable
                if link_count:
                    continue

                run_ids = (await session.exec(
                    select(ExperimentProcessRunInDB.id).where(ExperimentProcessRunInDB.user_id == user_id))).all()
                if run_ids:
                    for run_id in run_ids:
                        run_record = await get_process_run_by_id(session, run_id, include_deleting=True) # type: ignore
                        if run_record is None or run_is_deleting(run_record.status):
                            continue
                        snapshot = await mark_run_for_deletion(session, run_record)
                        await session.commit()
                        delete_objects_task.delay(  # type: ignore
                            kind="run", resource_id=run_id, paths=list(snapshot.paths),
                        )
                    continue

                await session.delete(deleted_user)
                await session.commit()
                finalized += 1
                logger.info("cleanup_deleted_users: finalized user_id=%s", user_id)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                await session.rollback()
                logger.error("cleanup_deleted_users failed user_id=%s error=%s", user_id, exc)

        logger.info("cleanup_deleted_users completed finalized=%s scanned=%s", finalized, len(user_ids))
    return finalized


@celery_app.task()  # type: ignore
async def retry_stale_file_deletions(
    timeout_minutes: int = STALE_DELETION_TIMEOUT_MINUTES,
    batch_size: int = 100,
) -> int:
    """重投停留过久的文件删除（扫 file 表）。**A1 唯一的自动兜底**。

    条件是 status ∈ {DELETING, DELETE_FAILED} 且 delete_marked_at 早于 timeout——
    加超时是为了不和正在跑的 worker 抢：刚登记几秒的行不该被重投。
    重投直接走 begin_file_deletion 的幂等分支（它负责重收集快照）。

    A4（cleanup_failed_experiments 清失败实验）登记之后的重试同样归这里：那边只负责
    发起（扫描已排除删除流程中的文件），不再自己重投。
    前提契约：进入删除流程的行必有 delete_marked_at（置位点唯一，
    composite_service.mark_file_for_deletion 同事务写 status 与时间戳）——
    本任务靠它判定「卡住」，NULL 的行会两个任务都捞不到。
    """
    async with get_standalone_async_session() as session:
        cutoff = utc_time() - timedelta(minutes=timeout_minutes)
        rows = (await session.exec(
            select(FileInDB)
            .where(FileInDB.status.in_(DELETING_FILE_STATUSES),  # type: ignore # pylint: disable=no-member
                   FileInDB.delete_marked_at < cutoff) # type: ignore # pylint: disable=no-member
            .limit(batch_size)
        )).all()

        retried = 0
        for row in rows:
            file_id = row.file_id
            try:
                snapshot = await begin_file_deletion(session, row)
                await session.commit()
                if snapshot is None:  # 防御：已在删除流程的行不会走判定分支
                    continue
                delete_objects_task.delay(  # type: ignore
                    kind="file", resource_id=file_id, paths=list(snapshot.paths))
                retried += 1
            except Exception as exc:  # pylint: disable=broad-exception-caught
                # 行已被并发 finalize 删除（本来要重投的目标已消失）也走这里，属正常
                await session.rollback()
                logger.info("retry_stale_file_deletions skipped file_id=%s error=%s", file_id, exc)

        if retried:
            logger.info("retry_stale_file_deletions re-dispatched %d file(s)", retried)
    return retried


@celery_app.task()  # type: ignore
async def retry_stale_run_deletions(
    timeout_minutes: int = STALE_DELETION_TIMEOUT_MINUTES,
    batch_size: int = 100,
) -> int:
    """重投停留过久的 run 删除（扫 run 表）。**A2 唯一的自动兜底**
    （run 类删除没有 admin 旁路，此前完全没有自动重投）。

    条件与文件侧完全同构，只是对象换成 run；走 mark_run_for_deletion 的幂等分支。
    """
    async with get_standalone_async_session() as session:
        cutoff = utc_time() - timedelta(minutes=timeout_minutes)
        rows = (await session.exec(
            select(ExperimentProcessRunInDB)
            .where(ExperimentProcessRunInDB.status.in_(DELETING_RUN_STATUSES),  # type: ignore # pylint: disable=no-member
                   ExperimentProcessRunInDB.delete_marked_at < cutoff) # type: ignore # pylint: disable=no-member
            .limit(batch_size)
        )).all()

        retried = 0
        for row in rows:
            run_id = row.id
            try:
                snapshot = await mark_run_for_deletion(session, row)
                await session.commit()
                delete_objects_task.delay(  # type: ignore
                    kind="run", resource_id=run_id, paths=list(snapshot.paths))
                retried += 1
            except Exception as exc:  # pylint: disable=broad-exception-caught
                await session.rollback()
                logger.info("retry_stale_run_deletions skipped run_id=%s error=%s", run_id, exc)

        if retried:
            logger.info("retry_stale_run_deletions re-dispatched %d run(s)", retried)
    return retried


@celery_app.task()  # type: ignore
async def cleanup_failed_experiments(
    ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES,
    batch_size: int = 100
) -> int:
    """
    清理过期的失败 origin 记录（场景 1 的批量版：删除状态位三件套）

    每个失败 origin 的源文件独立事务，互不拖累：begin_file_deletion(force=True) 登记
    （失败产物整份删除，不保留共享者引用；配额在 finalize 结算）→ 先 commit 再投递
    delete_objects_task。对象删失败则行置 DELETE_FAILED 等重投——重投归
    retry_stale_file_deletions，本任务只负责发起，不再扫已进入删除流程的文件。

    扫描条件：origin=FAILED 且早于 TTL，**且源文件未进入删除流程**，按 id 排序（FIFO）。
    force 不能省：不加则多持有者时返回 None → 文件不删但 origin 仍是 FAILED →
    每轮重扫且永不清理。失败文件几乎必然单持有者，唯"打 persistent tag 失败"那条路径
    存在 file 已 COMPLETED 且已提交的窄窗口，秒传可在其中建出第二条 link。

    另：行锁只护住本次查询事务（多 worker 会重复捞同一批，由 begin_file_deletion 的
    行锁 + DELETING 守卫幂等吸收）；include_deleting=True 只兜「扫描到处理之间被并发
    删除」的窗口；FC 未回调、路径未落库的孤儿对象由 OSS 生命周期回收。
    """
    async with get_standalone_async_session() as session:
        logger.info(f"starting cleanup_failed_experiments with TTL {ttl_minutes} minutes... ")
        now = utc_time()
        threshold = now - timedelta(minutes=ttl_minutes)
        cleaned = 0
        total_errors = 0
        # 已进入删除流程的文件不在此列：登记完即移交 retry_stale_file_deletions，
        # 再捞回来只会永久占满批次（它们的 origin 恒满足 FAILED + 早于 TTL，见 docstring）
        # JOIN 会连带锁 file 行（MariaDB 无 FOR UPDATE OF），SKIP LOCKED 不等待、首个 commit 即释放
        failed_origins = (await session.exec(
            select(OriginImzMLInDB)
            .join(FileInDB, FileInDB.file_id == OriginImzMLInDB.source_file_id)  # type: ignore[arg-type]
            .where(
                OriginImzMLInDB.status == ExperimentStatus.FAILED,
                OriginImzMLInDB.created_at < threshold,
                visible_files_clause(),
            )
            .order_by(OriginImzMLInDB.id)  # type: ignore[call-overload]  # FIFO，堵住老行长期占位
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )).all()
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
                snapshot = await begin_file_deletion(session, file_record, force=True)
                # 先 commit 再投递（顺序理由同 file_api.delete_user_file / clean_user_all_task）
                await session.commit()

                if snapshot is None:  # 防御：force=True 不会返回 None
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
        # deleting_status 用于判断是否首次失败（DELETING → DELETE_FAILED）还是重复投递（DELETE_FAILED → DELETE_FAILED）
        alert_public_id: str | None = None
        if kind == "file":
            record = (await session.exec(
                select(FileInDB)
                .where(FileInDB.file_id == resource_id)
                .with_for_update()
            )).first()
            if record is not None:
                alert_public_id = record.public_id
            deleting_status = FileStatus.DELETING
        elif kind == "run":
            record = (await session.exec(
                select(ExperimentProcessRunInDB)
                .where(ExperimentProcessRunInDB.id == resource_id)
                .with_for_update()
            )).first()
            deleting_status = ProcessingStatus.DELETING
        else:
            logger.error("_mark_delete_failed: unknown kind=%s resource_id=%s", kind, resource_id)
            return

        is_deleting = (file_is_deleting if kind == "file" else run_is_deleting)
        if record is not None and is_deleting(record.status):
            # 告警只挂「DELETING → DELETE_FAILED」
            # DELETE_FAILED，不重发，
            first_failure = record.status == deleting_status
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
