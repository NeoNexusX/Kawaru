#########################
# Composite Service - 编排层
# 职责：跨 file / experiment / processing 三个 service 的编排逻辑。
#
# 为什么需要这一层：
# file_service 与 processing_service 都在顶层 import experiment_service，
# experiment_service 处于依赖最底层，它反向引用 file_service / processing_service
# 会形成循环，原先靠函数内延迟导入（import-outside-toplevel）绕开。
# 把这类"同时操作多个 service 记录"的编排函数上移到本层后，依赖方向始终
# 自上而下（composite -> file/experiment/processing），不再有反向引用，可全部
# 顶层 import，消除延迟导入。
#
# 约定：本层只做编排（组合多个 service 调用 + 事务控制），不新增单表 CRUD 逻辑；
#      单表的增删改查仍归各自的 service。
#########################
from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

from sqlmodel import select, delete
from sqlmodel.ext.asyncio.session import AsyncSession

from api_services.file_service import (
    refund_storage_quota_for_file,
    update_file_record,
)
from api_services.experiment_service import (
    get_origin_imzml_by_file_id,
    update_origin,
)
from api_services.processing_service import (
    refund_processed_quota,
    refund_processed_quota_for_file,
)
from api_helpers.deletion_helper import file_is_deleting, run_is_deleting
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from exceptions.file_exceptions import file_not_found_exception
from models.collection_models import CollectionMember
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus, RAW_CONVERT_MARKER
from models.file_models import FileInDB, FileStatus
from models.link_models import UserFileLink
from models.user_models import UserInDB

from tools import utc_time
from tools.logger import get_logger

logger = get_logger("api_services.composite_service")


async def failed_mark_origin_record_clean(
    async_session: AsyncSession,
    origin_record: OriginImzMLInDB | None,
    source_file: FileInDB | None,
    e: None | Exception = None,
    f_status: FileStatus = FileStatus.FAILED,
    o_status: ExperimentStatus = ExperimentStatus.FAILED,
) -> None:
    """
    标记对应的数据为失败状态，并不清理文件。

    同时更新 file 与 origin 两条记录为 FAILED，并在同一事务内提交、刷新。
    跨 file_service / experiment_service 的编排逻辑集中在此，避免在
    experiment_service 内反向 import file_service 造成循环依赖。

    若异常发生在成功分支第一次 commit 之后，三个 OSS 路径已经落库，
    后续 rollback 不会抹掉；若异常更早发生，则数据库不保证有完整路径。
    本函数只标 FAILED，不补写路径，也不删除对象。已有路径由
    cleanup_failed_experiments 尝试删除，其余对象依赖 OSS 生命周期规则。
    """
    # update file status to failed
    assert async_session is not None
    if source_file:
        await update_file_record(async_session, source_file, status=f_status)
    # update origin status to failed
    if origin_record:
        await update_origin(async_session, origin_record, status=o_status,
                            error_message=str(e))


#########################
# 删除状态位 - 登记 / 快照 / 收尾（OSS 先删、DB 后删、失败标记）
# 见 docs/zh/删除状态位改造方案.md；场景 1（用户删文件）先行落地
#########################

class FileDeleteSnapshot(NamedTuple):
    """文件删除快照：登记时收集、随任务参数传递；兜底重投时重新收集"""
    file_id: int
    paths: list[str]        # 待删除的 OSS 对象/目录前缀


async def collect_file_delete_snapshot(
    session: AsyncSession,
    file_record: FileInDB,
) -> FileDeleteSnapshot:
    """
    收集删除一个文件所需的全部路径快照。

    登记与兜底重投共用；调用方必须持有 file 行锁后调用，保证快照与状态位一致。
    覆盖：主文件（path）、预览图、origin（imzML/ibd + 目录）、全部 run 的输出目录
    （run 行随 file 级联删除，不提前收集会成为孤儿对象）。
    """
    file_id = file_record.file_id
    assert file_id is not None
    paths: list[str] = []

    # 主文件：跳过空路径（未完成上传的预检记录）与「已清理」哨兵
    main_path = (file_record.path or "").strip()
    if main_path and main_path != SOURCE_FILE_PATH_CLEARED:
        paths.append(main_path)

    if file_record.image_path:
        paths.append(file_record.image_path)
    # 预览图目录按约定命名推导（image_dest 约定见 processing_background）：
    # FC 可能在删除登记之后才写回 image_path，仅凭 DB 字段会漏删成孤儿；
    # 新约定 images/{public_id}/：public_id 随机前缀避免 OSS 写入热点；
    # 旧约定（file_id 取模 1000 分片）过渡期保留，兜住发布时仍在途的旧格式
    # 预览任务（OSS 删除 204 幂等，多推无副作用），确认无旧任务后可移除
    paths.append(f"images/{file_record.public_id}/")
    paths.append(f"images/{file_id % 1000:03d}/file_{file_id}/")

    origin = await get_origin_imzml_by_file_id(session, file_id)
    if origin:
        if origin.imzml_path:
            paths.append(origin.imzml_path)
        if origin.ibd_path:
            paths.append(origin.ibd_path)
        if origin.imzml_path:
            paths.append(f"{str(Path(origin.imzml_path).parent)}/")

    runs = (await session.exec(
        select(ExperimentProcessRunInDB).where(
            ExperimentProcessRunInDB.source_file_id == file_id))).all()
    for run in runs:
        if run.out_path:
            paths.append(run.out_path)

    # 保序去重（image_path 与推导的预览图目录可能重复）
    return FileDeleteSnapshot(
        file_id=file_id,
        paths=list(dict.fromkeys(paths)),
    )


async def mark_file_for_deletion(
    session: AsyncSession,
    file_record: FileInDB,
) -> FileDeleteSnapshot:
    """置 DELETING + 摘集合成员 + 收路径快照。纯准备，不改任何用户可见状态。

    调用方须已持有 file 行锁，且已确认该文件可进入删除流程。
    """
    assert file_record.file_id is not None

    file_record.status = FileStatus.DELETING
    file_record.delete_marked_at = utc_time()
    session.add(file_record)

    # 集合成员也是用户可见引用，与置位同事务摘掉
    await session.exec(
        delete(CollectionMember).where(CollectionMember.file_id == file_record.file_id)  # type: ignore # pylint: disable=no-member
    )

    snapshot = await collect_file_delete_snapshot(session, file_record)
    logger.info(
        "marked file for deletion file_id=%s paths=%s",
        file_record.file_id, snapshot.paths,
    )
    return snapshot


async def begin_file_deletion(
    session: AsyncSession,
    file_record: FileInDB,
    *,
    current_user: UserInDB | None = None,
    force: bool = False,
) -> FileDeleteSnapshot | None:
    """① 登记文件删除（调用方 commit）：行锁 → 幂等分支 → 判定持有者 → 置位 + 收快照。

    唯一持有者时不解除关联：link 随 finalize 删行级联，配额也在那时结算。
    行锁串行化并发删除，顺带修掉「最后两个 owner 并发删除」的 TOCTOU。

    Args:
        current_user: 发起删除的用户；None 表示关联已由调用方解除，只统计剩余持有者
        force: 忽略持有者判定，无条件进入删除流程（beat 清失败实验）

    Returns:
        FileDeleteSnapshot: 已置 DELETING，调用方须**先 commit 再投递** delete_objects_task
            （反过来先投后 commit 的崩溃会留下「对象已删、行未标记」的不可自愈形态）
        None: 还有其他持有者，未进入删除流程。**调用方负责**处理该分支（用户删文件时
            即"退出共享"，自行 unlink）。行锁持有到事务结束，处理须落在同一事务内
    """
    assert file_record.file_id is not None

    locked = (await session.exec(
        select(FileInDB)
        .where(FileInDB.file_id == file_record.file_id)
        .with_for_update()
    )).first()
    if not locked:
        # 并发窗口内行已被 finalize 删除
        logger.info("file row gone before marking deletion file_id=%s", file_record.file_id)
        raise file_not_found_exception

    if file_is_deleting(locked.status):
        # 已在删除流程：不重复解除 / 扣配额，直接返回快照供重新投递
        # （DELETE_FAILED 也走此分支：管理员重复删除即为手动重试入口）
        logger.info("file already marked for deletion file_id=%s status=%s", locked.file_id, locked.status)
        return await collect_file_delete_snapshot(session, locked)

    if not force:
        # 加锁读（当前读）：普通 SELECT 读的是请求起始快照，会把并发中已解除的持有者
        # 误判为仍在——最后两个 owner 并发删除会双双「退出共享」，留下 0 持有者孤儿
        owner_ids = (await session.exec(
            select(UserFileLink.user_id)
            .where(UserFileLink.file_id == locked.file_id)
            .with_for_update()
        )).all()
        current_id = current_user.id if current_user is not None else None
        if any(uid is not None and uid != current_id for uid in owner_ids):
            # 除 current_user 外还有别人持有 → 不进入删除流程，交给调用方处理
            return None

    return await mark_file_for_deletion(session, locked)


async def finalize_file_deletion(
    session: AsyncSession,
    file_id: int,
) -> bool:
    """删除收尾（worker 在 OSS 全删成功后于独立事务中调用）：
    退全部持有者的 storage 配额 + 全部 run 的 processed 配额（都必须在删行前读），
    然后删 file 行（级联 origin / UserFileLink / run）。

    容忍行已不在：退配额与删行同事务，行不在则整个收尾跳过，配额不会重复退。
    """
    file_record = (await session.exec(
        select(FileInDB)
        .where(FileInDB.file_id == file_id)
        .with_for_update()
    )).first()
    if not file_record:
        logger.info("finalize file deletion: row already gone file_id=%s", file_id)
        return True

    await refund_storage_quota_for_file(session, file_record)
    await refund_processed_quota_for_file(session, file_id)
    await session.delete(file_record)
    logger.info("finalized file deletion file_id=%s", file_id)
    return True


#########################
# 删除状态位 - 场景 2：删除算法/聚类 run
# 与场景 1 的差异：run 无 owner/unlink 语义，用户可见语义（删行、退配额、
# 清 default_run_id）全部放在 finalize 与行同生共死，天然幂等不重复退
#########################

class RunDeleteSnapshot(NamedTuple):
    """run 删除快照：登记时收集、随任务参数传递；兜底重投时重新收集"""
    run_id: int
    paths: list[str]  # 待删除的 OSS 对象/目录前缀


async def collect_run_delete_snapshot(
    run_record: ExperimentProcessRunInDB,
) -> RunDeleteSnapshot:
    """
    收集删除一个 run 所需的路径快照。

    run 的全部产物（算法输出 zarr + 聚类结果写回内部）都在 out_path 一个
    目录前缀下，快照只有这一项；out_path 为空（PROCESSING 中被删、从未产出）
    时快照为空，worker 直接 finalize。
    """
    run_id = run_record.id
    assert run_id is not None
    paths = [run_record.out_path] if run_record.out_path else []
    return RunDeleteSnapshot(run_id=run_id, paths=paths)


async def mark_run_for_deletion(
    session: AsyncSession,
    run_record: ExperimentProcessRunInDB,
) -> RunDeleteSnapshot:
    """
    登记删除算法/聚类 run（① 登记事务，调用方 commit）：
    行锁抢占 → 置 DELETING → 收集路径快照。

    与 begin_file_deletion 不同：无 unlink / 配额操作——run 的用户可见
    语义（删行、退 processed 配额、清 default_run_id）全部在 finalize
    同一事务内完成，行在则未退、行删则已退，天然幂等。

    Returns:
        RunDeleteSnapshot: 已置 DELETING（含 DELETING/DELETE_FAILED 的幂等重入，
        直接重收集快照供重新投递——重复删除即手动重试入口），调用方必须
        **先 commit 再投递** delete_objects_task（顺序理由同场景 1）。

    重投入口的限制：DELETE /processes/{run_id} 无 admin 旁路（assert_run_ownership
    只认 owner），所以 run 类失败的重投仅限该 run 的 owner 本人——管理员删不动，
    只能等 owner 操作，或等兜底任务上线（见删除架构现状说明 §6）。
    """
    assert run_record.id is not None

    # 行锁抢占
    locked = (await session.exec(
        select(ExperimentProcessRunInDB)
        .where(ExperimentProcessRunInDB.id == run_record.id)
        .with_for_update()
    )).first()
    if not locked:
        logger.info("run row gone before marking deletion run_id=%s", run_record.id)
        raise file_not_found_exception

    if run_is_deleting(locked.status):
        # 已在删除流程中：幂等重入，直接返回快照供重新投递
        logger.info("run already marked for deletion run_id=%s status=%s", locked.id, locked.status)
        return await collect_run_delete_snapshot(locked)

    locked.status = ProcessingStatus.DELETING
    locked.delete_marked_at = utc_time()
    session.add(locked)

    snapshot = await collect_run_delete_snapshot(locked)
    logger.info(
        "marked run for deletion run_id=%s paths=%s",
        locked.id, snapshot.paths,
    )
    return snapshot


async def finalize_run_deletion(
    session: AsyncSession,
    run_id: int,
) -> bool:
    """
    删除收尾（② finalize，worker 在 OSS 全部删除成功后于独立事务中调用）：
    清 default_run_id（若删的是 raw_convert run 且仍被引用）→ 退还 processed
    配额 → 删 run 行，同一事务完成。

    容忍行已不在（重复投递 / 兜底与 worker 并发时直接算成功：退配额与删行
    同事务，行不在则整个收尾跳过，配额不会重复退）。

    与文件级 finalize 并发时（两条链路同时收尾同一个 run），靠 run 行锁判定
    「谁退这个 run 的配额」——见 refund_processed_quota_for_file。
    """
    run_record = (await session.exec(
        select(ExperimentProcessRunInDB)
        .where(ExperimentProcessRunInDB.id == run_id)
        .with_for_update()
    )).first()
    if not run_record:
        logger.info("finalize run deletion: row already gone run_id=%s", run_id)
        return True

    # 删除的是 raw_convert 直转 run 且仍被 file 引用 → 清空 default_run_id
    # （file 行可能已随文件删除消失，容忍）
    if run_record.params_json == RAW_CONVERT_MARKER:
        file_record = (await session.exec(
            select(FileInDB).where(
                FileInDB.file_id == run_record.source_file_id)
        )).first()
        if file_record and file_record.default_run_id == run_record.id:
            file_record.default_run_id = None
            session.add(file_record)
            logger.info(
                "cleared default_run_id for file_id=%s (deleting raw_convert run_id=%s)",
                run_record.source_file_id, run_record.id,
            )

    # 退还用户已处理配额（原子操作；GREATEST 保证不低于 0）
    if run_record.output_size and run_record.output_size > 0 and run_record.user_id is not None:
        await refund_processed_quota(session, run_record.user_id, run_record.output_size)

    await session.delete(run_record)
    logger.info("finalized run deletion run_id=%s", run_id)
    return True
