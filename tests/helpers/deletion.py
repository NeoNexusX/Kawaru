"""
删除状态位场景测试的共用构件（场景 ②③④，见 tests/test_deletion_*.py）。

- sweep_file_rows_by_prefix: 各场景开头清扫上次运行残留的文件行 + OSS 对象
- insert_completed_run: 直插一条 COMPLETED run 并按生产约定回填 out_path、放真实对象
- wait_db_row_gone: 轮询等待 worker finalize 删行（「要么都删除」的 DB 侧收敛）
"""
from __future__ import annotations

import asyncio
import os
import time

from sqlmodel import select

from api_services.composite_service import collect_file_delete_snapshot
from background.file_background_helper import delete_oss_objects
from database.db import get_standalone_async_session
from models.experiment_models import OriginImzMLInDB
from models.file_models import FileInDB
from models.processing_models import RAW_CONVERT_MARKER, ExperimentProcessRunInDB, ProcessingStatus
from models.user_models import UserInDB
from storage.oss.oss_storage import OssStorage
from tools.logger import get_logger

from .oss import put_oss_object

logger = get_logger("test.helpers.deletion")


async def sweep_file_rows_by_prefix(filename_prefix: str) -> int:
    """硬删文件名以 prefix 开头的全部 file 行（任意状态），并按删除快照口径删净其 OSS 对象。

    供各删除场景测试开头清扫上次运行残留；行删按 FK 级联 origin / run / UserFileLink。
    残留对象在模拟回调后已被打 persistent tag，bucket 生命周期规则不回收，
    必须按快照路径显式删除。
    """
    paths: list[str] = []
    async with get_standalone_async_session() as session:
        rows = (await session.exec(
            select(FileInDB).where(FileInDB.filename.startswith(filename_prefix))
        )).all()
        for row in rows:
            try:
                # 复用删除快照收集逻辑（主文件/packup 推导/预览图/origin/全部 run 输出）
                snapshot = await collect_file_delete_snapshot(session, row)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("sweep: collect snapshot failed file_id=%s error=%s", row.file_id, exc)
                snapshot = None
            if snapshot is not None:
                paths.extend(snapshot.paths)
            await session.delete(row)
        await session.commit()
    if paths:
        await delete_oss_objects(list(dict.fromkeys(paths)))
    if rows:
        logger.info("sweep: hard-deleted %d leftover file row(s) with prefix %s", len(rows), filename_prefix)
    return len(rows)


async def insert_completed_run(
    *,
    file_id: int,
    user_id: int,
    output_size: int,
    object_count: int = 8,
    wire_default_run: bool = True,
) -> dict:
    """直插一条 COMPLETED run，out_path 按生产约定（prepare_run_output_path）回填，
    并向前缀下传真实对象供 worker 真实清理。

    真实算法 run 无法对随机数据产出（FC 谱图校验必失败，见 test_deletion_flow
    模块 docstring），run 行只能直插；out_path 两步回填与生产「先建行、后写路径」
    的时序一致。params_json 置 RAW_CONVERT_MARKER 并把 file.default_run_id 指向
    该 run（wire_default_run=True 时），供场景 ② 断言 finalize 清 default_run_id。

    配额口径：真实链路中 processed 配额在 run 完成回调时计入用户 processed_file_size，
    删 run 的 finalize 再减回（refund 是减法）。直插行没有完成回调，这里同步把
    output_size 计入 user.processed_file_size 模拟该笔占用，否则 finalize 的退还
    只会撞 GREATEST(x-output_size, 0) 下限、断言无从成立。

    Returns:
        dict: run_id / out_path / keys（前缀下已上传的对象键列表）
    """
    async with get_standalone_async_session() as session:
        origin = (await session.exec(
            select(OriginImzMLInDB).where(OriginImzMLInDB.source_file_id == file_id)
        )).first()
        assert origin is not None and origin.id is not None, (
            f"origin row missing for file_id={file_id}: "
            "insert_completed_run must be called after upload_completed_file"
        )
        run = ExperimentProcessRunInDB(
            experiment_id=origin.id,
            source_file_id=file_id,
            user_id=user_id,
            status=ProcessingStatus.COMPLETED,
            params_json=RAW_CONVERT_MARKER if wire_default_run else None,
            output_size=output_size,
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)
        run_id = run.id
        assert run_id is not None
        out_path = OssStorage.prepare_run_output_path(run_id)
        run.out_path = out_path
        session.add(run)

        if wire_default_run:
            file_row = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )).first()
            assert file_row is not None, f"file row missing file_id={file_id}"
            file_row.default_run_id = run_id
            session.add(file_row)

        # 模拟 run 完成回调的配额计入（finalize 退还时才能减回去，见 docstring）
        user_row = (await session.exec(
            select(UserInDB).where(UserInDB.id == user_id))).first()
        assert user_row is not None, f"user row missing user_id={user_id}"
        user_row.processed_file_size = (user_row.processed_file_size or 0) + output_size
        session.add(user_row)
        await session.commit()

    keys = [f"{out_path}chunk_{i}" for i in range(object_count)]
    for key in keys:
        await put_oss_object(oss_path=key, data=os.urandom(2048))
    logger.info("insert_completed_run ok: run_id=%s out_path=%s objects=%d", run_id, out_path, len(keys))
    return {"run_id": run_id, "out_path": out_path, "keys": keys}


async def wait_db_row_gone(
    model: type,
    *where,
    timeout: float = 30,
    interval: float = 1,
    desc: str = "",
) -> None:
    """轮询 DB 等 worker 完成 OSS 删除 + finalize 删行（核心不变式的 DB 侧收敛）。

    登记接口（文件/run 删除、删用户）返回只代表登记事务已提交并投递任务，
    OSS 删除与删行在 worker 异步完成，必须轮询等待。
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        async with get_standalone_async_session() as session:
            row = (await session.exec(select(model).where(*where))).first()
        if row is None:
            logger.info("row gone after %.1fs (%s)", time.monotonic() - start, desc)
            return
        status = getattr(row, "status", None)
        logger.info("waiting for deletion to converge (%s status=%s, %.1fs elapsed)...",
                    desc, status, time.monotonic() - start)
        await asyncio.sleep(interval)
    raise AssertionError(f"deletion did not converge within {timeout}s ({desc}); "
                         f"check worker logs for delete_objects_task execution")
