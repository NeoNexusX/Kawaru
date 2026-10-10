"""
删除状态位场景 ④：beat 清失败实验（直插失败 origin → 触发 beat 任务 → 断言收敛）

test_cleanup_failed_experiments_flow  [deletion]  清扫残留 → 直插 file + FAILED origin +
                                                  双持有者 link + run（全部真实对象）→
                                                  cleanup_failed_experiments(ttl=0) →
                                                  断言登记/收尾两段配额语义与全链路收敛

验证删除状态位改造（docs/zh/删除架构现状说明.md 场景 4，场景 1 的批量版）的核心承诺：
  ★1 unlink_all_owners：全部持有者的 storage 配额在登记事务内释放（用户可见语义
     登记时生效——beat 任务返回即已提交，无需等 worker）
  ★2 文件行由 worker 收敛删除（级联 origin / run / link）
  ★3 processed 配额由 finalize 退还（登记时不动，行删时退）
  ★4 OSS 全部对象删净（主文件 + origin + run 输出）
  ★5 幂等重投语义由任务自身的 include_deleting=True 分支保证，重复触发即重投

数据构造取舍（与场景 ②③ 一致）：被测对象是 beat 任务本身，文件不需要 COMPLETED
（任务扫的是 origin.status=FAILED），故 file/origin/link/run 全部直插、对象直传，
不依赖上传链路。ttl_minutes=0 会扫到环境中全部过期 FAILED origin——开发库无
需保全数据（已确认）、CI 每次全新建库；断言只锚定本测试造的行，不碰全局计数。

注意：origin.created_at 显式回拨 1 分钟——部分 DB DATETIME 秒级精度会把
「刚刚」四舍五入到未来，导致 created_at < now 的扫描条件漏掉本行。

前置：
  - 服务器 + celery worker 在跑（任务经 .delay 投递，与 beat 触发同执行路径）
  - OSS_ACCESS_KEY_ID/SECRET 可用
  - 数据库已迁移到 head（含 delete_marked_at 列）

运行方式:
    uv run pytest tests/test_deletion_failed_experiments.py -v
    uv run pytest -m deletion -v
"""
from __future__ import annotations

import os
import secrets
from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlmodel import select

from background.file_background import cleanup_failed_experiments
from celery_runtime.result import wait_for_result
from database.db import get_standalone_async_session
from models.experiment_models import ExperimentStatus, OriginImzMLInDB
from models.file_models import FileInDB, FileStatus
from models.link_models import UserFileLink
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus
from models.user_models import UserInDB
from storage.oss.oss_storage import OssStorage
from tools import utc_time
from .conftest import logger
from .helpers.accounts import get_user_id_by_username, signup_and_login, sweep_users_by_prefix
from .helpers.deletion import sweep_file_rows_by_prefix, wait_db_row_gone
from .helpers.oss import put_oss_object

# 专用前缀（文件名/用户名）
DELETION_EXP_FILENAME = "test_del_failed_exp"
DELETION_EXP_USER_PREFIX = "delexp_owner"
DELETION_EXP_PASSWORD = "DelExp123456!"
# 文件 size = imzml + ibd（与真实回调口径一致：size 为压缩前总大小）
DELETION_IMZML_SIZE = 256 * 1024
DELETION_IBD_SIZE = 256 * 1024
FILE_SIZE = DELETION_IMZML_SIZE + DELETION_IBD_SIZE
RUN_OUTPUT_SIZE = 2048
CONVERGE_MAX_WAIT = 30
RUN_OBJECT_COUNT = 3


async def _read_user_quota(user_id: int) -> tuple[int, int, int]:
    """读用户 (file_count, total_file_size, processed_file_size)，配额断言用。"""
    async with get_standalone_async_session() as session:
        row = (await session.exec(
            select(UserInDB).where(UserInDB.id == user_id)
        )).first()
    assert row is not None, f"user row missing user_id={user_id}"
    return row.file_count, row.total_file_size, row.processed_file_size


@pytest.mark.deletion
async def test_cleanup_failed_experiments_flow(async_client: AsyncClient):
    """beat 清失败实验：登记退 storage / finalize 退 processed / 全对象删净（★1-★5）"""
    client = async_client

    # 0. 清扫上次运行残留
    await sweep_file_rows_by_prefix(DELETION_EXP_FILENAME)
    await sweep_users_by_prefix(DELETION_EXP_USER_PREFIX)

    # 1. 两个真实用户作持有者（unlink_all_owners 逐个退配额的断言对象）
    suffix = secrets.token_hex(3)
    name_a = f"{DELETION_EXP_USER_PREFIX}_a_{suffix}"
    name_b = f"{DELETION_EXP_USER_PREFIX}_b_{suffix}"
    await signup_and_login(client, username=name_a, email=f"{name_a}@example.com",
                           password=DELETION_EXP_PASSWORD)
    await signup_and_login(client, username=name_b, email=f"{name_b}@example.com",
                           password=DELETION_EXP_PASSWORD)
    user_a_id = await get_user_id_by_username(name_a)
    user_b_id = await get_user_id_by_username(name_b)
    assert user_a_id is not None and user_b_id is not None

    # 2. 直插 file + FAILED origin + 双持有者 link + run（对象路径先按主键拼好）
    file_verify_code = secrets.token_hex(16)
    filename = f"{DELETION_EXP_FILENAME}_{suffix}"
    async with get_standalone_async_session() as session:
        file_row = FileInDB(
            filename=filename,
            path=f"data/{file_verify_code}/exp_{suffix}.zip",
            status=FileStatus.FAILED,
            storage_type="oss",
            size=FILE_SIZE,
            file_verify_code=file_verify_code,
            file_type="zip",
            experiment_type="imzML",
        )
        session.add(file_row)
        await session.commit()
        await session.refresh(file_row)
        file_id = file_row.file_id
        assert file_id is not None
        # commit 会 expire 全部 ORM 实例（异步上下文访问过期属性会触发隐式 IO 报错，
        # 同 cleanup_failed_experiments 内 failed_pairs 的处理），所需标量立刻落地为局部变量
        main_key = file_row.path

        # created_at 显式回拨（见模块 docstring 的 DATETIME 精度说明）
        imzml_path = f"origin/{file_verify_code}/{file_id}/{filename}.imzML"
        ibd_path = f"origin/{file_verify_code}/{file_id}/{filename}.ibd"
        origin = OriginImzMLInDB(
            experiment_name=filename,
            source_file_id=file_id,
            status=ExperimentStatus.FAILED,
            error_message="deletion-test fixture",
            imzml_path=imzml_path,
            imzml_size=DELETION_IMZML_SIZE,
            ibd_path=ibd_path,
            ibd_size=DELETION_IBD_SIZE,
            created_at=utc_time() - timedelta(minutes=1),
        )
        session.add(origin)
        await session.commit()
        await session.refresh(origin)
        assert origin.id is not None
        # commit 会 expire 全部 ORM 实例（expire_on_commit），跨 commit 访问属性会
        # 触发隐式 IO 报错（同 cleanup_failed_experiments 内 failed_pairs 的处理），
        # 日志所需的标量紧跟各自的 refresh 落成普通 int
        origin_id = origin.id

        session.add(UserFileLink(user_id=user_a_id, file_id=file_id))
        session.add(UserFileLink(user_id=user_b_id, file_id=file_id))
        run = ExperimentProcessRunInDB(
            experiment_id=origin.id,
            source_file_id=file_id,
            user_id=user_a_id,
            status=ProcessingStatus.COMPLETED,
            output_size=RUN_OUTPUT_SIZE,
        )
        session.add(run)
        await session.commit()
        await session.refresh(run)
        assert run.id is not None
        run.out_path = OssStorage.prepare_run_output_path(run.id)
        session.add(run)
        run_out = run.out_path
        run_id = run.id

        # 配额写死已知值：storage 由登记退（★1），processed 由 finalize 退（★3）
        user_a = (await session.exec(
            select(UserInDB).where(UserInDB.id == user_a_id))).first()
        user_b = (await session.exec(
            select(UserInDB).where(UserInDB.id == user_b_id))).first()
        user_a.file_count = 1
        user_a.total_file_size = FILE_SIZE
        user_a.processed_file_size = RUN_OUTPUT_SIZE
        user_b.file_count = 1
        user_b.total_file_size = FILE_SIZE
        user_b.processed_file_size = 0
        session.add(user_a)
        session.add(user_b)
        await session.commit()

    assert run_out is not None
    run_keys = [f"{run_out}chunk_{i}" for i in range(RUN_OBJECT_COUNT)]

    # 3. 传真实对象（主文件 + origin + run 输出）
    for key in (main_key, imzml_path, ibd_path, *run_keys):
        await put_oss_object(oss_path=key, data=os.urandom(2048))
    logger.info("fixture ready: file_id=%s origin_id=%s run_id=%s", file_id, origin_id, run_id)

    # 4. 触发 beat 任务（ttl=0 扫全部过期 FAILED origin，与 beat 触发同一 worker 执行路径；
    #    cleaned 可能包含环境中其他过期 FAILED origin，断言只锚定本测试的行）
    result = cleanup_failed_experiments.delay(ttl_minutes=0, batch_size=100)
    cleaned = await wait_for_result(result, poll_interval=0.5, timeout=60)
    assert isinstance(cleaned, int) and cleaned >= 1, (
        f"cleanup task reported {cleaned!r} cleaned — our FAILED origin was not scanned?"
    )
    logger.info("beat task completed, cleaned=%d (registration committed)", cleaned)

    # 5. ★1 任务返回即登记已提交：两持有者 storage 配额已释放（unlink_all_owners）
    for name, user_id in ((name_a, user_a_id), (name_b, user_b_id)):
        file_count, total_size, _ = await _read_user_quota(user_id)
        assert file_count == 0, f"{name}: file_count not refunded at registration: {file_count}"
        assert total_size == 0, f"{name}: total_file_size not refunded at registration: {total_size}"
    logger.info("★1 storage quota refunded for both owners at registration time")

    # 6. ★2 等 worker 删净 OSS 并 finalize 删文件行
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == file_id,
        timeout=CONVERGE_MAX_WAIT, desc=f"file_id={file_id}",
    )
    logger.info("★2 file row finalized")

    # 7. ★3 processed 配额由 finalize 退还（登记时不动）
    _, _, processed_a = await _read_user_quota(user_a_id)
    _, _, processed_b = await _read_user_quota(user_b_id)
    assert processed_a == 0, f"processed quota not refunded via finalize: {processed_a}"
    assert processed_b == 0, f"user_b processed quota changed unexpectedly: {processed_b}"
    logger.info("★3 processed quota refunded via finalize")

    # 8. 行级联：origin / run 都没了；用户仍在（只 unlink 不删用户）
    async with get_standalone_async_session() as session:
        origin_row = (await session.exec(
            select(OriginImzMLInDB).where(OriginImzMLInDB.source_file_id == file_id))).first()
        run_row = (await session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.source_file_id == file_id))).first()
    assert origin_row is None, "origin row must be cascade-deleted with file row"
    assert run_row is None, "run row must be cascade-deleted with file row"
    assert await get_user_id_by_username(name_a) is not None, "owner users must survive unlink"
    assert await get_user_id_by_username(name_b) is not None
    logger.info("cascade verified: origin/run rows gone, users alive")

    # 9. ★4 OSS 全部对象删净
    for key in (main_key, imzml_path, ibd_path, *run_keys):
        assert not await OssStorage.check_file_exist(key), f"object still exists: {key}"
    logger.info("★4 all %d objects gone from OSS", 3 + len(run_keys))

    # 10. 收尾：删除本测试创建的两个用户（成功路径自清；中断残留由下次运行开头 sweep 兜底）
    await sweep_users_by_prefix(name_a)
    await sweep_users_by_prefix(name_b)
    logger.info("test_cleanup_failed_experiments_flow passed: file_id=%s", file_id)
