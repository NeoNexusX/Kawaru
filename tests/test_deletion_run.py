"""
删除状态位场景 ②：删除算法/聚类 run（完整上传 → 直插 run → 删除 → 断言收敛）

test_delete_run_flow  [deletion]  清扫残留 → 真实上传链（无 FC，见 helpers/uploads
                                  .upload_completed_file）→ 直插 COMPLETED run + 真实
                                  out_path 对象 → DELETE /processes/{run_id} → 断言

验证删除状态位改造（场景 2）的核心承诺：
  ★1 删除接口秒回（登记事务 + 投递，不等 OSS 删除）
  ★2 读侧拦截：runs 列表立即不再展示该 run；zarr 端点按不存在处理（404）
  ★3 处理次数配额在 finalize 才结算（run 的用户可见语义与行同生共死，
     行在则未退、行删则已退）
  ★4 run 行最终被 worker 删除（「要么都删除」DB 侧）
  ★5 直转 run 不占处理次数：删除 raw_convert run 不改变 processing_count_used
     （非直转 run 经 run 级删除恰好退 1 次由本文件的
     test_delete_algorithm_run_refunds_processing_count 覆盖；文件级路径的退还
     见场景 ④ test_deletion_failed_experiments）
  ★6 finalize 清 default_run_id（raw_convert run 且仍被引用时）
  ★7 OSS out_path 前缀删净；文件本身（主 zip / origin）不受 run 删除影响
  附  重复删除即手动重试入口（DELETING 中重复调用幂等分支 200）

与真实链路的取舍：真实算法 run 无法对随机数据产出（FC 谱图校验必失败），
run 行直插、out_path 按生产约定回填、对象真实上传——快照收集 / worker 删除 /
finalize 全部真实执行；文件侧走真实上传链（helpers/uploads.upload_completed_file）。

前置（同 test_deletion_flow）：
  - 服务器 + celery worker 在跑（worker 不在则删除停在 DELETING，★4 超时）
  - OSS_ACCESS_KEY_ID/SECRET 可用（helpers/oss.put_oss_object 直传任意键）
  - 数据库已迁移到 head（含 delete_marked_at 列）

运行方式:
    uv run pytest tests/test_deletion_run.py -v     # 单跑本文件
    uv run pytest -m deletion -v                      # 按 marker 跑
"""
from __future__ import annotations

import time
from typing import Tuple

import pytest
from httpx import AsyncClient
from sqlmodel import select

from constants.file_constants import OSS_ORIGIN_STORAGE_ROOT
from database.db import get_standalone_async_session
from models.file_models import FileInDB, FileStatus
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus
from storage.oss.oss_storage import OssStorage
from .conftest import TEST_USER, logger
from .helpers.accounts import get_user_id_by_username
from .helpers.deletion import insert_completed_run, sweep_file_rows_by_prefix, wait_db_row_gone
from .helpers.uploads import upload_completed_file

# 删除测试专用文件名前缀（与 oss / 场景 1/3/4 组隔离；sweep 按前缀清扫残留）
DELETION_RUN_FILENAME = "test_del_run_flow"
# 非直转 run 退还用例的前缀（与上面的直转用例隔离，避免两个用例互相清扫）
DELETION_ALGO_RUN_FILENAME = "test_del_algo_run_flow"
# 小尺寸文件（256KB × 2）：上传与回调都快，删除流程本身与文件大小无关
DELETION_IMZML_SIZE = 256 * 1024
DELETION_IBD_SIZE = 256 * 1024
# 直插 run 的输出大小与前缀下对象数（处理次数配额按 run 计，不按大小）
RUN_OUTPUT_SIZE = 4096
RUN_OBJECT_COUNT = 8

# 删除接口耗时上限：新流程只做一次 DB 事务 + 任务投递
DELETE_API_MAX_SECONDS = 2.0
CONVERGE_MAX_WAIT = 30


@pytest.mark.deletion
async def test_delete_run_flow(admin_login: Tuple[AsyncClient, str, str]):
    """真实上传文件 → 直插 COMPLETED run → 删除 → 断言接口语义与最终一致性（见模块 docstring）"""
    client, token, token_type = admin_login
    auth = {"Authorization": f"{token_type.title()} {token}"}

    # 0. 清扫上次运行残留（任意状态的行级联清 run/origin，OSS 对象按快照显式删）
    await sweep_file_rows_by_prefix(DELETION_RUN_FILENAME)

    # 管理员内部 user_id（run.user_id 必填，对外接口不暴露，从 DB 反查）
    admin_id = await get_user_id_by_username(TEST_USER)
    assert admin_id is not None, f"admin user row not found for TEST_USER={TEST_USER}"

    # 1. 真实上传链造 COMPLETED 文件（文件行/origin 行由服务端真实代码创建）
    up = await upload_completed_file(
        client, auth,
        filename=DELETION_RUN_FILENAME,
        imzml_size=DELETION_IMZML_SIZE,
        ibd_size=DELETION_IBD_SIZE,
    )

    # 2. 直插 COMPLETED run：out_path 按生产约定回填 + 真实对象；
    #    params_json=__RAW_CONVERT__ 且 file.default_run_id 指向它（★6 的前提）
    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=admin_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=RUN_OBJECT_COUNT,
    )
    run_id, run_keys = run_info["run_id"], run_info["keys"]

    # 3. 基线：直插 run 出现在 runs 列表；处理次数配额基线
    rs = await client.post("/processes/mine", headers=auth, json={})
    assert rs.status_code == 200, rs.text
    assert run_id in [row["id"] for row in rs.json()["data"]], "inserted run must be listed before deletion"
    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    quota_before = rs.json()

    # 4. ★1 删除接口秒回
    start = time.perf_counter()
    rs = await client.delete(f"/processes/{run_id}", headers=auth)
    elapsed = time.perf_counter() - start
    assert rs.status_code == 200, rs.text
    assert rs.json() == {"run_id": run_id, "status": "deleted"}, rs.text
    assert elapsed < DELETE_API_MAX_SECONDS, (
        f"delete API took {elapsed:.2f}s (> {DELETE_API_MAX_SECONDS}s): "
        f"looks like it waited for OSS deletion — async registration broken?"
    )
    logger.info("★1 delete returned in %.3fs with status=deleted", elapsed)

    # 5. ★3 处理次数配额在 finalize 才结算。
    #    读序防竞态：先读配额、再复读行——结算与删行在同一 finalize 事务提交，
    #    复读仍是 DELETING ⟹ finalize 未提交 ⟹ 刚才的配额读数必是登记后的值。
    #    复读行已不在（worker 极快收敛）则跳过该中途断言，最终态由 ★5 保证
    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    quota_mid = rs.json()
    async with get_standalone_async_session() as session:
        row = (await session.exec(
            select(ExperimentProcessRunInDB).where(ExperimentProcessRunInDB.id == run_id)
        )).first()
    if row is not None and row.status == ProcessingStatus.DELETING:
        assert quota_mid["processing_count_used"] == quota_before["processing_count_used"], (
            "processing count must NOT be refunded at registration time (finalize-only semantics)"
        )
        logger.info("★3 processing count not refunded while DELETING (finalize-only)")
    else:
        logger.info("row already finalized before mid-flight quota check; ★3 skipped")

    # 6. 附：重复删除即手动重试入口——行仍在删除流程中时幂等分支应 200。
    #    worker 可能在本检查与请求之间抢先 finalize（合法竞态）：此时确认行确已
    #    删除即可收敛验证；4xx 以外的异常状态仍视为失败
    async with get_standalone_async_session() as session:
        row = (await session.exec(
            select(ExperimentProcessRunInDB).where(ExperimentProcessRunInDB.id == run_id)
        )).first()
    if row is not None:
        rs = await client.delete(f"/processes/{run_id}", headers=auth)
        if rs.status_code == 200:
            logger.info("idempotent re-delete returned 200 (row still in deletion flow)")
        else:
            async with get_standalone_async_session() as session:
                row_after = (await session.exec(
                    select(ExperimentProcessRunInDB).where(ExperimentProcessRunInDB.id == run_id)
                )).first()
            assert row_after is None and rs.status_code in (400, 404), (
                f"idempotent re-delete failed unexpectedly: {rs.status_code} {rs.text}"
            )
            logger.info("worker finalized before re-delete landed; convergence verified instead")
    else:
        logger.info("worker finalized before idempotent re-delete check; skipping")

    # 7. ★2 读侧拦截：列表立即不再展示（DELETING 过滤，或行已删）；zarr 端点 404
    rs = await client.post("/processes/mine", headers=auth, json={})
    assert rs.status_code == 200, rs.text
    assert run_id not in [row["id"] for row in rs.json()["data"]], (
        "run in deletion flow must disappear from runs list immediately"
    )
    logger.info("★2 runs list no longer contains run_id=%s", run_id)
    rs = await client.get(f"/processes/{run_id}/zarr", headers=auth)
    assert rs.status_code == 404, f"zarr access must 404 for DELETING/gone run, got {rs.status_code}: {rs.text}"

    # 8. ★4 等 worker 删净 out_path 前缀并 finalize 删行
    await wait_db_row_gone(
        ExperimentProcessRunInDB, ExperimentProcessRunInDB.id == run_id,
        timeout=CONVERGE_MAX_WAIT, desc=f"run_id={run_id}",
    )
    logger.info("★4 run row finalized")

    # 9. ★5 直转 run 不占处理次数，删除后 processing_count_used 不变。
    #    （算法 run 的退 1 次由 test_deletion_failed_experiments 的文件删除路径覆盖）
    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    quota_after = rs.json()
    assert quota_after["processing_count_used"] == quota_before["processing_count_used"], (
        "raw_convert run must not consume or refund processing count: "
        f"before={quota_before['processing_count_used']} "
        f"after={quota_after['processing_count_used']}"
    )
    logger.info("★5 raw_convert run did not change processing count (%s)",
                quota_after["processing_count_used"])

    # 10. ★6 finalize 清 default_run_id；★7 文件本身不受 run 删除影响
    async with get_standalone_async_session() as session:
        file_row = (await session.exec(
            select(FileInDB).where(FileInDB.file_id == up["file_id"])
        )).first()
    assert file_row is not None, "file row must survive run deletion"
    assert file_row.default_run_id is None, (
        f"default_run_id must be cleared after raw_convert run finalized, got {file_row.default_run_id}"
    )
    assert file_row.status == FileStatus.COMPLETED, f"file status changed: {file_row.status}"
    logger.info("★6 default_run_id cleared; file untouched (status=%s)", file_row.status)

    # 11. ★7 OSS：run 前缀删净；主 zip 与 origin 对象仍在
    for key in run_keys:
        assert not await OssStorage.check_file_exist(key), f"run output still exists: {key}"
    assert await OssStorage.check_file_exist(up["oss_path"]), f"main zip must survive: {up['oss_path']}"
    assert await OssStorage.check_file_exist(up["imzml_path"]), f"origin imzML must survive: {up['imzml_path']}"
    assert await OssStorage.check_file_exist(up["ibd_path"]), f"origin ibd must survive: {up['ibd_path']}"
    logger.info("★7 run out_path cleaned (%d objects); file objects intact", len(run_keys))

    # 12. 兜底：测试自身收尾——文件行已无用处，走正常用户删除（顺带多验一次场景 1 链路）
    rs = await client.delete(f"/files/{up['public_id']}", headers=auth)
    assert rs.status_code == 200, f"file cleanup delete failed: {rs.text}"
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"cleanup file_id={up['file_id']}",
    )
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"]):
        assert not await OssStorage.check_file_exist(key), f"cleanup file object still exists: {key}"
    logger.info("test_delete_run_flow passed: run_id=%s file_id=%s", run_id, up["file_id"])


@pytest.mark.deletion
async def test_delete_algorithm_run_refunds_processing_count(admin_login: Tuple[AsyncClient, str, str]):
    """非直转 COMPLETED run 走 run 级删除：finalize 恰好退还 1 次处理次数。

    覆盖 finalize_run_deletion → run_counts_toward_quota=True → refund_processing_count，
    与 test_delete_run_flow 的 ★5（直转 run 不退）互补。
    """
    client, token, token_type = admin_login
    auth = {"Authorization": f"{token_type.title()} {token}"}

    # 0. 清扫残留 + 真实上传链造 COMPLETED 文件
    await sweep_file_rows_by_prefix(DELETION_ALGO_RUN_FILENAME)
    admin_id = await get_user_id_by_username(TEST_USER)
    assert admin_id is not None, f"admin user row not found for TEST_USER={TEST_USER}"
    up = await upload_completed_file(
        client, auth,
        filename=DELETION_ALGO_RUN_FILENAME,
        imzml_size=DELETION_IMZML_SIZE,
        ibd_size=DELETION_IBD_SIZE,
    )

    # 1. 配额基线在直插之前读：helper 直插非直转 run 时会模拟完成回调 +1
    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    quota_before = rs.json()["processing_count_used"]

    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=admin_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=RUN_OBJECT_COUNT,
        wire_default_run=False,
    )
    run_id, run_keys = run_info["run_id"], run_info["keys"]

    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    assert rs.json()["processing_count_used"] == quota_before + 1, (
        "helper must simulate the completion-callback +1: "
        f"before={quota_before} after={rs.json()['processing_count_used']}"
    )

    # 2. 删除 → 等 worker 删净 out_path 并 finalize 删行
    rs = await client.delete(f"/processes/{run_id}", headers=auth)
    assert rs.status_code == 200, rs.text
    await wait_db_row_gone(
        ExperimentProcessRunInDB, ExperimentProcessRunInDB.id == run_id,
        timeout=CONVERGE_MAX_WAIT, desc=f"algorithm run_id={run_id}",
    )

    # 3. 恰好退还 1 次（多退 = 重复退；没退 = 永久虚占）
    rs = await client.get("/users/quota", headers=auth)
    assert rs.status_code == 200, rs.text
    quota_after = rs.json()["processing_count_used"]
    assert quota_after == quota_before, (
        "processing count must be refunded exactly once: "
        f"before={quota_before} after={quota_after}"
    )
    logger.info("algorithm run refunded exactly 1 processing count: before=%s after=%s",
                quota_before, quota_after)

    # 4. OSS out_path 前缀删净
    for key in run_keys:
        assert not await OssStorage.check_file_exist(key), f"run output still exists: {key}"

    # 5. 收尾：文件行走正常用户删除（run 行已不在，文件级 finalize 不会再退一次）
    rs = await client.delete(f"/files/{up['public_id']}", headers=auth)
    assert rs.status_code == 200, f"file cleanup delete failed: {rs.text}"
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"cleanup file_id={up['file_id']}",
    )
    logger.info("test_delete_algorithm_run_refunds_processing_count passed: run_id=%s", run_id)
