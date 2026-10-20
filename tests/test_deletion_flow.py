"""
删除状态位流程测试（完整上传链路 → 删除 → 断言收敛）

test_delete_file_flow  [deletion]  清扫残留 → preflight → STS 直传 → 等 FC 解压
                                    → 模拟回调 → 删除 → 关键断言 → finally 清理

验证删除状态位改造（场景 1，用户删文件）的核心承诺：
  ★1 删除接口秒回（不等 OSS 删除完成）
  ★2 读侧拦截生效（DELETING 行按 404 处理）
  ★3 配额在 finalize 才退（登记阶段不动配额）
  ★4 fileindb 行最终被 worker 删除（「要么都删除」DB 侧）
  ★5 OSS 对象最终删净（「要么都删除」OSS 侧，即不变式
     「DB 行已删 ⟹ OSS 已确认删完」）

预览图（FC 生成 images/{public_id}/）不测、忽略：随机字节数据无法通过 FC 的
imzML 谱图校验（pyimzml 解析必失败），预览图不会生成；且 images 前缀已由
删除快照的推导路径兜底，生成与否不影响删除收敛。

与 oss 组测试的隔离：
  - 独立文件名（DELETION_TEST_FILENAME），残留互不影响
  - 测试开头清扫同文件名残留（含上次失败遗留的 DELETING 行）
  - zip 内容用 os.urandom 生成，hash 每次天然唯一，秒传/重复哈希检查不会命中

前置：
  - 服务器 + celery worker 在跑（worker 不在则删除会停在 DELETING，★4 超时）
  - FC 解压函数可用（完整上传链路依赖 bucket 事件触发解压）
  - 数据库已迁移到 head（含 fileindb.delete_marked_at 列）

运行方式:
    uv run pytest tests/test_deletion_flow.py -v     # 单跑本文件
    uv run pytest -m deletion -v                      # 按 marker 跑
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Tuple

import pytest
from httpx import AsyncClient
from sqlmodel import select

from tools.cal_hash import compute_imzml_md5
from storage.oss.oss_storage import OssStorage
from database.db import get_standalone_async_session
from api_services.file_service import refund_storage_quota_for_file
from constants.file_constants import OSS_ORIGIN_STORAGE_ROOT
from models.file_models import FileInDB, FileStatus
from .conftest import logger
from .helpers.oss import parse_file_id_from_oss_path, upload_file_to_oss
from .helpers.uploads import build_preflight_payload, make_test_zip_to_local

# 回调验证 token：本地从 .env 读，CI 用 compose 默认值
FC_CALLBACK_TOKEN = os.environ.get("FC_CALLBACK_TOKEN", "test-fc-callback-token")

# 删除测试专用文件名（与 oss 组的 TEST_FILENAME 隔离，残留互不影响）
DELETION_TEST_FILENAME = "test_delete_flow"
# 小尺寸文件（256KB × 2）：上传与 FC 解压都快，删除流程本身与文件大小无关
DELETION_IMZML_SIZE = 256 * 1024
DELETION_IBD_SIZE = 256 * 1024

# FC 解压轮询参数（同 test_oss）
FC_POLL_INTERVAL = 5
FC_MAX_WAIT = 120

# 删除收敛轮询参数：worker 执行 delete_objects_task（删全部 OSS 对象 + finalize 删行）
CONVERGE_POLL_INTERVAL = 1
CONVERGE_MAX_WAIT = 30
# 删除接口耗时上限：新流程只做一次 DB 事务 + 任务投递，应远小于旧流程的
# 同步等待 OSS 删除（celery 往返 + OSS 删除通常 2 秒以上）
DELETE_API_MAX_SECONDS = 2.0


async def _sweep_leftovers(client: AsyncClient, auth: dict) -> None:
    """清扫上次运行残留的同名文件行，保证本次 preflight 不被重名检查拒绝。

    已处于 DELETING / DELETE_FAILED 的行硬删前补退一次配额（配额在 finalize 才退，
    残留行尚未退，不补会跨轮次漂移）；仍有 owner 的行走 HTTP DELETE。
    """
    linked_public_ids: list[str] = []
    async with get_standalone_async_session() as session:
        rows = (await session.exec(
            select(FileInDB).where(FileInDB.filename == DELETION_TEST_FILENAME)
        )).all()
        for row in rows:
            if row.status in (FileStatus.DELETING, FileStatus.DELETE_FAILED):
                await refund_storage_quota_for_file(session, row)
                await session.delete(row)
                logger.info("sweep: hard-deleted leftover row file_id=%s status=%s", row.file_id, row.status)
            else:
                linked_public_ids.append(row.public_id)
        await session.commit()

    for public_id in linked_public_ids:
        rs = await client.delete(f"/files/{public_id}", headers=auth)
        assert rs.status_code == 200, f"sweep delete failed: public_id={public_id} body={rs.text}"
        logger.info("sweep: deleted linked leftover public_id=%s status=%s", public_id, rs.json().get("status"))


async def _wait_fc_unzip(file_id: int, file_verify_code: str, imzml_path: str, ibd_path: str) -> None:
    """轮询 OSS 等待 FC 解压完成（origin imzML/ibd 就绪），超时报错。"""
    logger.info("polling OSS for FC unzip (max %ds, interval %ds)...", FC_MAX_WAIT, FC_POLL_INTERVAL)
    start = time.monotonic()
    while time.monotonic() - start < FC_MAX_WAIT:
        imzml_ready = await OssStorage.check_file_exist(imzml_path)
        ibd_ready = await OssStorage.check_file_exist(ibd_path)
        if imzml_ready and ibd_ready:
            logger.info("FC unzip complete after %.1fs (file_id=%s)", time.monotonic() - start, file_id)
            return
        await asyncio.sleep(FC_POLL_INTERVAL)
    raise AssertionError(
        f"FC unzip did not complete within {FC_MAX_WAIT}s for file_id={file_id}: "
        f"expected origin files at {imzml_path} / {ibd_path}"
    )


async def _wait_row_gone(file_id: int) -> None:
    """轮询 DB 等 worker 完成 OSS 删除 + finalize 删行（核心不变式的 DB 侧）。"""
    start = time.monotonic()
    while time.monotonic() - start < CONVERGE_MAX_WAIT:
        async with get_standalone_async_session() as session:
            row = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )).first()
        if not row:
            logger.info("file row finalized after %.1fs (file_id=%s)", time.monotonic() - start, file_id)
            return
        logger.info("waiting for deletion to converge (file_id=%s status=%s, %.1fs elapsed)...",
                    file_id, row.status, time.monotonic() - start)
        await asyncio.sleep(CONVERGE_POLL_INTERVAL)
    raise AssertionError(
        f"deletion did not converge within {CONVERGE_MAX_WAIT}s for file_id={file_id}: "
        f"check worker logs for delete_objects_task execution"
    )


@pytest.mark.deletion
async def test_delete_file_flow(admin_login: Tuple[AsyncClient, str, str]):
    """完整上传一个文件 → 删除 → 断言接口语义与最终一致性（见模块 docstring ★1-★5）"""
    client, token, token_type = admin_login
    auth = {"Authorization": f"{token_type.title()} {token}"}

    # 0. 清扫上次运行残留（同名行会挡住 preflight 的重名检查）
    await _sweep_leftovers(client, auth)

    zip_path: Path | None = None
    try:
        # 1. 造测试 zip（urandom → hash 天然唯一；FC 谱图校验必然拒绝，
        #    预览图不会生成——该路径忽略，见模块 docstring）
        zip_path, imzml_hash, ibd_hash = make_test_zip_to_local(
            imzml_size=DELETION_IMZML_SIZE,
            ibd_size=DELETION_IBD_SIZE,
            filename=DELETION_TEST_FILENAME,
        )
        file_verify_code, size = await compute_imzml_md5(zip_path)  # type: ignore

        # 2. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=build_preflight_payload(
                size=size,
                file_verify_code=file_verify_code,
                storage_type="oss",
                filename=DELETION_TEST_FILENAME,
            ),
        )
        assert rs.status_code == 200, rs.text
        pre_public_id = rs.json()["public_id"]
        logger.info("preflight ok, pre_public_id=%s", pre_public_id)

        # 3. /files/upload 拿 STS → 直传 OSS
        rs = await client.post(
            "/files/upload",
            headers=auth,
            data={"pre_public_id": pre_public_id, "filename": DELETION_TEST_FILENAME},
        )
        assert rs.status_code == 200, rs.text
        body = rs.json()
        # 内部自增 file_id 不得对外暴露（可枚举），对外标识一律用 public_id
        assert "file_id" not in body, "upload must not expose file_id"
        public_id = body["public_id"]
        oss_sts_token = body["oss_sts_token"]
        oss_path = body["oss_path"]
        oss_bucket = body["oss_bucket"]
        assert public_id == pre_public_id
        # 拼 origin 路径、模拟 FC 回调、查 DB 行需要内部 file_id：与真实 FC 一样从对象键解析
        file_id = parse_file_id_from_oss_path(oss_path)

        await upload_file_to_oss(
            oss_sts_token=oss_sts_token,
            oss_bucket=oss_bucket,
            oss_path=oss_path,
            filepath=zip_path,
            checkpoint_dir=zip_path.parent / "checkpoint",
        )
        logger.info("uploaded to oss, public_id=%s file_id=%s oss_path=%s", public_id, file_id, oss_path)

        # 4. 等 FC 解压完成（origin 对象就绪）
        stem = Path(DELETION_TEST_FILENAME).stem
        origin_root = OSS_ORIGIN_STORAGE_ROOT.rstrip("/")
        imzml_path = f"{origin_root}/{file_verify_code}/{file_id}/{stem}.imzML"
        ibd_path = f"{origin_root}/{file_verify_code}/{file_id}/{stem}.ibd"
        await _wait_fc_unzip(file_id, file_verify_code, imzml_path, ibd_path)

        # 5. 模拟 FC 回调 → 文件 COMPLETED
        rs = await client.post(
            "/callback/oss_upload_callback",
            json={
                "file_id": file_id,
                "file_path": oss_path,
                "is_success": True,
                "act_hash": file_verify_code,
                "imzml_path": imzml_path,
                "ibd_path": ibd_path,
                "imzml_size": DELETION_IMZML_SIZE,
                "ibd_size": DELETION_IBD_SIZE,
            },
            headers={"X-FC-Token": FC_CALLBACK_TOKEN},
        )
        assert rs.status_code == 200, f"FC callback failed: {rs.status_code} {rs.text}"
        assert rs.json()["status"] == "completed"
        logger.info("callback ok, file_id=%s status=completed", file_id)

        # 6. 配额基线
        rs = await client.get("/users/quota", headers=auth)
        assert rs.status_code == 200, rs.text
        quota_before = rs.json()

        # 7. ★1 删除接口秒回（新流程只做登记事务 + 投递，不等 OSS 删除）
        start = time.perf_counter()
        rs = await client.delete(f"/files/{public_id}", headers=auth)
        elapsed = time.perf_counter() - start
        assert rs.status_code == 200, rs.text
        assert rs.json()["public_id"] == public_id, rs.text
        assert rs.json()["status"] == "deleted", rs.text
        assert elapsed < DELETE_API_MAX_SECONDS, (
            f"delete API took {elapsed:.2f}s (> {DELETE_API_MAX_SECONDS}s): "
            f"looks like it waited for OSS deletion — async registration broken?"
        )
        logger.info("★1 delete returned in %.3fs with status=deleted", elapsed)

        # 8. ★2 读侧拦截：行还在 DELETING，metadata 应 404
        #    带持有者登录态：确保 404 来自 DELETING 拦截，而不是私有文件对匿名的权限闸
        rs = await client.get(f"/files/{public_id}/metadata", headers=auth)
        assert rs.status_code == 404, (
            f"expected 404 for DELETING file, got {rs.status_code}: {rs.text}"
        )
        logger.info("★2 metadata correctly returns 404 while DELETING")

        # 9. ★3 配额在 finalize 才退：行仍处 DELETING 时配额应原封不动。
        #    worker 若已抢先 finalize，行与配额必然一起消失（同事务），此时跳过本断言
        async with get_standalone_async_session() as session:
            row = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )).first()
        rs = await client.get("/users/quota", headers=auth)
        assert rs.status_code == 200, rs.text
        quota_during = rs.json()
        if row is not None:
            assert quota_during["file_count"] == quota_before["file_count"], (
                f"file_count refunded before finalize: before={quota_before['file_count']} "
                f"now={quota_during['file_count']}"
            )
            assert quota_during["total_uploaded_size_bytes"] == quota_before["total_uploaded_size_bytes"], (
                f"total_uploaded_size refunded before finalize: "
                f"before={quota_before['total_uploaded_size_bytes']} now={quota_during['total_uploaded_size_bytes']}"
            )
            logger.info("★3 quota still held while DELETING (refund happens at finalize)")
        else:
            logger.info("★3 row already finalized before quota probe; skipping the 'still held' check")

        # 10. ★4 等 worker 删完全部 OSS 对象并 finalize 删行
        await _wait_row_gone(file_id)
        logger.info("★4 file row finalized (OSS delete + DB row delete both done)")

        # 10b. ★3（续）配额已随 finalize 退还
        rs = await client.get("/users/quota", headers=auth)
        assert rs.status_code == 200, rs.text
        quota_after = rs.json()
        assert quota_after["file_count"] == quota_before["file_count"] - 1, (
            f"file_count not refunded at finalize: "
            f"before={quota_before['file_count']} after={quota_after['file_count']}"
        )
        assert quota_after["total_uploaded_size_bytes"] == quota_before["total_uploaded_size_bytes"] - size, (
            f"total_uploaded_size not refunded at finalize: "
            f"before={quota_before['total_uploaded_size_bytes']} after={quota_after['total_uploaded_size_bytes']} expected_delta={size}"
        )
        logger.info("★3 quota refunded at finalize: file_count=%s total_size=%s",
                    quota_after["file_count"], quota_after["total_uploaded_size_bytes"])

        # 11. ★5 OSS 对象删净（行已删 ⟹ OSS 已删，不变式保证此刻对象必然不在）
        assert not await OssStorage.check_file_exist(oss_path), (
            f"main zip still exists in OSS after row finalized: {oss_path}"
        )
        assert not await OssStorage.check_file_exist(imzml_path), (
            f"origin imzML still exists in OSS after row finalized: {imzml_path}"
        )
        assert not await OssStorage.check_file_exist(ibd_path), (
            f"origin ibd still exists in OSS after row finalized: {ibd_path}"
        )
        logger.info("★5 OSS objects all gone: zip + imzML + ibd verified")

        logger.info("test_delete_file_flow passed: public_id=%s file_id=%s imzml_hash=%s ibd_hash=%s",
                    public_id, file_id, imzml_hash, ibd_hash)

    finally:
        # 兜底清理：本地临时 zip 与解压产物；DB/OSS 残留（如断言失败时卡在
        # DELETING 的行）无需处理——下次运行开头的 _sweep_leftovers 会清扫
        if zip_path is not None:
            zip_path.unlink(missing_ok=True)
            import shutil
            shutil.rmtree(zip_path.parent / "extract_tmp", ignore_errors=True)
            shutil.rmtree(zip_path.parent / "checkpoint", ignore_errors=True)
