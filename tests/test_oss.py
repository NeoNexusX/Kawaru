"""
OSS 测试集合（与本地上传测试区分开）

TestOss.test_upload_to_oss              [oss]   preflight -> 拿 STS -> SDK 直传 OSS
TestOss.test_file_metadata              [oss]   GET /files/{file_id}/metadata（公开接口，不依赖回调）
TestOss.test_simulate_fc_callback       [oss]   轮询 OSS 等待 FC 解压 → 模拟回调
TestOss.test_raw_download_metadata      [oss]   GET /files/{file_id}/download_raw（回调后直接验证）
TestOss.test_cleanup_oss_upload         [oss]   等 20s celery 收尾 → 清理 OSS 文件 + DB 记录 + 临时 zip

共用构件：
  - tests/helpers/uploads.py  build_preflight_payload / make_test_zip_to_local / TEST_FILENAME
  - tests/helpers/oss.py      upload_file_to_oss（STS 直传）

运行方式:
    uv run pytest -m oss            # 本地或 CI（需真实 OSS、Bucket 事件与解包 FC）
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Tuple

import pytest
from httpx import AsyncClient

from tools.cal_hash import compute_imzml_md5
from storage.oss.oss_storage import OssStorage
from constants.file_constants import OSS_ORIGIN_STORAGE_ROOT
from .conftest import logger
from .helpers.oss import upload_file_to_oss
from .helpers.uploads import (
    TEST_FILENAME,
    TEST_IMZML_SIZE,
    TEST_IBD_SIZE,
    build_preflight_payload,
    make_test_zip_to_local,
)

# 回调验证 token：本地从 .env 读，CI 用 compose 默认值
FC_CALLBACK_TOKEN = os.environ.get("FC_CALLBACK_TOKEN", "test-fc-callback-token")


class TestOss:
    """OSS 直传功能测试"""
    _uploaded_file_id: int | None = None
    _oss_path: str | None = None
    _file_verify_code: str | None = None
    _imzml_hash: str | None = None
    _ibd_hash: str | None = None

    @pytest.mark.oss
    async def test_upload_to_oss(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试上传到 OSS 的完整流程（preflight -> STS -> 直传），验证文件成功上传"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path, imzml_hash, ibd_hash = make_test_zip_to_local()
        file_verify_code, size = await compute_imzml_md5(zip_path) # type: ignore
        logger.info(f"test_upload_to_oss: file_verify_code={file_verify_code}, size={size}")

        # 1. preflight
        start_time_seconds = time.perf_counter()
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=build_preflight_payload(
                size=size,
                file_verify_code=file_verify_code,
                total_parts=1,
                storage_type="oss",
            ),
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(f"test_upload_to_oss: preflight ok, pre_file_id={pre_file_id}")

        # 2. get upload oss_sts_token and oss_path
        rs = await client.post(
            "/files/upload",
            headers=auth,
            data={"pre_file_id": str(pre_file_id), "filename": TEST_FILENAME},
        )
        assert rs.status_code == 200, rs.text
        file_id = rs.json()["file_id"]
        oss_sts_token = rs.json()["oss_sts_token"]
        oss_path = rs.json()["oss_path"]
        oss_bucket = rs.json()["oss_bucket"]

        # 3. 获取当前登录用户名
        rs = await client.get(
            "/user",
            headers=auth,
        )
        assert rs.status_code == 200, rs.text
        username = rs.json()["username"]

        # 4. 用 STS 临时凭证直传 OSS
        result = await upload_file_to_oss(
            oss_sts_token=oss_sts_token,
            oss_bucket=oss_bucket,
            oss_path=oss_path,
            filepath=zip_path,
            checkpoint_dir=zip_path.parent / "checkpoint",
        )
        fc_request_id = result.headers.get("x-oss-request-id")
        end_time_seconds = time.perf_counter()
        upload_time_seconds = round(end_time_seconds - start_time_seconds, 2)

        TestOss._uploaded_file_id = file_id
        TestOss._oss_path = oss_path
        TestOss._file_verify_code = file_verify_code
        TestOss._imzml_hash = imzml_hash
        TestOss._ibd_hash = ibd_hash
        logger.info((f"test_upload_to_oss passed: upload_time_seconds={upload_time_seconds} username={username} file_id={file_id} size={size} oss_path={oss_path}\n"
                     f"etag={result.etag} fc-request-id={fc_request_id} real-md5={file_verify_code}"))

    @pytest.mark.oss
    async def test_file_metadata(self, async_client: AsyncClient):
        """测试 GET /files/{file_id}/metadata（公开接口，无需鉴权）"""
        if TestOss._uploaded_file_id is None:
            pytest.skip("no uploaded file to test metadata against")

        file_id = TestOss._uploaded_file_id
        rs = await async_client.get(f"/files/{file_id}/metadata")
        assert rs.status_code == 200, f"metadata failed: {rs.text}"
        body = rs.json()
        assert body["file_id"] == file_id
        # 内容指纹（file_verify_code/MD5）不得对外泄露：匿名拿到即可通过预检秒传
        # 成为私有文件持有者（可改元数据、置公开、删除），响应必须不含该字段
        assert "file_verify_code" not in body, (
            "metadata must not expose file_verify_code: 防止秒传接管私有文件"
        )
        logger.info("test_file_metadata passed: file_id=%s filename=%s", file_id, body.get("filename"))

    @pytest.mark.oss
    async def test_simulate_fc_callback(self, admin_login: Tuple[AsyncClient, str, str]):
        """等待 FC 解压完成 → 模拟 FC 回调

        OSS 路径规则：
          zip       → data/{file_id}/{file_verify_code}.zip
          imzML/ibd → origin/{file_id}/{file_verify_code}/{stem}.imzML / .ibd
          （stem 为 zip 内 imzML/ibd 文件名 stem，与测试 zip 文件名一致）

        流程：
          1. 轮询 OSS 检查 origin 文件是否存在，等待 FC 解压完成
          2. 模拟 FC 回调，将 origin 文件路径写入 DB
          3. 最长等待 120 秒，超时直接报错
        """
        if TestOss._uploaded_file_id is None:
            pytest.skip("no uploaded file to simulate callback against")

        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}
        file_id = TestOss._uploaded_file_id

        stem = Path(TEST_FILENAME).stem  # "test_oss_100MB_2"
        origin_root = OSS_ORIGIN_STORAGE_ROOT.rstrip("/")
        imzml_path = f"{origin_root}/{file_id}/{TestOss._file_verify_code}/{stem}.imzML"
        ibd_path = f"{origin_root}/{file_id}/{TestOss._file_verify_code}/{stem}.ibd"

        # 1. 轮询 OSS 等待 FC 解压完成（origin 文件就绪），再发回调
        FC_POLL_INTERVAL = 5   # 轮询间隔（秒）
        FC_MAX_WAIT = 120      # 最长等待时间（秒）

        logger.info("polling OSS for FC unzip (max %ds, interval %ds)...",
                    FC_MAX_WAIT, FC_POLL_INTERVAL)
        start = time.monotonic()
        while time.monotonic() - start < FC_MAX_WAIT:
            imzml_ready = await OssStorage.check_file_exist(imzml_path)
            ibd_ready = await OssStorage.check_file_exist(ibd_path)
            if imzml_ready and ibd_ready:
                elapsed = time.monotonic() - start
                logger.info("FC unzip complete after %.1fs", elapsed)
                break
            logger.info("FC unzip pending (%.1fs elapsed, imzml=%s, ibd=%s)...",
                        time.monotonic() - start, imzml_ready, ibd_ready)
            await asyncio.sleep(FC_POLL_INTERVAL)
        else:
            elapsed = time.monotonic() - start
            raise AssertionError(
                f"FC unzip did not complete within {FC_MAX_WAIT}s "
                f"(elapsed={elapsed:.1f}s). "
                f"Expected origin files at {imzml_path} / {ibd_path}"
            )

        # 2. 模拟 FC 回调：将 origin 文件路径写入 DB（此时 origin 文件已就绪）
        callback_body = {
            "file_id": file_id,
            "file_path": TestOss._oss_path,
            "is_success": True,
            "act_hash": TestOss._file_verify_code,
            "imzml_path": imzml_path,
            "ibd_path": ibd_path,
            "imzml_size": TEST_IMZML_SIZE,
            "ibd_size": TEST_IBD_SIZE,
        }

        rs = await client.post(
            "/callback/oss_upload_callback",
            json=callback_body,
            headers={
                **auth,
                "X-FC-Token": FC_CALLBACK_TOKEN,
            },
        )
        assert rs.status_code == 200, (
            f"FC callback failed: status={rs.status_code} body={rs.text}"
        )
        body = rs.json()
        assert body["file_id"] == file_id
        assert body["status"] == "completed"
        logger.info("callback registered, file_id=%s status=completed", file_id)

        logger.info("test_simulate_fc_callback passed: file_id=%s", file_id)

    @pytest.mark.oss
    async def test_raw_download_metadata(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试 GET /files/{file_id}/download_raw（回调后 DB 已更新，下载验证 MD5）"""
        if TestOss._uploaded_file_id is None:
            pytest.skip("no uploaded file to test raw download against")

        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}
        file_id = TestOss._uploaded_file_id

        # 最多重试 3 次（容错网络抖动），回调成功后 imzML/ibd 已在 OSS
        rs = None
        for _ in range(3):
            rs = await client.get(f"/files/{file_id}/download_raw", headers=auth)
            if rs.status_code == 200:
                break
            await asyncio.sleep(0.5)

        assert rs.status_code == 200, (  # type: ignore
            f"raw download failed after 3 retries: status={rs.status_code} body={rs.text}"  # type: ignore
        )

        body = rs.json()  # type: ignore
        assert "files" in body
        assert len(body["files"]) == 2, f"expected 2 files, got {len(body['files'])}"

        # 按 filename 排序保证顺序（字母序：.ibd < .imzML）
        files = sorted(body["files"], key=lambda f: f["filename"])
        stem = Path(TEST_FILENAME).stem
        expected_ibd   = f"{stem}.ibd"
        expected_imzml = f"{stem}.imzML"

        assert files[0]["filename"] == expected_ibd
        assert files[1]["filename"] == expected_imzml

        # 下载并验证 MD5（hash 按 ibd → imzML 顺序匹配 sorted）
        import hashlib
        for f, expected_hash in zip(files, [TestOss._ibd_hash, TestOss._imzml_hash]):
            url = f["url"]
            assert url.startswith("https://"), (
                f"expected presigned OSS URL (https://), got '{url[:60]}...' for {f['filename']}"
            )

            h = hashlib.md5()
            async with client.stream("GET", url) as stream:
                async for chunk in stream.aiter_bytes(1024 * 1024):
                    h.update(chunk)
            got_hash = h.hexdigest()
            assert got_hash == expected_hash, (
                f"MD5 mismatch for {f['filename']}: expected={expected_hash} got={got_hash}"
            )

            logger.info("downloaded %s size=%s hash=%s", f["filename"],
                        f.get("size"), got_hash)

        logger.info("test_raw_download_metadata passed: got %d files", len(body["files"]))

    @pytest.mark.oss
    @pytest.mark.order("last")
    async def test_cleanup_oss_upload(self, admin_login: Tuple[AsyncClient, str, str]):
        """清理直传测试产生的文件与本地临时 zip"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        if TestOss._uploaded_file_id is None:
            pytest.skip("nothing to clean up")

        # 等 celery 后台任务（trigger_fc_feature_task 等）自然结束
        logger.info("waiting 20s for celery tasks to settle before cleanup...")
        await asyncio.sleep(20)

        rs = await client.delete(f"/files/{TestOss._uploaded_file_id}", headers=auth)
        assert rs.status_code == 200, rs.text
        assert rs.json()["status"] in ("deleted", "unlinked")
        logger.info("test_cleanup_oss_upload passed: file_id=%s", TestOss._uploaded_file_id)

        (Path("../test_temp") / TEST_FILENAME).unlink(missing_ok=True)
        TestOss._uploaded_file_id = None
        TestOss._oss_path = None
        TestOss._file_verify_code = None
        TestOss._imzml_hash = None
        TestOss._ibd_hash = None
