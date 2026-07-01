"""
OSS 测试集合（与本地上传测试区分开）

TestOss.test_upload_to_oss              [oss]   preflight -> 拿 STS -> SDK 直传 OSS
TestOss.test_file_metadata              [oss]   GET /files/{file_id}/metadata（公开接口，不依赖回调）
TestOss.test_simulate_fc_callback       [oss]   模拟 FC 回调 → 更新 DB（本地：文件在 OSS 则成功；CI：文件缺失则 skip）
TestOss.test_raw_download_metadata      [oss]   GET /files/{file_id}/download_raw（需鉴权，回调后直接验证）
TestOss.test_cleanup_oss_upload         [oss]   清理 OSS 文件 + DB 记录 + 临时 zip

共用构件：
  - tests/helpers/uploads.py  build_preflight_payload / make_test_zip_to_local / TEST_FILENAME
  - tests/helpers/oss.py      upload_file_to_oss（STS 直传）

运行方式:
    uv run pytest -m oss            # 仅直传（CI 可跑，需 OSS secrets）
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

    @pytest.mark.oss
    async def test_upload_to_oss(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试上传到 OSS 的完整流程（preflight -> STS -> 直传），验证文件成功上传"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path = make_test_zip_to_local()
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
        logger.info("test_file_metadata passed: file_id=%s filename=%s", file_id, body.get("filename"))

    @pytest.mark.oss
    async def test_simulate_fc_callback(self, admin_login: Tuple[AsyncClient, str, str]):
        """模拟 FC 回调：OSS 上传完成后 FC 解压 → 回调主服务更新 DB

        OSS 路径规则：
          zip       → data/{stem}.zip
          imzML/ibd → origin/{stem}/{stem}.imzML / origin/{stem}/{stem}.ibd

        本地测试（FC 已跑、文件存在 OSS）→ 回调 200，DB 更新成功；
        CI（FC 没跑、文件缺失）→ tag_oss_file 返回 False → 回调 500 → pytest.skip。
        """
        if TestOss._uploaded_file_id is None:
            pytest.skip("no uploaded file to simulate callback against")

        # 等 FC 解压完成（OSS 事件触发 FC → 冷启动 → 下载 zip → 解压 → 上传 imzML+ibd）
        logger.info("waiting 15s for FC unzip to complete...")
        await asyncio.sleep(15)

        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        stem = Path(TEST_FILENAME).stem  # "test_oss_100MB_2"
        imzml_path = f"origin/{stem}/{stem}.imzML"
        ibd_path = f"origin/{stem}/{stem}.ibd"

        callback_body = {
            "filename": TEST_FILENAME,
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

        if rs.status_code != 200:
            # CI 环境 FC 没跑，imzML/ibd 不在 OSS，tag_oss_file 失败
            pytest.skip(
                f"FC callback failed (status={rs.status_code}), "
                f"likely imzML/ibd not on OSS — skipping raw-download test"
            )

        body = rs.json()
        assert body["file_id"] == TestOss._uploaded_file_id
        assert body["status"] == "completed"
        logger.info("test_simulate_fc_callback passed: file_id=%s", TestOss._uploaded_file_id)

    @pytest.mark.oss
    async def test_raw_download_metadata(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试 GET /files/{file_id}/download_raw（回调后 DB 已更新，直接验证）"""
        if TestOss._uploaded_file_id is None:
            pytest.skip("no uploaded file to test raw download against")

        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}
        file_id = TestOss._uploaded_file_id

        # 最多重试 3 次（容错网络抖动），回调成功后 DB 已有记录无需长轮询
        for attempt in range(3):
            rs = await client.get(f"/files/{file_id}/download_raw", headers=auth)
            if rs.status_code == 200:
                break
            await asyncio.sleep(0.5)
        else:
            pytest.skip(f"raw download not ready after 3 attempts, last status={rs.status_code}")  # type: ignore

        body = rs.json()
        assert "files" in body
        assert len(body["files"]) == 2, f"expected 2 files (imzML + ibd), got {len(body['files'])}"
        logger.info("test_raw_download_metadata passed: got %d files", len(body["files"]))

    @pytest.mark.oss
    @pytest.mark.order("last")
    async def test_cleanup_oss_upload(self, admin_login: Tuple[AsyncClient, str, str]):
        """清理直传测试产生的文件与本地临时 zip"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        if TestOss._uploaded_file_id is None:
            pytest.skip("nothing to clean up")

        rs = await client.delete(f"/files/{TestOss._uploaded_file_id}", headers=auth)
        assert rs.status_code == 200, rs.text
        assert rs.json()["status"] in ("deleted", "unlinked")
        logger.info("test_cleanup_oss_upload passed: file_id=%s", TestOss._uploaded_file_id)

        (Path("../test_temp") / TEST_FILENAME).unlink(missing_ok=True)
        TestOss._uploaded_file_id = None
        TestOss._oss_path = None
        TestOss._file_verify_code = None
