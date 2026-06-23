"""
OSS 测试集合（与本地上传测试区分开）

TestOss.test_upload_to_oss          [oss]
    preflight -> 拿 STS -> SDK 直传 OSS。不依赖 OSS 上传完成回调，
    托管 runner 只要有出站网络 + OSS secrets 即可跑。

共用构件：
  - tests/helpers/uploads.py  build_preflight_payload / make_test_zip_to_local / TEST_FILENAME
  - tests/helpers/oss.py      upload_file_to_oss（STS 直传）

运行方式:
    uv run pytest -m oss            # 仅直传（CI 可跑，需 OSS secrets）
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Tuple

import pytest
from httpx import AsyncClient

from tools.cal_hash import compute_imzml_md5
from .conftest import logger
from .helpers.oss import upload_file_to_oss
from .helpers.uploads import TEST_FILENAME, build_preflight_payload, make_test_zip_to_local


class TestOss:
    """OSS 直传功能测试（无回调，CI 可跑）"""
    _uploaded_file_id: int | None = None

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
        logger.info((f"test_upload_to_oss passed: upload_time_seconds={upload_time_seconds} username={username} file_id={file_id} size={size} oss_path={oss_path}\n"
                     f"etag={result.etag} fc-request-id={fc_request_id} real-md5={file_verify_code}"))

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
