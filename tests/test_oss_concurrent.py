"""
OSS 高并发上传压力测试

基于 test_file.py 中的 test_upload_to_oss 流程，复用其辅助函数（preflight payload 构造、md5 校验、conftest 中的登录 fixture）；
将单个上传流程封装为 _perform_oss_upload，再用 asyncio.gather + Semaphore 限流并发执行。

运行方式:
    uv run pytest -m oss_concurrent
"""
import os
import asyncio
import zipfile
from pathlib import Path
from typing import Tuple, List
import pytest
from httpx import AsyncClient

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from tools.cal_hash import compute_file_md5
from .conftest import logger
from .test_file import _build_preflight_payload, TEST_FILENAME

# ------------------------------------------------------------------ #
# 并发测试参数，按需修改
# ------------------------------------------------------------------ #
CONCURRENCY_TOTAL = 10      # 总文件数
CONCURRENCY_LIMIT = 3       # 同时进行的任务上限（信号量）
TEST_IMZML_SIZE = 1024 * 1024  # 1 MB
TEST_IBD_SIZE = 1024 * 1024    # 1 MB
TEMP_DIR = Path("../test_temp_concurrent")


def _make_unique_test_zip(index: int) -> Path:
    """为并发任务生成唯一内容的 zip（不同 md5），避免 preflight 秒传去重"""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = TEMP_DIR / f"{TEST_FILENAME}_{index}.zip"
    imzml_path = TEMP_DIR / f"test_imzML_{index}.imzML"
    ibd_path = TEMP_DIR / f"test_ibd_{index}.ibd"

    _CHUNK = 1024 # pylint: disable=invalid-name
    with open(imzml_path, "wb") as zf:
        for _ in range(0, TEST_IMZML_SIZE, _CHUNK):
            zf.write(os.urandom(_CHUNK))
    with open(ibd_path, "wb") as zf:
        for _ in range(0, TEST_IBD_SIZE, _CHUNK):
            zf.write(os.urandom(_CHUNK))

    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        zf.write(imzml_path, "test_imzML.imzML")
        zf.write(ibd_path, "test_ibd.ibd")

    imzml_path.unlink()
    ibd_path.unlink()
    return zip_path


async def _perform_oss_upload(
    client: AsyncClient,
    auth: dict,
    username: str,
    zip_path: Path,
    file_verify_code: str,
    size: int,
    index: int,
) -> int:
    """完整跑一次 OSS 上传流程，返回上传后的 file_id"""
    filename = f"{TEST_FILENAME}_{index}"

    # 1. preflight
    rs = await client.post(
        "/files/preflight",
        headers=auth,
        json=_build_preflight_payload(
            size=size,
            file_verify_code=file_verify_code,
            total_parts=1,
            storage_type="oss",
            filename=filename,
        ),
    )
    assert rs.status_code == 200, f"[{index}] preflight failed: {rs.text}"
    pre_file_id = rs.json()["file_id"]

    # 2. /files/upload 拿 STS + oss_path
    rs = await client.post(
        "/files/upload",
        headers=auth,
        data={"pre_file_id": str(pre_file_id), "filename": filename},
    )
    assert rs.status_code == 200, f"[{index}] /files/upload failed: {rs.text}"
    body = rs.json()
    file_id = body["file_id"]
    oss_sts_token = body["oss_sts_token"]
    oss_path = body["oss_path"]
    oss_bucket = body["oss_bucket"]

    # 3. 直传 OSS
    credentials_provider = oss.credentials.StaticCredentialsProvider(
        access_key_id=oss_sts_token["AccessKeyId"],
        access_key_secret=oss_sts_token["AccessKeySecret"],
        security_token=oss_sts_token["SecurityToken"],
    )
    cfg = oss.config.load_default()
    cfg.credentials_provider = credentials_provider
    cfg.region = os.getenv("OSS_REGION_ID") or ""

    oss_client = oss.Client(cfg)
    checkpoint_dir = str(zip_path.parent / f"checkpoint_{index}")

    def _put_object_from_path() -> oss.UploadResult:
        uploader = oss_client.uploader(enable_checkpoint=True, checkpoint_dir=checkpoint_dir)
        return uploader.upload_file(
            PutObjectRequest(bucket=oss_bucket, key=oss_path),
            filepath=str(zip_path),
        )

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _put_object_from_path)
    assert result.status_code == 200, f"[{index}] oss put failed: status={result.status}"
    fc_request_id = result.headers.get("x-oss-request-id")

    # 4. oss_upload_complete
    rs = await client.post(
        "/files/oss_upload_complete",
        headers={**auth, "x-oss-pub-key-url": "https://fake.example.com/fake-key"},
        data={
            "file_id": str(file_id),
            "current_username": username,
            "fc_request_id": fc_request_id,
        },
    )
    assert rs.status_code == 200, f"[{index}] oss_upload_complete failed: {rs.text}"
    logger.info("[concurrent oss] index=%s file_id=%s done", index, file_id)
    return file_id


class TestOssConcurrent:
    """OSS 并发上传压力测试"""
    _uploaded_file_ids: List[int] = []

    @pytest.mark.oss_concurrent
    async def test_concurrent_oss_upload(self, admin_login: Tuple[AsyncClient, str, str]):
        """并发 N 个文件的完整 OSS 上传流程"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        # 拿一次当前用户名（所有任务共用）
        rs = await client.get("/user", headers=auth)
        assert rs.status_code == 200, rs.text
        username = rs.json()["username"]

        # 1. 预先串行生成 N 个不同内容的 zip（CPU/IO 压力小，避免并发争抢磁盘）
        logger.info("[concurrent oss] generating %s zips...", CONCURRENCY_TOTAL)
        zip_paths: List[Path] = []
        verify_codes: List[Tuple[str, int]] = []
        for i in range(CONCURRENCY_TOTAL):
            zp = _make_unique_test_zip(i)
            file_verify_code, size = await compute_file_md5(zp)  # type: ignore
            zip_paths.append(zp)
            verify_codes.append((file_verify_code, size))
        logger.info("[concurrent oss] %s zips ready", CONCURRENCY_TOTAL)

        # 2. 信号量限流并发执行
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def _bounded(idx: int) -> int:
            async with sem:
                fvc, sz = verify_codes[idx]
                return await _perform_oss_upload(
                    client, auth, username, zip_paths[idx], fvc, sz, idx,
                )

        tasks = [_bounded(i) for i in range(CONCURRENCY_TOTAL)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 3. 汇总结果
        successes: List[int] = []
        failures: List[Tuple[int, BaseException]] = []
        for i, r in enumerate(results):
            if isinstance(r, BaseException):
                failures.append((i, r))
            else:
                successes.append(r)
        TestOssConcurrent._uploaded_file_ids = successes

        logger.info(
            "[concurrent oss] total=%s success=%s fail=%s",
            CONCURRENCY_TOTAL, len(successes), len(failures),
        )
        for i, err in failures:
            logger.error("[concurrent oss] index=%s error: %r", i, err)

        assert not failures, f"{len(failures)} of {CONCURRENCY_TOTAL} uploads failed"

    # ------------------------------------------------------------------ #
    # 清理
    # ------------------------------------------------------------------ #
    @pytest.mark.oss_concurrent
    @pytest.mark.order("last")
    async def test_cleanup_concurrent_uploads(self, admin_login: Tuple[AsyncClient, str, str]):
        """删除并发测试产生的所有文件，并清掉本地临时 zip"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        cleanup_ids = list(TestOssConcurrent._uploaded_file_ids)
        if not cleanup_ids:
            pytest.skip("no concurrent uploads to clean up")

        deleted, failed = 0, 0
        for file_id in cleanup_ids:
            try:
                rs = await client.delete(f"/files/{file_id}", headers=auth)
                if rs.status_code == 200 and rs.json().get("status") in ("deleted", "unlinked"):
                    deleted += 1
                else:
                    failed += 1
                    logger.error("cleanup failed: file_id=%s status=%s body=%s", file_id, rs.status_code, rs.text)
            except Exception as e:  # pylint: disable=broad-except
                failed += 1
                logger.error("cleanup exception: file_id=%s err=%r", file_id, e)

        logger.info("[concurrent oss cleanup] deleted=%s failed=%s", deleted, failed)

        # 清理本地临时目录
        if TEMP_DIR.exists():
            for child in TEMP_DIR.glob("*"):
                if child.is_file():
                    child.unlink(missing_ok=True)
                elif child.is_dir():
                    for sub in child.glob("*"):
                        sub.unlink(missing_ok=True)
                    child.rmdir()
            TEMP_DIR.rmdir()

        TestOssConcurrent._uploaded_file_ids = []
        assert failed == 0, f"{failed} cleanup failures"
