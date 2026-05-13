"""
单用户大文件本地 vs OSS 上传速度对比测试

文件大小: 500MB / 1GB / 1.5GB / 2GB
存储类型:
  - local : preflight + /files/upload_simple
  - oss   : preflight + /files/upload + OSS SDK 直传 + /files/oss_upload_complete

实现严格照搬 tests/test_file.py 中 test_preflight_simple_upload_download 与
test_upload_to_oss 的请求方式（httpx 默认 files=、OSS SDK upload_file），
仅在外层加上"按尺寸循环 + 计时 + JSON 落盘"。

复用:
  - tests/conftest.py admin_login fixture (单用户 xiong)
  - tests/test_file.py _build_preflight_payload / _make_test_zip_to_local
  - tools/cal_hash.compute_file_md5
  - tests/perf_helper.dump_json / results_path

每次循环重新生成 imzML+ibd zip (md5 不同 → 防秒传)，上传完立即 unlink。
最大瞬时磁盘占用 ≈ 一份 zip ≤ 2GB。

运行方式:
    uv run pytest -m perf_upload
"""
from __future__ import annotations

import os
import asyncio
import time
from pathlib import Path
from typing import Tuple, List

import pytest
from httpx import AsyncClient

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from tools.cal_hash import compute_file_md5
from .conftest import logger, BASE_URL
from .test_file import _build_preflight_payload, _make_test_zip_to_local
from .perf_helper import dump_json, results_path

# ------------------------------------------------------------------ #
# 测试参数（按论文要求固定）
# ------------------------------------------------------------------ #
BYTES_IN_MB = 1024 ** 2

# (imzml_size, ibd_size, label) — STORED 不压缩，最终 zip 大小 ≈ imzml + ibd
SIZES: list[tuple[int, int, str]] = [
    (10 * BYTES_IN_MB,  10 * BYTES_IN_MB,  "20MB"),
    (20 * BYTES_IN_MB,  20 * BYTES_IN_MB,  "40MB"),
    (30 * BYTES_IN_MB,  30 * BYTES_IN_MB,  "60MB"),
    (40 * BYTES_IN_MB, 40 * BYTES_IN_MB, "80MB"),
]
STORAGES: list[str] = ["local", "oss"]
TEST_FILENAME_PREFIX = "kawaru_perfupload"


async def _upload_local(
    client: AsyncClient,
    auth: dict[str, str],
    zip_path: Path,
    file_verify_code: str,
    size: int,
    filename: str,
) -> Tuple[float, int]:
    """本地上传：preflight + upload_simple，与 test_file.py 同款。返回 (耗时秒, file_id)"""
    t0 = time.perf_counter()

    rs = await client.post(
        "/files/preflight",
        headers=auth,
        json=_build_preflight_payload(
            size=size,
            file_verify_code=file_verify_code,
            total_parts=1,
            storage_type="local",
            filename=filename,
        ),
    )
    assert rs.status_code == 200, f"preflight failed: {rs.text}"
    pre_file_id = rs.json()["file_id"]

    with open(zip_path, "rb") as f:
        rs = await client.post(
            "/files/upload_simple",
            headers=auth,
            data={"storage_type": "local", "pre_file_id": str(pre_file_id)},
            files={"file": (filename, f, "application/zip")},
        )
    assert rs.status_code == 200, f"upload_simple failed: {rs.text[:500]}"
    file_id = rs.json()["file_id"]

    return time.perf_counter() - t0, file_id


async def _upload_oss(
    client: AsyncClient,
    auth: dict[str, str],
    username: str,
    zip_path: Path,
    file_verify_code: str,
    size: int,
    filename: str,
    checkpoint_dir: Path,
) -> Tuple[float, int]:
    """OSS 上传：与 test_file.py test_upload_to_oss 完全一致。返回 (耗时秒, file_id)"""
    t0 = time.perf_counter()

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
    assert rs.status_code == 200, f"preflight failed: {rs.text}"
    pre_file_id = rs.json()["file_id"]

    rs = await client.post(
        "/files/upload",
        headers=auth,
        data={"pre_file_id": str(pre_file_id), "filename": filename},
    )
    assert rs.status_code == 200, f"/files/upload failed: {rs.text}"
    body = rs.json()
    file_id = body["file_id"]
    oss_sts_token = body["oss_sts_token"]
    oss_path = body["oss_path"]
    oss_bucket = body["oss_bucket"]

    credentials_provider = oss.credentials.StaticCredentialsProvider(
        access_key_id=oss_sts_token["AccessKeyId"],
        access_key_secret=oss_sts_token["AccessKeySecret"],
        security_token=oss_sts_token["SecurityToken"],
    )
    cfg = oss.config.load_default()
    cfg.credentials_provider = credentials_provider
    cfg.region = os.getenv("OSS_REGION_ID") or ""

    oss_client = oss.Client(cfg)

    def _put_object_from_path() -> oss.UploadResult:
        uploader = oss_client.uploader(enable_checkpoint=True, checkpoint_dir=str(checkpoint_dir))
        return uploader.upload_file(
            PutObjectRequest(bucket=oss_bucket, key=oss_path),
            filepath=str(zip_path),
        )

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _put_object_from_path)
    assert result.status_code == 200, f"oss put failed: status={result.status}"
    fc_request_id = result.headers.get("x-oss-request-id")

    rs = await client.post(
        "/files/oss_upload_complete",
        headers={**auth, "x-oss-pub-key-url": "https://fake.example.com/fake-key"},
        data={
            "file_id": str(file_id),
            "current_username": username,
            "fc_request_id": fc_request_id,
        },
    )
    assert rs.status_code == 200, f"oss_upload_complete failed: {rs.text}"

    return time.perf_counter() - t0, file_id


def _cleanup_dir(path: Path) -> None:
    """递归清掉一个临时目录（不抛错）"""
    if not path.exists():
        return
    for sub in path.glob("*"):
        try:
            if sub.is_file():
                sub.unlink(missing_ok=True)
            elif sub.is_dir():
                _cleanup_dir(sub)
        except OSError:
            pass
    try:
        path.rmdir()
    except OSError:
        pass


class TestPerfUploadSize:
    """单用户多大小文件 本地 vs OSS 上传速度对比"""
    _uploaded_file_ids: List[int] = []

    @pytest.mark.perf_upload
    async def test_upload_size_compare(self, admin_login: Tuple[AsyncClient, str, str]):
        """对每个 (size × storage) 组合各上传一次，记录耗时与吞吐量"""
        _, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        # admin_login 自带 60s 超时不够 GB 级上传，沿用其 token 但开 timeout=None 的 client
        async with AsyncClient(base_url=BASE_URL, timeout=None) as client:  # type: ignore
            # 拿当前用户名（OSS complete 需要）
            rs = await client.get("/user", headers=auth)
            assert rs.status_code == 200, rs.text
            username = rs.json()["username"]

            rows: list[dict] = []

            for imzml_size, ibd_size, label in SIZES:
                for storage in STORAGES:
                    filename = f"{TEST_FILENAME_PREFIX}_{label}_{storage}"

                    logger.info(
                        "[perf upload] generating zip filename=%s storage=%s (imzml=%dMB ibd=%dMB)",
                        filename, storage, imzml_size // BYTES_IN_MB, ibd_size // BYTES_IN_MB,
                    )
                    gen_t0 = time.perf_counter()
                    # 复用 test_file.py 的 zip 生成（默认写到固定路径 ../test_temp/kawaru_functest）
                    # 立即 rename 到唯一物理路径，避免 8 次循环共享同一物理文件名
                    default_zip_path = _make_test_zip_to_local(imzml_size=imzml_size, ibd_size=ibd_size)
                    zip_path = default_zip_path.parent / f"{filename}.zip"
                    zip_path.unlink(missing_ok=True)
                    default_zip_path.rename(zip_path)
                    gen_elapsed = time.perf_counter() - gen_t0

                    file_verify_code, actual_size = await compute_file_md5(zip_path)  # type: ignore
                    logger.info(
                        "[perf upload] zip ready label=%s actual_size=%d gen=%.1fs md5=%s",
                        label, actual_size, gen_elapsed, file_verify_code[:8],
                    )

                    duration: float | None = None
                    file_id: int | None = None
                    error: str | None = None
                    checkpoint_dir = zip_path.parent / f"checkpoint_{label}_{storage}"

                    try:
                        if storage == "local":
                            duration, file_id = await _upload_local(
                                client, auth, zip_path, file_verify_code, actual_size, filename,
                            )
                        else:
                            duration, file_id = await _upload_oss(
                                client, auth, username, zip_path,
                                file_verify_code, actual_size, filename, checkpoint_dir,
                            )
                        TestPerfUploadSize._uploaded_file_ids.append(file_id)
                        logger.info(
                            "[perf upload] DONE label=%s storage=%s duration=%.2fs throughput=%.2fMB/s file_id=%s",
                            label, storage, duration,
                            (actual_size / BYTES_IN_MB) / duration if duration else 0.0,
                            file_id,
                        )
                    except Exception as e:  # pylint: disable=broad-except
                        error = repr(e)
                        logger.error(
                            "[perf upload] FAILED label=%s storage=%s err=%r",
                            label, storage, e,
                        )

                    throughput_mbps = (
                        (actual_size / BYTES_IN_MB) / duration if duration and duration > 0 else None
                    )
                    rows.append({
                        "size_label": label,
                        "size_bytes": actual_size,
                        "imzml_size": imzml_size,
                        "ibd_size": ibd_size,
                        "storage": storage,
                        "duration_s": duration,
                        "throughput_mbps": throughput_mbps,
                        "gen_elapsed_s": gen_elapsed,
                        "file_id": file_id,
                        "error": error,
                    })

                    # 立即清理本次的 zip + checkpoint
                    zip_path.unlink(missing_ok=True)
                    _cleanup_dir(checkpoint_dir)

            json_path = results_path("upload_size")
            payload = {
                "sizes": [
                    {"label": l, "imzml_size": iz, "ibd_size": ib}
                    for iz, ib, l in SIZES
                ],
                "storages": STORAGES,
                "rows": rows,
            }
            dump_json(payload, json_path)
            logger.info("[perf upload] result saved to %s", json_path)

            failures = [r for r in rows if r["error"] is not None]
            assert not failures, f"{len(failures)} upload(s) failed; see {json_path}"

    @pytest.mark.perf_upload
    @pytest.mark.order("last")
    async def test_cleanup_perf_uploads(self, admin_login: Tuple[AsyncClient, str, str]):
        """删除测试上传的所有文件"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        cleanup_ids = list(TestPerfUploadSize._uploaded_file_ids)
        if not cleanup_ids:
            pytest.skip("no perf uploads to clean")

        deleted, failed = 0, 0
        for file_id in cleanup_ids:
            try:
                rs = await client.delete(f"/files/{file_id}", headers=auth)
                if rs.status_code == 200 and rs.json().get("status") in ("deleted", "unlinked"):
                    deleted += 1
                else:
                    failed += 1
                    logger.error(
                        "cleanup failed file_id=%s status=%s body=%s",
                        file_id, rs.status_code, rs.text,
                    )
            except Exception as e:  # pylint: disable=broad-except
                failed += 1
                logger.error("cleanup exception file_id=%s err=%r", file_id, e)

        logger.info("[perf upload cleanup] deleted=%s failed=%s", deleted, failed)

        TestPerfUploadSize._uploaded_file_ids = []
        assert failed == 0, f"{failed} cleanup failures"
