"""
文件模块功能测试

覆盖接口：
  POST /files/preflight           预检
  POST /files/upload_simple       简单上传
  POST /files/chunked/upload      分片上传
  GET  /files/{id}/download       下载文件流

测试文件说明：
  - 测试 zip 由内存动态构造，不依赖磁盘文件
  - 调整 TEST_FILL_SIZE 可控制 zip 大小（字节）
  - 使用 ZIP_STORED 不压缩，大小精确可控
  - 调整 TEST_CHUNK_COUNT 可控制分片数量

运行方式:
    uv run pytest -m file
"""
import os
import io
import json
import time
import asyncio
from pathlib import Path
import math
import hashlib
import zipfile
from typing import Tuple, List
import pytest
from httpx import AsyncClient
from sqlmodel.ext.asyncio.session import AsyncSession

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from api.file_api_helper import get_file_record_by_id
from background.file_background import cleanup_old_download_zips
from constants.experiment_constants import SOURCE_FILE_PATH_CLEARED
from database.db import get_async_engine
from tools.cal_hash import compute_file_md5, compute_imzml_md5
from .conftest import logger

# ------------------------------------------------------------------ #
# 测试文件参数，按需修改
# ------------------------------------------------------------------ #
TEST_FILENAME = "test_oss_100MB_2"
TEST_FILL_SIZE = 512 * 1024  # 512 KB，调整此值控制 zip 大小
TEST_CHUNK_COUNT = 4          # 分片数量，调整此值控制分片数
TEST_IMZML_SIZE = 100 * 1024 * 1024  # 512 MB，调整此值控制 imzML 大小
TEST_IBD_SIZE =  100 * 1024 * 1024  # 512 MB，调整此值控制 ibd 大小


def _make_test_zip_to_local(imzml_size: int = TEST_IMZML_SIZE, ibd_size: int = TEST_IBD_SIZE) -> Path:
    """创建合法包含imzML和ibd文件的zip包到本地临时目录，并返回zip文件的路径"""
    temp_dir = Path("../test_temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = temp_dir / TEST_FILENAME

    # 分块创建imzML文件
    _CHUNK_BUFFER_SIZE = 1024 # 1 KB # pylint:disable = invalid-name
    with open(temp_dir / "test_imzML.imzML", "wb") as zf:
        for _ in range(0, imzml_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 分块创建ibd文件
    with open(temp_dir / "test_ibd.ibd", "wb") as zf:
        for _ in range(0, ibd_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 压缩zip包
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED,) as zf:
        zf.write(temp_dir / "test_imzML.imzML", "test_imzML.imzML")
        zf.write(temp_dir / "test_ibd.ibd", "test_ibd.ibd")



    # 删除源文件
    (temp_dir / "test_imzML.imzML").unlink()
    (temp_dir / "test_ibd.ibd").unlink()

    return zip_path


def _split_into_chunks(data: bytes, chunk_count: int) -> List[bytes]:
    """
    将 data 均匀切分为 chunk_count 片，最后一片包含余量。
    """
    size = len(data)
    chunk_size = math.ceil(size / chunk_count)
    return [data[i:i + chunk_size] for i in range(0, size, chunk_size)]

def _build_preflight_payload(
    *,
    size: int,
    file_verify_code: str,
    total_parts: int,
    storage_type: str = "local",
    filename: str = TEST_FILENAME,
) -> dict:
    """统一构造 preflight 请求体，减少重复代码"""
    return {
        "storage_type": storage_type,
        "filename": filename,
        "file_type": "zip",
        "size": size,
        "file_verify_code": file_verify_code,
        "experiment_type": "imzML",
        "organism": "org3",
        "organism_part": "part4",
        "condition": "",
        "sample_growth_conditions": "",
        "sample_stabilization": "",
        "tissue_modification": "",
        "maldi_matrix": "",
        "maldi_matrix_application": "",
        "solvent": "",
        "is_public": True,
        "polarity": "",
        "ionisation_source": "",
        "analyzer": "",
        "pixel_size_horizontal": 0,
        "pixel_size_vertical": 0,
        "resolving_power": 0,
        "mz": 0,
        "total_parts": total_parts,
    }

class TestFile:
    """文件接口功能测试"""
    _simple_uploaded_file_id: int | None = None
    _simple_uploaded_content: bytes | None = None

    _chunked_uploaded_file_id: int | None = None
    _chunked_uploaded_content: bytes | None = None

    _uploaded_file_id: int | None = None #oss的id

    @staticmethod
    async def _fetch_file_record(file_id: int):
        """读取数据库中的文件记录，便于断言 path/size 变化"""
        async with AsyncSession(get_async_engine()) as session:
            return await get_file_record_by_id(file_id, session)

    # ------------------------------------------------------------------ #
    # 文件主链路 1：预检 -> 简单上传 -> 下载 -> cleanup 后重建下载
    # ------------------------------------------------------------------ #
    @pytest.mark.file
    async def test_preflight_simple_upload_download(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证本地 zip 文件的预检、普通上传、下载及 cleanup 后重建下载主链路"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path = _make_test_zip_to_local()
        content = zip_path.read_bytes()
        file_verify_code, size = await compute_file_md5(zip_path)  # type: ignore

        # 1. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=_build_preflight_payload(
                size=size,
                file_verify_code=file_verify_code,
                total_parts=1,
            ),
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(
            "test_preflight_simple_upload_download: preflight ok, pre_file_id=%s",
            pre_file_id,
        )

        # 2. upload_simple
        with open(zip_path, "rb") as f:
            rs = await client.post(
                "/files/upload_simple",
                headers=auth,
                data={"storage_type": "local", "pre_file_id": str(pre_file_id)},
                files={"file": (TEST_FILENAME, f, "application/zip")},
            )
        assert rs.status_code == 200, rs.text
        file_id = rs.json()["file_id"]

        TestFile._simple_uploaded_file_id = file_id
        TestFile._simple_uploaded_content = content

        logger.info(
            "test_preflight_simple_upload_download: upload ok, file_id=%s size=%s",
            file_id,
            size,
        )

        # 3. download
        rs = await client.get(f"/files/{file_id}/download", headers=auth)
        assert rs.status_code == 200, rs.text

        disposition = rs.headers.get("content-disposition", "")
        assert TEST_FILENAME in disposition, f"filename not in Content-Disposition: {disposition}"
        assert rs.content == content, "downloaded content does not match uploaded content"

        logger.info(
            "test_preflight_simple_upload_download passed: file_id=%s download_size=%s",
            file_id,
            len(rs.content),
        )

        # 4. cleanup 后再次下载，验证重建 zip 的内容长度与真实文件大小一致
        cleaned = await cleanup_old_download_zips.run(retention_minutes=0, batch_size=1000) # type: ignore
        await asyncio.sleep(10) # 延迟10秒，等待
        assert cleaned >= 1

        record = await self._fetch_file_record(file_id)
        assert record.path == SOURCE_FILE_PATH_CLEARED

        rs = await client.get(f"/files/{file_id}/download", headers=auth)
        assert rs.status_code == 200, rs.text
        content_length = int(rs.headers.get("content-length", "0"))
        assert content_length == len(rs.content)

        record = await self._fetch_file_record(file_id)
        rebuilt_path = Path(record.path)
        assert rebuilt_path.exists()
        real_size = rebuilt_path.stat().st_size
        assert record.size == real_size == len(rs.content)

    # ------------------------------------------------------------------ #
    # 文件主链路 2：预检 -> 分片上传 -> 下载
    # ------------------------------------------------------------------ #
    @pytest.mark.file
    async def test_preflight_chunked_upload_download(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证本地 zip 文件的预检、分片上传、下载主链路"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        chunked_filename = "kawaru_chunked_test.zip"
        zip_path = _make_test_zip_to_local()
        content = zip_path.read_bytes()
        chunks = _split_into_chunks(content, TEST_CHUNK_COUNT)
        total_hash = hashlib.md5(content).hexdigest()
        part_hashes = [hashlib.md5(chunk).hexdigest() for chunk in chunks]
        total_parts = len(chunks)

        logger.info(
            "test_preflight_chunked_upload_download: size=%s total_parts=%s chunk_sizes=%s",
            len(content),
            total_parts,
            [len(chunk) for chunk in chunks],
        )

        # 1. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=_build_preflight_payload(
                size=len(content),
                file_verify_code=total_hash,
                total_parts=total_parts,
                filename=chunked_filename,
            ),
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(
            "test_preflight_chunked_upload_download: preflight ok, pre_file_id=%s",
            pre_file_id,
        )

        # 2. 逐片上传
        for idx, chunk in enumerate(chunks, start=1):
            rs = await client.post(
                "/files/chunked/upload",
                headers=auth,
                data={
                    "part_data_json": json.dumps({
                        "file_hash": total_hash,
                        "part_number": idx,
                        "part_hash": part_hashes[idx - 1],
                        "part_size": len(chunk),
                    })
                },
                files={
                    "part": ("chunk.bin", io.BytesIO(chunk), "application/octet-stream")
                },
            )
            assert rs.status_code == 200, f"part {idx} failed: {rs.text}"
            logger.info(
                "test_preflight_chunked_upload_download: part %s/%s uploaded",
                idx,
                total_parts,
            )

        # 3. 查询状态确认合并完成
        await asyncio.sleep(5) # 延迟5秒，等待
        rs = await client.get(f"/files/chunked/{pre_file_id}/status", headers=auth)
        assert rs.status_code == 200, rs.text
        status = rs.json()
        assert status["is_complete"] is True, f"expected is_complete=True, got: {status}"

        TestFile._chunked_uploaded_file_id = pre_file_id
        TestFile._chunked_uploaded_content = content

        # 4. download
        rs = await client.get(f"/files/{pre_file_id}/download", headers=auth)
        assert rs.status_code == 200, rs.text

        disposition = rs.headers.get("content-disposition", "")
        assert chunked_filename in disposition, f"filename not in Content-Disposition: {disposition}"
        assert rs.content == content, "downloaded content does not match chunk-merged content"

        logger.info(
            "test_preflight_chunked_upload_download passed: file_id=%s download_size=%s",
            pre_file_id,
            len(rs.content),
        )

    # ------------------------------------------------------------------ #
    # OSS上传测试
    # ------------------------------------------------------------------ #
    @pytest.mark.oss
    async def test_upload_to_oss(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试上传到OSS的完整流程，验证文件成功上传并可访问"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path = _make_test_zip_to_local()
        file_verify_code, size = await compute_imzml_md5(zip_path) # type: ignore
        logger.info(f"test_upload_to_oss: file_verify_code={file_verify_code}, size={size}")

        # 1. preflight
        start_time_seconds = time.perf_counter()
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=_build_preflight_payload(
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

        # 3. upload file to oss
        # 3.1 get oss config
        credentials_provider = oss.credentials.StaticCredentialsProvider(
            access_key_id=oss_sts_token["AccessKeyId"],
            access_key_secret=oss_sts_token["AccessKeySecret"],
            security_token=oss_sts_token["SecurityToken"],
        )

        cfg = oss.config.load_default()
        cfg.credentials_provider = credentials_provider
        cfg.region = os.getenv("OSS_REGION_ID") or ""

        # 3.2 create oss client and upload file
        oss_client = oss.Client(cfg)

        # 3.3 获取当前登录用户名
        rs = await client.get(
            "/user",
            headers=auth,
        )
        assert rs.status_code == 200, rs.text
        username = rs.json()["username"]

        def _put_obect_from_path() -> oss.UploadResult:
            uploader = oss_client.uploader(enable_checkpoint=True, checkpoint_dir=str(zip_path.parent / "checkpoint"))
            result = uploader.upload_file(PutObjectRequest(
                bucket=oss_bucket,
                key=oss_path,
            ), filepath=str(zip_path))
            return result

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _put_obect_from_path)
        assert result.status_code == 200, result.status
        fc_request_id = result.headers.get("x-oss-request-id")
        end_time_seconds = time.perf_counter()
        upload_time_seconds = round(end_time_seconds - start_time_seconds, 2)

        TestFile._uploaded_file_id = file_id
        logger.info((f"test_upload_to_oss passed: upload_time_seconds={upload_time_seconds} username={username} file_id={file_id} size={size} oss_path={oss_path}\n"
                     f"etag={result.etag} fc-request-id={fc_request_id} real-md5={file_verify_code}"))

    @pytest.mark.file
    async def test_upload_to_local(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试上传到本地存储的完整流程，验证文件成功上传并可访问"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path = _make_test_zip_to_local()
        file_verify_code, size = await compute_file_md5(zip_path) # type: ignore
        logger.info(f"test_upload_to_oss: file_verify_code={file_verify_code}, size={size}")

        # 1. preflight
        start_time_seconds = time.perf_counter()
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=_build_preflight_payload(
                size=size,
                file_verify_code=file_verify_code,
                total_parts=1,
                storage_type="local",
            ),
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(f"test_upload_to_oss: preflight ok, pre_file_id={pre_file_id}")

        # 2. upload
        with open(zip_path, "rb") as f:
            rs = await client.post(
                "/files/upload_simple",
                headers=auth,
                data={"pre_file_id": str(pre_file_id)},
                files={"file": (TEST_FILENAME, f, "application/zip")},
            )
        assert rs.status_code == 200, rs.text
        end_time_seconds = time.perf_counter()
        upload_time_seconds = round(end_time_seconds - start_time_seconds, 2)

        file_id = rs.json()["file_id"]

        # 3. 获取当前登录用户名
        rs = await client.get(
            "/user",
            headers=auth,
        )
        assert rs.status_code == 200, rs.text
        username = rs.json()["username"]

        rs = await client.post(
            "/files/oss_upload_complete", # 在测试时把会注视点对于x-oss-pub-key-url的验证
            headers={**auth, "x-oss-pub-key-url": "https://fake.example.com/fake-key"},
            data={
                "file_id": str(file_id),
                "current_username": username,
                "fc_request_id": fc_request_id,
            },
        )
        assert rs.status_code == 200, rs.text

        await asyncio.sleep(20) # 延迟20秒，等待实验处理完成

        TestFile._uploaded_file_id = file_id
        logger.info((f"test_upload_to_local passed: upload_time_seconds={upload_time_seconds} username={username} file_id={file_id} size={size}\n"
                     f"real-md5={file_verify_code}"))


    # ------------------------------------------------------------------ #
    # 清理
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    # @pytest.mark.oss
    @pytest.mark.order("last")
    async def test_cleanup_uploaded_file(self, admin_login: Tuple[AsyncClient, str, str]):
        """清理测试产生的文件，确保不污染数据库"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        cleanup_ids = [
            file_id for file_id in (
                TestFile._simple_uploaded_file_id,
                TestFile._chunked_uploaded_file_id,
                TestFile._uploaded_file_id,
            )
            if file_id is not None
        ]

        if not cleanup_ids:
            pytest.skip("nothing to clean up")

        for file_id in cleanup_ids:
            rs = await client.delete(f"/files/{file_id}", headers=auth)
            assert rs.status_code == 200, rs.text
            assert rs.json()["status"] in ("deleted", "unlinked")
            logger.info("test_cleanup_uploaded_file passed: file_id=%s", file_id)

        temp_dir = Path("../test_temp")
        zip_path = temp_dir / TEST_FILENAME

        zip_path.unlink(missing_ok=True)

        TestFile._simple_uploaded_file_id = None
        TestFile._simple_uploaded_content = None
        TestFile._chunked_uploaded_file_id = None
        TestFile._chunked_uploaded_content = None
        TestFile._uploaded_file_id = None
