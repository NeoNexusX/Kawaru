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
import asyncio
from pathlib import Path
import math
import hashlib
import zipfile
from typing import Tuple, List
import pytest
from httpx import AsyncClient

import alibabacloud_oss_v2 as oss
from alibabacloud_oss_v2 import PutObjectRequest

from tools.cal_hash import compute_file_md5
from .conftest import logger

# ------------------------------------------------------------------ #
# 测试文件参数，按需修改
# ------------------------------------------------------------------ #
TEST_FILENAME = "kawaru_functest"
TEST_FILL_SIZE = 512 * 1024  # 512 KB，调整此值控制 zip 大小
TEST_CHUNK_COUNT = 4          # 分片数量，调整此值控制分片数
TEST_IMZML_SIZE = 10240 * 1024  # 10 MB，调整此值控制 imzML 大小
TEST_IBD_SIZE = 10240 * 1024  # 10 MB，调整此值控制 ibd 大小


def _make_test_zip(fill_size: int = TEST_FILL_SIZE) -> bytes:
    """
    构造指定大小的合法 zip 文件（内存中）。
    fill_size: 内部填充随机字节数，单位 bytes
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("data.bin", os.urandom(fill_size))
    return buf.getvalue()


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


class TestFile:
    """文件接口功能测试：预检 → 简单上传 → 下载"""

    # class 级别共享状态，供后续测试复用
    _uploaded_file_id: int | None = None
    _uploaded_content: bytes | None = None  # 保存上传内容，用于下载校验

    # ------------------------------------------------------------------ #
    # 预检接口测试
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    async def test_preflight_ok(self, admin_login: Tuple[AsyncClient, str, str]):
        """正常预检，验证返回合法的 file_id"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        content = _make_test_zip()
        file_verify_code = hashlib.md5(content).hexdigest()

        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "local",
                "filename": TEST_FILENAME,
                "file_type": "zip",
                "size": len(content),
                "file_verify_code": file_verify_code,
                "experiment_type": "imzML",
                "organism": "", 
                "organism_part": "",
                 "condition": "",
                "sample_growth_conditions": "", 
                "sample_stabilization": "",
                "tissue_modification": "", 
                "maldi_matrix": "",
                "maldi_matrix_application": "", 
                "solvent": "",
                "total_parts": 1
            },
        )
        assert rs.status_code == 200, rs.text
        data = rs.json()
        assert "file_id" in data
        assert data["filename"] == TEST_FILENAME
        logger.info(f"test_preflight_ok passed: file_id={data['file_id']}")

        # 清理预检记录（避免影响后续测试的重复文件名检查）
        file_id = data["file_id"]
        await client.delete(f"/files/{file_id}", headers=auth)

    @pytest.mark.file
    async def test_preflight_invalid_file_type_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """预检：非允许的文件类型（非 zip）应返回 400"""
        client, token, token_type = admin_login

        rs = await client.post(
            "/files/preflight",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={
                "storage_type": "local",
                "filename": "kawaru_test.exe",   # 不在 allowed_file_types 中
                "file_type": "exe",
                "size": 1024,
                "file_verify_code": "a" * 32,
                "experiment_type": "",
            },
        )
        assert rs.status_code == 400, rs.text
        logger.info("test_preflight_invalid_file_type_rejected passed")

    @pytest.mark.file
    async def test_preflight_invalid_storage_type_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """预检：非法 storage_type 应返回 400"""
        client, token, token_type = admin_login
        content = _make_test_zip()

        rs = await client.post(
            "/files/preflight",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={
                "storage_type": "s3_invalid",
                "filename": TEST_FILENAME,
                "file_type": "zip",
                "size": len(content),
                "file_verify_code": hashlib.md5(content).hexdigest(),
                "experiment_type": "",
            },
        )
        assert rs.status_code == 400, rs.text
        logger.info("test_preflight_invalid_storage_type_rejected passed")

    # ------------------------------------------------------------------ #
    # 简单上传接口测试
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    async def test_simple_upload_hash_mismatch_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """
        上传时实际文件内容与预检 hash 不一致，应返回 500（file_persist_failed）
        流程：用 content_a 的 hash 做预检，实际上传 content_b
        """
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        content_a = _make_test_zip()
        content_b = _make_test_zip()  # 不同的随机内容，hash 必然不同
        file_verify_code_a = hashlib.md5(content_a).hexdigest()
        mismatch_filename = "kawaru_mismatch_test.zip"

        # preflight 用 content_a 的 hash
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "local",
                "filename": mismatch_filename,
                "file_type": "zip",
                "size": len(content_a),
                "file_verify_code": file_verify_code_a,
                "experiment_type": "imzML",
                "organism": "",
                 "organism_part": "",
                 "condition": "",
                "sample_growth_conditions": "",
                "sample_stabilization": "",
                "tissue_modification": "",
                "maldi_matrix": "",
                "maldi_matrix_application": "",
                "solvent": "",
                "total_parts": 1
            },
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]

        # 实际上传 content_b（hash 不匹配）
        rs = await client.post(
            "/files/upload_simple",
            headers=auth,
            data={"storage_type": "local", "pre_file_id": str(pre_file_id)},
            files={"file": (mismatch_filename, io.BytesIO(content_b), "application/zip")},
        )
        assert rs.status_code == 500, rs.text
        logger.info("test_simple_upload_hash_mismatch_rejected passed")

    @pytest.mark.file
    async def test_simple_upload_ok(self, admin_login: Tuple[AsyncClient, str, str]):
        """
        完整简单上传流程：preflight → upload_simple
        上传成功后将 file_id 和内容存入 class 变量供后续测试复用
        """
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        content = _make_test_zip()
        file_verify_code = hashlib.md5(content).hexdigest()

        # 1. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "local",
                "filename": TEST_FILENAME,
                "file_type": "zip",
                "size": len(content),
                "file_verify_code": file_verify_code,
                "experiment_type": "imzML",
                "organism": "", 
                "organism_part": "",
                 "condition": "",
                "sample_growth_conditions": "", 
                "sample_stabilization": "",
                "tissue_modification": "", 
                "maldi_matrix": "",
                "maldi_matrix_application": "", 
                "solvent": "",
                "total_parts": 1
            },
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(f"test_simple_upload_ok: preflight ok, pre_file_id={pre_file_id}")

        # 2. upload_simple
        rs = await client.post(
            "/files/upload_simple",
            headers=auth,
            data={"storage_type": "local", "pre_file_id": str(pre_file_id)},
            files={"file": (TEST_FILENAME, io.BytesIO(content), "application/zip")},
        )
        assert rs.status_code == 200, rs.text
        file_id = rs.json()["file_id"]

        # 验证下载时间字段已初始化为上传时间
        rs_files = await client.get("/files", headers=auth)
        assert rs_files.status_code == 200, rs_files.text
        files_data = rs_files.json()["files"]
        file_entry = next((f for f in files_data if f["file_id"] == file_id), None)
        assert file_entry is not None
        assert file_entry.get("last_downloaded_at") == file_entry.get("uploaded_at")

        # 保存到 class 变量供后续测试使用
        TestFile._uploaded_file_id = file_id
        TestFile._uploaded_content = content

        logger.info(f"test_simple_upload_ok passed: file_id={file_id} size={len(content)}")



    # ------------------------------------------------------------------ #
    # 下载接口测试
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    # async def test_download_ok(self, admin_login: Tuple[AsyncClient, str, str]):
    #     """
    #     下载已上传的文件，验证：
    #     1. 响应状态码 200
    #     2. Content-Disposition header 包含文件名
    #     3. 响应字节内容与上传内容完全一致
    #     """
    #     client, token, token_type = admin_login

    #     if TestFile._uploaded_file_id is None:
    #         pytest.skip("no uploaded file, run test_simple_upload_ok first")

    #     file_id = TestFile._uploaded_file_id
    #     rs = await client.get(
    #         f"/files/{file_id}/download",
    #         headers={"Authorization": f"{token_type.title()} {token}"},
    #     )
    #     assert rs.status_code == 200, rs.text

    #     # 验证 Content-Disposition
    #     disposition = rs.headers.get("content-disposition", "")
    #     assert TEST_FILENAME in disposition, f"filename not in Content-Disposition: {disposition}"

    #     # 验证下载内容与上传内容字节一致
    #     assert rs.content == TestFile._uploaded_content, "downloaded content does not match uploaded content"

    #     logger.info(f"test_download_ok passed: file_id={file_id} size={len(rs.content)}")

    @pytest.mark.file
    async def test_download_nonexistent_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """下载不存在的文件应返回 404"""
        client, token, token_type = admin_login

        rs = await client.get(
            "/files/99999/download",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 404, rs.text
        logger.info("test_download_nonexistent_rejected passed")

    @pytest.mark.file
    async def test_download_no_token_rejected(self, async_client: AsyncClient):
        """无 token 下载应返回 401"""
        rs = await async_client.get("/files/1/download")
        assert rs.status_code == 401, rs.text
        logger.info("test_download_no_token_rejected passed")

    @pytest.mark.file
    async def test_download_updates_last_downloaded_at(self, admin_login: Tuple[AsyncClient, str, str]):
        """下载后 download_count 和 last_downloaded_at 应更新"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        if TestFile._uploaded_file_id is None:
            pytest.skip("no uploaded file, run test_simple_upload_ok first")

        file_id = TestFile._uploaded_file_id

        rs_before = await client.get("/files", headers=auth)
        assert rs_before.status_code == 200, rs_before.text
        before_entry = next((f for f in rs_before.json()["files"] if f["file_id"] == file_id), None)
        assert before_entry is not None

        before_count = before_entry["download_count"]

        rs = await client.get(f"/files/{file_id}/download", headers=auth)
        assert rs.status_code == 200, rs.text

        rs_after = await client.get("/files", headers=auth)
        assert rs_after.status_code == 200, rs_after.text
        after_entry = next((f for f in rs_after.json()["files"] if f["file_id"] == file_id), None)
        assert after_entry is not None

        assert after_entry["download_count"] == before_count + 1

        logger.info("test_download_updates_last_downloaded_at passed")


    # ------------------------------------------------------------------ #
    # 分片上传接口测试
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    async def test_chunked_upload_ok(self, admin_login: Tuple[AsyncClient, str, str]):
        """
        完整分片上传流程：preflight → 逐片 chunked/upload → 合并完成
        - 文件由内存 zip 构造，按 TEST_CHUNK_COUNT 均匀切分
        - 每片计算独立 file_verify_code，preflight 携带 total_parts
        - 上传完最后一片后服务端自动合并，file_id 存入 class 变量供清理
        """
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        content = _make_test_zip()
        chunks = _split_into_chunks(content, TEST_CHUNK_COUNT)
        total_hash = hashlib.md5(content).hexdigest()
        part_hashes = [hashlib.md5(c).hexdigest() for c in chunks]
        total_parts = len(chunks)
        chunked_filename = "kawaru_chunked_test.zip"

        logger.info(
            f"test_chunked_upload_ok: size={len(content)} "
            f"total_parts={total_parts} chunk_sizes={[len(c) for c in chunks]}"
        )

        # 1. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "local",
                "filename": chunked_filename,
                "file_type": "zip",
                "size": len(content),
                "file_verify_code": total_hash,
                "experiment_type": "imzML",
                "organism": "",
                "organism_part": "",
                "condition": "",
                "sample_growth_conditions": "",
                "sample_stabilization": "",
                "tissue_modification": "",
                "maldi_matrix": "",
                "maldi_matrix_application": "",
                "solvent": "",
                "total_parts": total_parts,
            },
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(f"test_chunked_upload_ok: preflight ok, pre_file_id={pre_file_id}")

        # 2. 逐片上传
        for idx, chunk in enumerate(chunks):
            part_number = idx + 1
            part_hash = part_hashes[idx]
            part_size = len(chunk)

            logger.info(f"  uploading part {part_number}/{total_parts} size={part_size} hash={part_hash[:8]}...")

            rs = await client.post(
                "/files/chunked/upload",
                headers=auth,
                data={
                    "part_data_json": json.dumps({
                        "file_hash": total_hash,
                        "part_number": part_number,
                        "part_hash": part_hash,
                        "part_size": part_size,
                    })
                },
                files={
                    "part": ("chunk.bin", io.BytesIO(chunk), "application/octet-stream")
                },
            )
            assert rs.status_code == 200, f"part {part_number} failed: {rs.text}"
            logger.info(f"  part {part_number}/{total_parts} uploaded ok")

        # 3. 合并后查询状态，确认 is_complete=True
        rs = await client.get(
            f"/files/chunked/{pre_file_id}/status",
            headers=auth,
        )
        assert rs.status_code == 200, rs.text
        status = rs.json()
        assert status["is_complete"] is True, f"expected is_complete=True, got: {status}"

        TestFile._uploaded_file_id = pre_file_id
        TestFile._uploaded_content = content
        logger.info(f"test_chunked_upload_ok passed: file_id={pre_file_id} total_parts={total_parts}")

    @pytest.mark.file
    async def test_chunked_upload_wrong_part_hash_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """
        分片上传：分片 hash 与实际内容不符，应返回 500
        """
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        content = _make_test_zip()
        total_hash = hashlib.md5(content).hexdigest()
        bad_filename = "kawaru_chunked_badhash.zip"

        # preflight（2片）
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "local",
                "filename": bad_filename,
                "file_type": "zip",
                "size": len(content),
                "file_verify_code": total_hash,
                "experiment_type": "imzML",
                "organism": "",
                "organism_part": "",
                "condition": "",
                "sample_growth_conditions": "",
                "sample_stabilization": "",
                "tissue_modification": "",
                "maldi_matrix": "",
                "maldi_matrix_application": "",
                "solvent": "",
                "total_parts": 2,
            },
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]

        chunk = content  # 整体作为第1片，但 hash 故意填错
        rs = await client.post(
            "/files/chunked/upload",
            headers=auth,
            data={
                "part_data_json": json.dumps({
                    "file_hash": total_hash,
                    "part_number": 1,
                    "part_hash": "a" * 64,   # 故意错误的 hash
                    "part_size": len(chunk),
                })
            },
            files={
                "part": ("chunk.bin", io.BytesIO(chunk), "application/octet-stream")
            },
        )
        assert rs.status_code == 500, rs.text
        logger.info("test_chunked_upload_wrong_part_hash_rejected passed")

        # 清理预检记录
        await client.delete(f"/files/{pre_file_id}", headers=auth)


    # ------------------------------------------------------------------ #
    # OSS上传测试
    # ------------------------------------------------------------------ #
    @pytest.mark.oss
    async def test_upload_to_oss(self, admin_login: Tuple[AsyncClient, str, str]):
        """测试上传到OSS的完整流程，验证文件成功上传并可访问"""
        client, token, token_type = admin_login
        auth = {"Authorization": f"{token_type.title()} {token}"}

        zip_path = _make_test_zip_to_local()
        file_verify_code, size = await compute_file_md5(zip_path)
        logger.info(f"test_upload_to_oss: file_verify_code={file_verify_code}, size={size}")

        # 1. preflight
        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json={
                "storage_type": "oss",
                "filename": TEST_FILENAME,
                "file_type": "zip",
                "size": size,
                "file_verify_code": file_verify_code,
                "experiment_type": "imzML",
                "organism": "",
                "organism_part": "",
                 "condition": "",
                "sample_growth_conditions": "",
                "sample_stabilization": "",
                "tissue_modification": "",
                "maldi_matrix": "",
                "maldi_matrix_application": "",
                "solvent": "",
                "total_parts": 1
            },
        )
        assert rs.status_code == 200, rs.text
        pre_file_id = rs.json()["file_id"]
        logger.info(f"test_upload_to_oss: preflight ok, pre_file_id={pre_file_id}")

        # 2. get upload oss_sts_token and oss_path
        with open(zip_path, "rb") as f:
            rs = await client.post(
                "/files/upload",
                headers=auth,
                data={"pre_file_id": str(pre_file_id)},
                files={"file": (TEST_FILENAME, f, "application/zip")},
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

        # 4. trigger oss upload complete
        # 测试端直接调用该接口仅供测试，实际上传时由aliyun自动调用

        # 4.1 获取当前登录用户名
        rs = await client.get(
            "/user",
            headers=auth,
        )
        assert rs.status_code == 200, rs.text
        username = rs.json()["username"]

        rs = await client.post(
            "/files/oss_upload_complete",
            headers=auth,
            params={
                "file_id": str(file_id),
                "current_username": username,
                "fc_request_id": fc_request_id,
            },
        )
        assert rs.status_code == 200, rs.text

        await asyncio.sleep(10) # 延迟10秒，等待实验处理完成

        TestFile._uploaded_file_id = file_id
        logger.info((f"test_upload_to_oss passed: username={username} file_id={file_id} size={size} oss_path={oss_path}\n"
                     f"etag={result.etag} fc-request-id={fc_request_id} real-md5={file_verify_code}"))


    # ------------------------------------------------------------------ #
    # 清理
    # ------------------------------------------------------------------ #

    @pytest.mark.file
    @pytest.mark.oss
    @pytest.mark.order("last")
    async def test_cleanup_uploaded_file(self, admin_login: Tuple[AsyncClient, str, str]):
        """清理测试产生的文件，确保不污染数据库"""
        client, token, token_type = admin_login

        if TestFile._uploaded_file_id is None:
            pytest.skip("nothing to clean up")

        rs = await client.delete(
            f"/files/{TestFile._uploaded_file_id}",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 200, rs.text
        assert rs.json()["status"] in ("deleted", "unlinked")
        logger.info(f"test_cleanup_uploaded_file passed: file_id={TestFile._uploaded_file_id}")
        TestFile._uploaded_file_id = None
        TestFile._uploaded_content = None
