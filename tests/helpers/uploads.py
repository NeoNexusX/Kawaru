"""
上传相关测试的共用构件

把 oss / perf 各测试共用的「测试 zip 生成」「preflight 请求体构造」抽到这里，
避免测试模块之间互相 import。

OSS 直传的共用逻辑见 tests/helpers/oss.py。
"""
from __future__ import annotations

import hashlib
import os
import shutil
import zipfile
from pathlib import Path

from httpx import AsyncClient

from constants.file_constants import OSS_ORIGIN_STORAGE_ROOT
from tools.cal_hash import compute_imzml_md5
from tools.logger import get_logger

from .oss import parse_file_id_from_oss_path, put_oss_object, upload_file_to_oss

logger = get_logger("test.helpers.uploads")

# ------------------------------------------------------------------ #
# 测试文件参数，按需修改
# ------------------------------------------------------------------ #
TEST_FILENAME = "test_oss_100MB_2"
TEST_IMZML_SIZE = 5 * 1024 * 1024  # 调整此值控制 imzML 大小
TEST_IBD_SIZE = 5 * 1024 * 1024    # 调整此值控制 ibd 大小


def _file_md5(path: Path) -> str:
    """同步计算单文件 MD5（测试用，文件小，不异步）"""
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def make_test_zip_to_local(
    imzml_size: int = TEST_IMZML_SIZE,
    ibd_size: int = TEST_IBD_SIZE,
    filename: str = TEST_FILENAME,
) -> tuple[Path, str, str]:
    """创建合法包含 imzML 和 ibd 文件的 zip 包到本地临时目录。

    filename 可自定义（删除测试用独立文件名与 oss 组隔离，互不残留影响）。

    Returns:
        (zip_path, imzml_md5, ibd_md5)
    """
    temp_dir = Path("../test_temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = temp_dir / filename

    stem = Path(filename).stem  # 统一 stem，保证内外文件名一致

    # 分块创建 imzML 文件
    _CHUNK_BUFFER_SIZE = 1024  # 1 KB # pylint:disable = invalid-name
    imzml_tmp = temp_dir / f"{stem}.imzML"
    with open(imzml_tmp, "wb") as zf:
        for _ in range(0, imzml_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 分块创建 ibd 文件
    ibd_tmp = temp_dir / f"{stem}.ibd"
    with open(ibd_tmp, "wb") as zf:
        for _ in range(0, ibd_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 压缩前算好 hash
    imzml_hash = _file_md5(imzml_tmp)
    ibd_hash = _file_md5(ibd_tmp)

    # 压缩 zip 包（STORED 不压缩，最终大小 ≈ imzml + ibd，精确可控）
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        zf.write(imzml_tmp, f"{stem}.imzML")
        zf.write(ibd_tmp, f"{stem}.ibd")

    # 删除源文件
    imzml_tmp.unlink()
    ibd_tmp.unlink()

    return zip_path, imzml_hash, ibd_hash


def build_preflight_payload(
    *,
    size: int,
    file_verify_code: str,
    storage_type: str = "oss",
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
    }


async def upload_completed_file(
    client: AsyncClient,
    auth: dict[str, str],
    *,
    filename: str,
    imzml_size: int,
    ibd_size: int,
) -> dict:
    """真实上传链（无 FC）：preflight → STS 直传 zip → 自传 origin 对象 → 模拟回调。

    文件行 / origin 行 / UserFileLink 全部由服务端真实代码创建，回调的真实校验链
    （zip 大小探测、解压产物存在性探测、size=imzml+ibd 核对、哈希一致性）全部真实
    执行；仅省去 FC 解压环节——随机数据本就过不了 FC 的 imzML 谱图校验（见
    test_deletion_flow 模块 docstring），改为先向 origin 路径直传占位对象再回调。

    删除场景 ②③ 共用；与 test_delete_file_flow 的差别只有「FC 解压等待」这一步。

    Returns:
        dict: file_id / public_id / oss_path / imzml_path / ibd_path /
              size / file_verify_code
    """
    zip_path, imzml_hash, ibd_hash = make_test_zip_to_local(
        imzml_size=imzml_size,
        ibd_size=ibd_size,
        filename=filename,
    )
    try:
        file_verify_code, size = await compute_imzml_md5(zip_path)  # type: ignore

        rs = await client.post(
            "/files/preflight",
            headers=auth,
            json=build_preflight_payload(
                size=size,
                file_verify_code=file_verify_code,
                storage_type="oss",
                filename=filename,
            ),
        )
        assert rs.status_code == 200, f"preflight failed: {rs.text}"
        pre_public_id = rs.json()["public_id"]

        rs = await client.post(
            "/files/upload",
            headers=auth,
            data={"pre_public_id": pre_public_id, "filename": filename},
        )
        assert rs.status_code == 200, f"upload authorize failed: {rs.text}"
        body = rs.json()
        public_id = body["public_id"]
        assert public_id == pre_public_id
        file_id = parse_file_id_from_oss_path(body["oss_path"])

        await upload_file_to_oss(
            oss_sts_token=body["oss_sts_token"],
            oss_bucket=body["oss_bucket"],
            oss_path=body["oss_path"],
            filepath=zip_path,
            checkpoint_dir=zip_path.parent / "checkpoint",
        )

        # 先放 origin 占位对象再回调：回调落库前会真实探测对象存在性
        stem = Path(filename).stem
        origin_root = OSS_ORIGIN_STORAGE_ROOT.rstrip("/")
        imzml_path = f"{origin_root}/{file_verify_code}/{file_id}/{stem}.imzML"
        ibd_path = f"{origin_root}/{file_verify_code}/{file_id}/{stem}.ibd"
        await put_oss_object(oss_path=imzml_path, data=os.urandom(1024))
        await put_oss_object(oss_path=ibd_path, data=os.urandom(1024))

        rs = await client.post(
            "/callback/oss_upload_callback",
            json={
                "file_id": file_id,
                "file_path": body["oss_path"],
                "is_success": True,
                "act_hash": file_verify_code,
                "imzml_path": imzml_path,
                "ibd_path": ibd_path,
                "imzml_size": imzml_size,
                "ibd_size": ibd_size,
            },
            headers={"X-FC-Token": os.environ.get("FC_CALLBACK_TOKEN", "test-fc-callback-token")},
        )
        assert rs.status_code == 200, f"FC callback failed: {rs.status_code} {rs.text}"
        assert rs.json()["status"] == "completed", rs.text
        logger.info("upload_completed_file ok: public_id=%s file_id=%s oss_path=%s",
                    public_id, file_id, body["oss_path"])
        return {
            "file_id": file_id,
            "public_id": public_id,
            "oss_path": body["oss_path"],
            "imzml_path": imzml_path,
            "ibd_path": ibd_path,
            "size": size,
            "file_verify_code": file_verify_code,
        }
    finally:
        zip_path.unlink(missing_ok=True)
        shutil.rmtree(zip_path.parent / "extract_tmp", ignore_errors=True)
        shutil.rmtree(zip_path.parent / "checkpoint", ignore_errors=True)
