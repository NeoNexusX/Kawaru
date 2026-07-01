"""
上传相关测试的共用构件

把 oss / perf 各测试共用的「测试 zip 生成」「preflight 请求体构造」抽到这里，
避免测试模块之间互相 import。

OSS 直传的共用逻辑见 tests/helpers/oss.py。
"""
from __future__ import annotations

import hashlib
import os
import zipfile
from pathlib import Path

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
) -> tuple[Path, str, str]:
    """创建合法包含 imzML 和 ibd 文件的 zip 包到本地临时目录。

    Returns:
        (zip_path, imzml_md5, ibd_md5)
    """
    temp_dir = Path("../test_temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = temp_dir / TEST_FILENAME

    stem = Path(TEST_FILENAME).stem  # 统一 stem，保证内外文件名一致

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
