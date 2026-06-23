"""
上传相关测试的共用构件

把 oss / perf 各测试共用的「测试 zip 生成」「preflight 请求体构造」抽到这里，
避免测试模块之间互相 import。

OSS 直传的共用逻辑见 tests/helpers/oss.py。
"""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

# ------------------------------------------------------------------ #
# 测试文件参数，按需修改
# ------------------------------------------------------------------ #
TEST_FILENAME = "test_oss_100MB_2"
TEST_IMZML_SIZE = 5 * 1024 * 1024  # 调整此值控制 imzML 大小
TEST_IBD_SIZE = 5 * 1024 * 1024    # 调整此值控制 ibd 大小


def make_test_zip_to_local(
    imzml_size: int = TEST_IMZML_SIZE,
    ibd_size: int = TEST_IBD_SIZE,
) -> Path:
    """创建合法包含 imzML 和 ibd 文件的 zip 包到本地临时目录，并返回 zip 文件的路径"""
    temp_dir = Path("../test_temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = temp_dir / TEST_FILENAME

    # 分块创建 imzML 文件
    _CHUNK_BUFFER_SIZE = 1024  # 1 KB # pylint:disable = invalid-name
    with open(temp_dir / "test_imzML.imzML", "wb") as zf:
        for _ in range(0, imzml_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 分块创建 ibd 文件
    with open(temp_dir / "test_ibd.ibd", "wb") as zf:
        for _ in range(0, ibd_size, _CHUNK_BUFFER_SIZE):
            zf.write(os.urandom(_CHUNK_BUFFER_SIZE))

    # 压缩 zip 包（STORED 不压缩，最终大小 ≈ imzml + ibd，精确可控）
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
        zf.write(temp_dir / "test_imzML.imzML", "test_imzML.imzML")
        zf.write(temp_dir / "test_ibd.ibd", "test_ibd.ibd")

    # 删除源文件
    (temp_dir / "test_imzML.imzML").unlink()
    (temp_dir / "test_ibd.ibd").unlink()

    return zip_path


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
