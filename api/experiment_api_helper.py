#########################
# Experiment API Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：目录管理、文件验证、ZIP解压
#########################
from __future__ import annotations

import zipfile
import shutil
from pathlib import Path

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from models.experiment_models import ExperimentImzMLInDB, ExperimentStatus

from constants.experiment_constants import (
    EXPERIMENT_STORAGE_ROOT,
    EXPERIMENT_TEMP_ROOT,
    EXPERIMENT_PROCESSED_ROOT,
    SOURCE_FILE_PATH_CLEARED,
)

from logger import get_logger

logger = get_logger("api.experiment_api_helper")



def ensure_experiment_dirs() -> None:
    """
    确保实验存储目录存在
    
    创建以下目录：
    - EXPERIMENT_STORAGE_ROOT: 实验根目录
    - EXPERIMENT_TEMP_ROOT: 临时目录
    - EXPERIMENT_PROCESSED_ROOT: 处理完成目录
    """
    for path in (EXPERIMENT_STORAGE_ROOT, EXPERIMENT_TEMP_ROOT, EXPERIMENT_PROCESSED_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def validate_imzml_files(imzml_path: Path, ibd_path: Path) -> bool:
    """
    验证 imzML 和 ibd 文件是否为有效可读的 MSI 实验数据
    
    Args:
        imzml_path: imzML 文件路径
        ibd_path: ibd 文件路径
    
    Returns:
        True 表示验证通过，False 表示验证失败
    
    TODO: 后续扩展验证逻辑
    - 检查文件格式是否正确
    - 验证文件完整性
    - 检查文件大小是否合理
    - 验证 imzML 和 ibd 文件的关联性
    """
    # 目前直接返回 True，后续扩展
    return True


def extract_and_validate_imzml_zip(zip_file_path: Path, temp_dir: Path) -> tuple[Path, Path] | None:
    """
    解压并验证 imzML ZIP 文件
    
    ZIP 文件必须包含两个文件：.imzML 和 .ibd
    包含 Zip Slip 路径遍历攻击防护
    
    Args:
        zip_file_path: ZIP 文件路径
        temp_dir: 解压目标目录
    
    Returns:
        成功时返回 (imzml_path, ibd_path) 元组
        失败时返回 None
    """
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            # 验证 ZIP 条目以防止路径遍历（Zip Slip）
            temp_dir_resolved = temp_dir.resolve()
            for member in zip_ref.namelist():
                member_path = (temp_dir / member).resolve()
                # 检查路径是否在临时目录内（避免路径遍历攻击）
                if not member_path.is_relative_to(temp_dir_resolved):
                    logger.error("unsafe file path in zip file: %s", member)
                    return None
            zip_ref.extractall(temp_dir)
    except (zipfile.BadZipFile, OSError) as e:
        logger.error("failed to extract zip file error=%s", e)
        return None

    # 查找 imzML 和 ibd 文件
    imzml_path = None
    ibd_path = None

    for file_path in temp_dir.iterdir():
        if file_path.suffix == ".imzML":
            imzml_path = file_path
        elif file_path.suffix.lower() == ".ibd":
            ibd_path = file_path

    # 验证文件是否存在
    if not imzml_path or not ibd_path:
        logger.error("zip file must contain both .imzML and .ibd files")
        return None

    # 验证文件（目前直接通过）
    if not validate_imzml_files(imzml_path, ibd_path):
        logger.error("imzML file validation failed")
        return None

    return imzml_path, ibd_path


def create_experiment_directories(experiment_id: int) -> tuple[Path, Path]:
    """创建并返回 (temp_dir, processed_dir)"""
    ensure_experiment_dirs()
    temp_dir = EXPERIMENT_TEMP_ROOT / f"exp_{experiment_id}"
    processed_dir = EXPERIMENT_PROCESSED_ROOT / f"exp_{experiment_id}"

    temp_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    return temp_dir, processed_dir


def move_experiment_files(
    imzml_temp_path: Path,
    ibd_temp_path: Path,
    experiment_dir: Path,
    experiment_name: str,
) -> tuple[Path, Path]:
    """将临时文件移动到正式存储位置，返回最终路径"""
    final_imzml_path = experiment_dir / f"{experiment_name}.imzML"
    final_ibd_path = experiment_dir / f"{experiment_name}.ibd"

    shutil.move(str(imzml_temp_path), str(final_imzml_path))
    shutil.move(str(ibd_temp_path), str(final_ibd_path))

    return final_imzml_path, final_ibd_path


def delete_source_file_and_clear_path(source_file) -> bool:
    """删除源文件并清空数据库中的 path 字段，返回是否成功"""
    if not getattr(source_file, "path", None):
        return True

    try:
        Path(source_file.path).unlink(missing_ok=True)
        # 不再将 path 置为空字符串，而是使用模块级占位常量
        source_file.path = SOURCE_FILE_PATH_CLEARED
        return True
    except (OSError, PermissionError) as e:
        logger.warning("failed to delete source file error=%s", e)
        return False


def cleanup_temp_directory(temp_dir: Path | None) -> None:
    """清理临时目录"""
    if temp_dir and temp_dir.exists():
        try:
            shutil.rmtree(temp_dir)
        except (OSError, PermissionError, shutil.Error) as e:
            logger.error("failed to cleanup temp directory error=%s", e)

async def is_file_uploaded(session: AsyncSession, file_record) -> bool:
    """
    判断文件是否真正上传：
    - 对于 experiment_type==imzML，需有对应 experiment 且 status=COMPLETED
    - 其他类型可根据实际业务扩展
    """
    if getattr(file_record, 'experiment_type', None) == 'imzML':
        exp = (await session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.source_file_id == file_record.file_id,
                ExperimentImzMLInDB.status == ExperimentStatus.COMPLETED
            )
        )).first()
        return exp is not None
    # 其他类型直接视为已上传
    return True
