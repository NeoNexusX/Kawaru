#########################
# Experiment API Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：目录管理、文件验证、ZIP解压
#########################
from __future__ import annotations

import zipfile
import shutil
from pathlib import Path

from background.celery_app import wait_for_result
from background.experiment_background_helper import packup_oss_file_task
from constants.file_constants import ALLOWED_EXPERIMENT_TYPES
from tools.logger import get_logger


logger = get_logger("api.experiment_api_helper")


async def packup_oss_file(packup_folder: str,
                          zip_file_path: str,
                          timeout_seconds: int | None = 20
                          ) -> tuple[str | None, str | None, str | None]:
    """
    打包OSS文件夹为zip文件

    :param packup_folder: 要打包的oss文件夹路径（如"experiment/aaa/")
    :param zip_file_path: 打包后的zip文件路径（如"data/aaa.zip")
    :return: 打包后的zip文件路径
    """
    res = packup_oss_file_task.delay(packup_folder, zip_file_path) # type: ignore
    dest_zip_path, zip_download_url, fc_request_id = await wait_for_result(res, poll_interval=0.1, timeout=timeout_seconds) # type: ignore # 大文件打包超时时间需配置大一些
    if dest_zip_path is None or zip_download_url is None:
        logger.error(f"failed to packup oss file due to server timeout: from {packup_folder} to {zip_file_path}")
        return (None, None, fc_request_id)
    return dest_zip_path, zip_download_url, fc_request_id


def extract_and_validate_imzml_zip(zip_file_path: Path, target_dir: Path) -> tuple[Path, Path]:
    """
    解压并验证 imzML ZIP 文件,ZIP 文件必须包含两个文件：.imzML 和 .ibd,包含 Zip Slip 路径遍历攻击防护

    Args:
        zip_file_path: ZIP 文件路径
        target_dir: 解压目标目录

    Returns:
        成功时返回 (imzml_path, ibd_path) 元组

    Raise:
        - zipfile.BadZipFile: ZIP 文件无效
        - FileNotFoundError: 缺少 .imzML 或 .ibd 文件
        - ValueError: ZIP 文件包含不安全的路径
    """
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            # 验证 ZIP 条目以防止路径遍历（Zip Slip）
            temp_dir_resolved = target_dir.resolve()
            for member in zip_ref.namelist():
                member_path = (target_dir / member).resolve()
                # 检查路径是否在临时目录内（避免路径遍历攻击）
                if not member_path.is_relative_to(temp_dir_resolved):
                    logger.error("unsafe file path in zip file: %s", member)
                    # TODO: 应该返回封禁标志，封禁该用户所有操作
                    raise ValueError("unsafe file path in zip file")
            zip_ref.extractall(target_dir)

        # 查找 imzML 和 ibd 文件
        imzml_path = None
        ibd_path = None

        for file_path in target_dir.iterdir():
            if file_path.suffix == ".imzML":
                imzml_path = file_path
            elif file_path.suffix.lower() == ".ibd":
                ibd_path = file_path

        # 验证文件是否存在
        if not imzml_path or not ibd_path:
            logger.error("zip file must contain both .imzML and .ibd files")
            raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")

        # 验证文件（目前直接通过）
        if not validate_imzml_files(imzml_path, ibd_path):
            logger.error("imzML file validation failed")
            raise AssertionError("Invalid imzML or ibd file")

    except Exception as e:
        logger.error("invalid zip file: %s", e)
        shutil.rmtree(target_dir, ignore_errors=True)
        raise

    return imzml_path, ibd_path


def validate_experiment_type(experiment_type: str) -> bool:
    """
    校验实验类型是否在允许列表中

    Args:
        experiment_type: 实验类型(如 imzML、raw 等)

    Returns:
        True 表示验证通过,False 表示验证失败
    """
    if experiment_type and (experiment_type in ALLOWED_EXPERIMENT_TYPES):
        return True

    return False


def validate_imzml_files(imzml_path: Path, ibd_path: Path) -> bool: # pylint: disable=unused-argument
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
