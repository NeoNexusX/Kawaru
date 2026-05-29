"""
算法处理运行 — 工具/辅助函数

纯函数，不依赖数据库，不依赖 HTTP 异常。
"""
from __future__ import annotations

from pathlib import Path

from models.processing_models import ProcessingAlgorithmsConfig
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.user_models import UserInDB
from constants.experiment_constants import PROCESSED_STORAGE_ROOT
from constants.file_constants import MAX_PROCESSING_SIZE_PER_USER
from tools.logger import get_logger

logger = get_logger("api.processing_api_helper")


def validate_algorithms_config(config: ProcessingAlgorithmsConfig) -> ProcessingAlgorithmsConfig | None:
    """
    校验算法配置至少选中一个算法

    Returns:
        config 本身（有效），或 None（一个都没选中）
    """
    has_any = (
        config.peak_pick is not None
        or config.normalization is not None
        or config.peak_align is not None
        or config.noise_reduction is not None
        or config.baseline_correction is not None
    )
    return config if has_any else None


def validate_algorithms_json(algorithms_json: str) -> ProcessingAlgorithmsConfig | None:
    """
    从 JSON 字符串反序列化并校验算法配置

    Returns:
        ProcessingAlgorithmsConfig 实例，校验失败返回 None
    """
    try:
        config = ProcessingAlgorithmsConfig.model_validate_json(algorithms_json)
    except Exception: # pylint: disable=broad-exception-caught
        logger.exception("failed to parse algorithms JSON")
        return None
    return validate_algorithms_config(config)


def assert_origin_ready(origin: OriginImzMLInDB) -> bool:
    """校验 origin 状态为 COMPLETED，就绪返回 True"""
    return origin.status == ExperimentStatus.COMPLETED


def assert_run_ownership(run_user_id: int, current_user_id: int) -> bool:
    """校验 process_run 归属当前用户"""
    return run_user_id == current_user_id


def create_run_output_dir(run_id: int) -> Path:
    """创建并返回算法处理输出目录"""
    output_dir = PROCESSED_STORAGE_ROOT / f"run_{run_id}.zarr"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def check_processing_quota(user: UserInDB) -> bool:
    """
    检查用户处理配额是否足够

    Returns:
        True 表示配额充足，False 表示已超限
    """
    if user.processed_file_size >= MAX_PROCESSING_SIZE_PER_USER:
        logger.error(
            "processing quota exceeded user=%s used=%s limit=%s",
            user.id, user.processed_file_size, MAX_PROCESSING_SIZE_PER_USER,
        )
        return False
    return True
