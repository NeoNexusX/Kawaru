#########################
# Processing Helper - 算法处理工具/辅助函数
# 纯函数，不依赖数据库，不依赖 HTTP 异常。
#########################
from __future__ import annotations

from models.processing_models import ProcessingAlgorithmsConfig, RAW_CONVERT_MARKER
from models.experiment_models import OriginImzMLInDB, ExperimentStatus
from models.user_models import UserInDB
from tools.logger import get_logger

logger = get_logger("api_helpers.processing_helper")


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


def assert_origin_ready(origin: OriginImzMLInDB) -> bool:
    """校验 origin 状态为 COMPLETED，就绪返回 True"""
    return origin.status == ExperimentStatus.COMPLETED


def assert_run_ownership(run_user_id: int, current_user_id: int) -> bool:
    """校验 process_run 归属当前用户"""
    return run_user_id == current_user_id


def run_counts_toward_quota(params_json: str | None, output_size: int | None) -> bool:
    """判断一个 run 是否占用 1 次处理配额。

    口径：非直转（raw_convert 不计）且曾经成功完成（output_size > 0）。
    不能只看 status == COMPLETED：删除流程会把状态改成 DELETING /
    DELETE_FAILED，但 output_size 仍在，finalize 时仍需退还这 1 次。
    迁移脚本按同一谓词回填 processing_count_used，两边必须保持一致。
    """
    return (
        params_json != RAW_CONVERT_MARKER
        and output_size is not None
        and output_size > 0
    )


def check_processing_quota(user: UserInDB) -> bool:
    """
    检查用户处理次数配额是否足够

    Returns:
        True 表示配额充足，False 表示已超限
    """
    if user.processing_count_used >= user.max_processing_count:
        logger.error(
            "processing quota exceeded user=%s used=%s limit=%s",
            user.id, user.processing_count_used, user.max_processing_count,
        )
        return False
    return True
