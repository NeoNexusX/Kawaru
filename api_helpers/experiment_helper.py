#########################
# Experiment Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：实验类型校验
#########################
from __future__ import annotations

from constants.file_constants import ALLOWED_EXPERIMENT_TYPES
from tools.logger import get_logger


logger = get_logger("api_helpers.experiment_helper")


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
