#########################
# Experiment Helper - 底层工具函数
# 职责：纯工具函数，无业务逻辑，无数据库依赖
# 包含：目录管理、文件验证、ZIP解压、OSS 打包任务派发
#########################
from __future__ import annotations

from celery.result import AsyncResult

from celery_runtime.result import wait_for_result
from background.experiment_background_helper import packup_oss_file_task
from constants.file_constants import ALLOWED_EXPERIMENT_TYPES
from tools.logger import get_logger


logger = get_logger("api_helpers.experiment_helper")


async def packup_oss_file(filename: str, packup_folder: str, zip_file_path: str) -> str | None:
    """
    打包OSS文件夹为zip文件

    :param filename: 文件名
    :param packup_folder: 要打包的oss文件夹路径（如"experiment/aaa/")
    :param zip_file_path: 打包后的zip文件路径（如"data/aaa.zip")
    :return: 函数调用ID
    """
    res: AsyncResult = packup_oss_file_task.delay(filename, packup_folder, zip_file_path)  # type: ignore
    fc_request_id: str | None = await wait_for_result(res, poll_interval=0.1) # type: ignore # 大文件打包超时时间需配置大一些
    if fc_request_id is None:
        logger.error(f"failed to trigger packup oss file due to server timeout: from {packup_folder} to {zip_file_path}")
        return None
    return fc_request_id


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
