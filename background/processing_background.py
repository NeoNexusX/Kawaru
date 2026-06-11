"""
FC 函数触发模块。

通过 HTTP 触发器触发 FC 函数：
- fc-algorithm: 算法管线处理
- fc-tic: TIC 图像生成（centroid 模式）
- fc-feature: UMAP/KMeans 特征分析（profile/continuous 模式）

FC 函数处理完成后通过回调接口返回结果。
"""
import os

from celery_runtime.state import get_celery_app
from constants.file_constants import (
    FC_ALGORITHM_ENDPOINT,
    FC_ALGORITHM_TIMEOUT_SECONDS,
    FC_TIC_ENDPOINT,
    FC_TIC_TIMEOUT_SECONDS,
    FC_FEATURE_ENDPOINT,
    FC_FEATURE_TIMEOUT_SECONDS,
)
from tools.fc_client import invoke_fc
from tools.logger import get_logger


logger = get_logger("background.processing_background")
celery_app = get_celery_app()

# ---- fc-algorithm（算法管线处理）----
FC_ALGORITHM_URL = FC_ALGORITHM_ENDPOINT
FC_ALGORITHM_TIMEOUT = FC_ALGORITHM_TIMEOUT_SECONDS
FC_ALGORITHM_TOKEN = os.getenv("OSS_FC_ALGORITHM_TOKEN")

# ---- fc-tic（TIC 图像生成，centroid 模式）----
FC_TIC_URL = FC_TIC_ENDPOINT
FC_TIC_TIMEOUT = FC_TIC_TIMEOUT_SECONDS
FC_TIC_TOKEN = os.getenv("OSS_FC_TIC_TOKEN")

# ---- fc-feature（UMAP/KMeans 特征分析，profile 模式，大内存）----
FC_FEATURE_URL = FC_FEATURE_ENDPOINT
FC_FEATURE_TIMEOUT = FC_FEATURE_TIMEOUT_SECONDS
FC_FEATURE_TOKEN = os.getenv("OSS_FC_FEATURE_TOKEN")


@celery_app.task()
async def trigger_fc_algorithm_task(
    run_id: int,
    imzml_path: str,
    config_json: str,
    output_path: str,
) -> None:
    """触发 fc-algorithm 执行算法管线处理（异步调用）"""
    event = {
        "run_id": run_id,
        "imzml_path": imzml_path,
        "config_json": config_json,
        "output_path": output_path,
    }
    logger.info("trigger fc algorithm run_id=%s imzml_path=%s", run_id, imzml_path)
    request_id = await invoke_fc(FC_ALGORITHM_URL, FC_ALGORITHM_TOKEN, event, FC_ALGORITHM_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc algorithm success run_id=%s request_id=%s", run_id, request_id)
    else:
        logger.error("trigger fc algorithm failed run_id=%s", run_id)


@celery_app.task()
async def trigger_fc_tic_task(
    file_id: int,
    imzml_path: str,
) -> None:
    """触发 fc-tic 生成 TIC 图像（异步调用，centroid 模式）"""
    event = {
        "file_id": file_id,
        "imzml_path": imzml_path,
        "tic_dest": f"tic/file_{file_id}/",
    }
    logger.info("trigger fc tic file_id=%s imzml_path=%s", file_id, imzml_path)
    request_id = await invoke_fc(FC_TIC_URL, FC_TIC_TOKEN, event, FC_TIC_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc tic success file_id=%s request_id=%s", file_id, request_id)
    else:
        logger.error("trigger fc tic failed file_id=%s", file_id)


@celery_app.task()
async def trigger_fc_feature_task(
    file_id: int,
    imzml_path: str,
) -> None:
    """触发 fc-feature 生成 UMAP/KMeans 特征分析图像（异步调用，profile 模式）"""
    event = {
        "file_id": file_id,
        "imzml_path": imzml_path,
        "feature_dest": f"feature/file_{file_id}/",
    }
    logger.info("trigger fc feature file_id=%s imzml_path=%s", file_id, imzml_path)
    request_id = await invoke_fc(FC_FEATURE_URL, FC_FEATURE_TOKEN, event, FC_FEATURE_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc feature success file_id=%s request_id=%s", file_id, request_id)
    else:
        logger.error("trigger fc feature failed file_id=%s", file_id)
