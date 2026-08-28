"""
FC 函数触发模块。

通过 HTTP 触发器触发 FC 函数：
- fc-algorithm: 算法管线处理
- fc-tic: 非 continuous 存储模式的 TIC 预览图生成
- fc-feature: continuous 存储模式的 UMAP 预览图生成

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
    FC_RAW_CONVERT_ENDPOINT,
    FC_RAW_CONVERT_TIMEOUT_SECONDS,
    FC_CLUSTERING_ENDPOINT,
    FC_CLUSTERING_TIMEOUT_SECONDS,
)
from tools.fc_client import invoke_fc
from tools.logger import get_logger


logger = get_logger("background.processing_background")
celery_app = get_celery_app()

# ---- fc-algorithm（算法管线处理）----
FC_ALGORITHM_URL = FC_ALGORITHM_ENDPOINT
FC_ALGORITHM_TIMEOUT = FC_ALGORITHM_TIMEOUT_SECONDS
FC_ALGORITHM_TOKEN = os.getenv("OSS_FC_ALGORITHM_TOKEN")

# ---- fc-tic（非 continuous 存储模式的 TIC 预览图）----
FC_TIC_URL = FC_TIC_ENDPOINT
FC_TIC_TIMEOUT = FC_TIC_TIMEOUT_SECONDS
FC_TIC_TOKEN = os.getenv("OSS_FC_TIC_TOKEN")

# ---- fc-feature（continuous 存储模式的 UMAP 预览图，大内存）----
FC_FEATURE_URL = FC_FEATURE_ENDPOINT
FC_FEATURE_TIMEOUT = FC_FEATURE_TIMEOUT_SECONDS
FC_FEATURE_TOKEN = os.getenv("OSS_FC_FEATURE_TOKEN")

# ---- fc-raw-convert（imzML 直转 Zarr，无算法处理）----
FC_RAW_CONVERT_URL = FC_RAW_CONVERT_ENDPOINT
FC_RAW_CONVERT_TIMEOUT = FC_RAW_CONVERT_TIMEOUT_SECONDS
FC_RAW_CONVERT_TOKEN = os.getenv("OSS_FC_RAW_CONVERT_TOKEN")

# ---- fc-clustering（UMAP/KMeans 聚类分析，结果写回源 zarr）----
FC_CLUSTERING_URL = FC_CLUSTERING_ENDPOINT
FC_CLUSTERING_TIMEOUT = FC_CLUSTERING_TIMEOUT_SECONDS
FC_CLUSTERING_TOKEN = os.getenv("OSS_FC_CLUSTERING_TOKEN")


@celery_app.task()
async def trigger_fc_algorithm_task(
    run_id: int,
    imzml_path: str,
    config_json: str,
    output_path: str,
    metadata: dict | None = None,
) -> None:
    """触发 fc-algorithm 执行算法管线处理（异步调用）"""
    event = {
        "run_id": run_id,
        "imzml_path": imzml_path,
        "config_json": config_json,
        "output_path": output_path,
        "metadata": metadata or {},
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
    """触发 fc-tic 生成 TIC 预览图（异步调用）。"""
    event = {
        "file_id": file_id,
        "imzml_path": imzml_path,
        "image_dest": f"images/file_{file_id}/",
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
    """触发 fc-feature 生成 UMAP 预览图（异步调用）。"""
    event = {
        "file_id": file_id,
        "imzml_path": imzml_path,
        "image_dest": f"images/file_{file_id}/",
    }
    logger.info("trigger fc feature file_id=%s imzml_path=%s", file_id, imzml_path)
    request_id = await invoke_fc(FC_FEATURE_URL, FC_FEATURE_TOKEN, event, FC_FEATURE_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc feature success file_id=%s request_id=%s", file_id, request_id)
    else:
        logger.error("trigger fc feature failed file_id=%s", file_id)


@celery_app.task()
async def trigger_fc_raw_convert_task(
    run_id: int,
    imzml_path: str,
    ibd_path: str,
    output_path: str,
    metadata: dict | None = None,
) -> None:
    """触发 fc-raw-convert 执行 imzML 直转 Zarr（无算法处理，异步调用）"""
    event = {
        "run_id": run_id,
        "imzml_path": imzml_path,
        "ibd_path": ibd_path,
        "output_path": output_path,
        "metadata": metadata or {},
    }
    logger.info("trigger fc raw_convert run_id=%s imzml_path=%s", run_id, imzml_path)
    request_id = await invoke_fc(
        FC_RAW_CONVERT_URL, FC_RAW_CONVERT_TOKEN, event, FC_RAW_CONVERT_TIMEOUT,
    )
    if request_id is not None:
        logger.info("trigger fc raw_convert success run_id=%s request_id=%s", run_id, request_id)
    else:
        logger.error("trigger fc raw_convert failed run_id=%s", run_id)


@celery_app.task()
async def trigger_fc_clustering_task(
    run_id: int,
    source_zarr_path: str,
) -> None:
    """触发 fc-clustering 执行 UMAP/KMeans 聚类分析（异步调用，结果写回源 zarr）"""
    event = {
        "run_id": run_id,
        "source_zarr_path": source_zarr_path,
    }
    logger.info("trigger fc clustering run_id=%s source_zarr=%s", run_id, source_zarr_path)
    request_id = await invoke_fc(
        FC_CLUSTERING_URL, FC_CLUSTERING_TOKEN, event, FC_CLUSTERING_TIMEOUT,
    )
    if request_id is not None:
        logger.info("trigger fc clustering success run_id=%s request_id=%s", run_id, request_id)
    else:
        logger.error("trigger fc clustering failed run_id=%s", run_id)
