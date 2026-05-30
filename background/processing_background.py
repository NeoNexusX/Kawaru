"""
FC 算法处理触发模块。

通过 HTTP 触发器统一触发 FC 函数，通过 task_type 字段区分任务类型：
- task_type=algorithm: 算法管线处理
- task_type=tic: TIC 图像生成

FC 函数处理完成后通过回调接口返回结果。
"""
import os

from celery_runtime.state import get_celery_app
from constants.file_constants import FC_ALGORITHM_ENDPOINT, FC_ALGORITHM_TIMEOUT_SECONDS
from tools.fc_client import invoke_fc
from tools.logger import get_logger


logger = get_logger("background.processing_background")
celery_app = get_celery_app()

# 共用同一个端点、token、超时
FC_URL = FC_ALGORITHM_ENDPOINT
FC_TIMEOUT = FC_ALGORITHM_TIMEOUT_SECONDS
FC_TOKEN = os.getenv("OSS_FC_ALGORITHM_TOKEN")


@celery_app.task()
async def trigger_fc_algorithm_task(
    run_id: int,
    imzml_path: str,
    config_json: str,
    output_path: str,
) -> None:
    """触发 FC 执行算法管线处理（异步调用）"""
    event = {
        "task_type": "algorithm",
        "run_id": run_id,
        "imzml_path": imzml_path,
        "config_json": config_json,
        "output_path": output_path,
    }
    logger.info("trigger fc algorithm run_id=%s imzml_path=%s", run_id, imzml_path)
    request_id = await invoke_fc(FC_URL, FC_TOKEN, event, FC_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc algorithm success run_id=%s request_id=%s", run_id, request_id)
    else:
        logger.error("trigger fc algorithm failed run_id=%s", run_id)


@celery_app.task()
async def trigger_fc_tic_task(
    file_id: int,
    imzml_path: str,
) -> None:
    """触发 FC 生成 TIC 图像（异步调用）"""
    event = {
        "task_type": "tic",
        "file_id": file_id,
        "imzml_path": imzml_path,
        "tic_dest": f"tic/file_{file_id}.jpeg",
    }
    logger.info("trigger fc tic file_id=%s imzml_path=%s", file_id, imzml_path)
    request_id = await invoke_fc(FC_URL, FC_TOKEN, event, FC_TIMEOUT)
    if request_id is not None:
        logger.info("trigger fc tic success file_id=%s request_id=%s", file_id, request_id)
    else:
        logger.error("trigger fc tic failed file_id=%s", file_id)
