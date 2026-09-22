# -*- coding: utf-8 -*-
"""
FC 算法处理函数入口。

OSS Bucket 已通过 OSSFS 挂载到 /home/storage，输入文件通过挂载读取。
最终 Zarr 先写入 NAS，再通过 OSS SDK 并行上传。
FC RAM 角色自动注入 STS 临时凭证。

环境变量（需手动配置）:
    ALGORITHM_FC_CALLBACK_URL     回调主服务的地址
    CALLBACK_TOKEN               回调鉴权 Token
    OSS_ENDPOINT_URL             OSS 内网 Endpoint
    OSS_BUCKET_NAME              OSS Bucket 名称
    NUMBA_CACHE_DIR              /home/app/numba_cache/massflow_v0.1.0
    NUMBA_CACHE_LOCATOR_CLASSES  numba_cache_locator.ContentHashCacheLocator
    OSS_UPLOAD_WORKERS           OSS 并发上传线程数，默认 8
"""
import base64
import json
import logging
import shutil
import uuid
from pathlib import Path

from models import ProcessingAlgorithmsConfig
from pipeline import (
    TEMP_ROOT,
    _delete_oss_prefix,
    _dir_size_oss,
    notify_callback,
    run_algorithm_pipeline,
)

logger = logging.getLogger()

# OSS Bucket 挂载根目录
STORAGE_ROOT = Path("/home/storage")


def handler(event, context):  # pylint: disable=unused-argument
    """
    FC HTTP 触发器入口。

    触发事件（JSON body）:
        run_id:        算法处理运行 ID
        imzml_path:    imzML 文件的 OSS 路径（如 origin/org_123/sample.imzML）
        config_json:   算法配置 JSON 字符串
        output_path:   处理结果的 OSS 输出路径（如 processed/run_a1b2c3_456.zarr/）
    """
    # ---------- 1. 解析 FC HTTP 事件 ----------

    if isinstance(event, bytes):
        event = event.decode("utf-8")

    try:
        event_json = json.loads(event)
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid event: not a valid JSON string")

    if not isinstance(event_json, dict):
        return _fc_response(400, f"Invalid event: expected dict, got {type(event_json).__name__}")

    if "body" not in event_json:
        return _fc_response(400, "Invalid event: missing 'body' field")

    req_body = event_json["body"]
    if event_json.get("isBase64Encoded"):
        req_body = base64.b64decode(req_body).decode("utf-8")

    try:
        body = json.loads(req_body)
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid body: not a valid JSON string")

    # ---------- 2. 提取参数 ----------

    run_id = body.get("run_id")
    imzml_oss_path = body.get("imzml_path")       # 如 "origin/org_123/sample.imzML"
    config_json_str = body.get("config_json")
    output_oss_path = body.get("output_path")      # 如 "processed/run_a1b2c3_456.zarr/"
    metadata = body.get("metadata", {}) 

    if not all([run_id, imzml_oss_path, config_json_str, output_oss_path]):
        return _fc_response(400, "Missing required fields")

    # ---------- 3. 构造本地路径 ----------

    local_imzml = str(STORAGE_ROOT / imzml_oss_path)

    # ibd 路径由 imzML 路径推导（同目录，.imzML → .ibd）
    if imzml_oss_path.endswith(".imzML"):
        ibd_oss_path = imzml_oss_path[:-6] + ".ibd"
    else:
        ibd_oss_path = imzml_oss_path + ".ibd"
    local_ibd = str(STORAGE_ROOT / ibd_oss_path)

    # 本次运行的中间产物目录（NAS 上）。每个调用使用独立子目录（run_id + 随机后缀），
    # 既避免并发调用互相干扰，也让 finally 中的清理只删本次的目录、不影响其它在跑的调用。
    run_temp_dir = str(Path(TEMP_ROOT) / f"{run_id}_{uuid.uuid4().hex[:8]}")

    # ---------- 4. 执行处理 ----------

    try:
        # 校验 OSSFS 挂载输入文件存在。
        if not Path(local_imzml).exists():
            raise FileNotFoundError(f"imzML file not found: {local_imzml}")
        if not Path(local_ibd).exists():
            raise FileNotFoundError(f"ibd file not found: {local_ibd}")

        # 输出 Zarr 先写入本次运行的 NAS 临时目录，再由 pipeline 并行上传 OSS。
        Path(run_temp_dir).mkdir(parents=True, exist_ok=True)

        logger.info(
            "Algorithm started: run_id=%s input_mode=ossfs "
            "imzml_path=%s imzml_bytes=%s ibd_bytes=%s "
            "output_path=%s temp_dir=%s",
            run_id,
            imzml_oss_path,
            Path(local_imzml).stat().st_size,
            Path(local_ibd).stat().st_size,
            output_oss_path,
            run_temp_dir,
        )

        config = ProcessingAlgorithmsConfig.model_validate_json(config_json_str)
        run_algorithm_pipeline(
            run_id=run_id,
            imzml_path=local_imzml,
            config=config,
            oss_output_path=output_oss_path,
            temp_dir=run_temp_dir,
            metadata=metadata,
        )

        # 通过 OSS API 计算输出大小（使用函数角色 STS 凭证）
        output_size = _dir_size_oss(output_oss_path)

        notify_callback(
            run_id=run_id,
            is_success=True,
            out_path=output_oss_path,
            output_size=output_size,
        )

        logger.info("Algorithm completed: run_id=%s output_size=%s", run_id, output_size)
        return _fc_response(200, {"status": "ok", "run_id": run_id, "output_size": output_size})

    except Exception as e: # pylint: disable=broad-except
        logger.exception("Algorithm failed: run_id=%s", run_id)

        # 清理并行上传过程中可能已经写入的部分 OSS 对象。
        try:
            deleted_objects = _delete_oss_prefix(output_oss_path)
            logger.info(
                "Cleaned up OSS output prefix: %s, deleted_objects=%s",
                output_oss_path,
                deleted_objects,
            )
        except Exception as cleanup_err:
            logger.warning(
                "Failed to clean up OSS output prefix: %s, error: %s",
                output_oss_path,
                cleanup_err,
            )

        notify_callback(
            run_id=run_id,
            is_success=False,
            error_type=type(e).__name__,
            error_message=str(e),
        )

        return _fc_response(500, {"status": "error", "run_id": run_id, "error": str(e)})

    finally:
        # 清理本次运行的 NAS 中间产物。NAS 是持久化存储（与本地盘"实例销毁即清空"不同），
        # 若不清理会不断累积占满；只删本次 run 的独立目录，不影响其它并发调用。
        try:
            run_temp_path = Path(run_temp_dir)
            if run_temp_path.exists():
                shutil.rmtree(run_temp_path)
                logger.info("Cleaned up temp directory: %s", run_temp_dir)
        except Exception as tmp_err:  # pylint: disable=broad-except
            logger.warning(
                "Failed to clean up temp directory: %s, error: %s",
                run_temp_dir,
                tmp_err,
            )


def _fc_response(status_code: int, body) -> dict:
    """构造 FC HTTP 触发器标准响应"""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False) if not isinstance(body, str) else body,
    }
