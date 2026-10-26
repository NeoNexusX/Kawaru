# -*- coding: utf-8 -*-
"""
FC TIC 图像生成函数入口。

OSS Bucket 通过 OSSFS 挂载到 /home/storage，文件直接本地读写。

回调上报的 image_path 是图片所在目录（如 images/{public_id}/）而非具体图片路径，
具体文件名由消费方拼接（本函数只生成 preview.jpg）。

环境变量（需在 FC 控制台手动配置）:
    TIC_FC_CALLBACK_URL    回调主服务 /callback/tic_callback 的完整 URL
    STORAGE_ROOT（可选）    OSSFS 挂载根目录，默认 /home/storage
"""

import json
import logging
import os
import random
from pathlib import Path

import requests
from massflow.data_manager import MSDataManagerImzML
from massflow.tools.plot_ion_image import plot_tic_image

logger = logging.getLogger()

STORAGE_ROOT = Path(os.getenv("STORAGE_ROOT", "/home/storage"))
CALLBACK_URL = os.environ["TIC_FC_CALLBACK_URL"]
CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]


def handler(event, context):  # pylint: disable=unused-argument
    """
    触发事件（JSON body）:
        file_id:    文件 ID
        imzml_path: imzML 文件的 OSS 相对路径（如 origin/org_123/sample.imzML）
        image_dest: 图像输出 OSS 相对目录（如 images/{public_id}/）
    """
    logger.info("fc-tic receive event: %s", event)

    body = _parse_event(event)
    if isinstance(body, dict) and "statusCode" in body:
        return body

    file_id = body.get("file_id")
    imzml_path = body.get("imzml_path")
    image_dest = body.get("image_dest")

    if not all([file_id is not None, imzml_path, image_dest]):
        return _fc_response(400, "Missing required fields: file_id, imzml_path, image_dest")

    image_dest_dir = image_dest.rstrip("/")
    # 回调/数据库记录输出目录本身（如 images/{public_id}/）而非具体图片路径，
    # 具体文件名由消费方拼接（本函数只生成 preview.jpg）
    image_dest = f"{image_dest_dir}/"
    local_imzml = str(STORAGE_ROOT / imzml_path)
    local_output = str(STORAGE_ROOT / image_dest_dir)

    dm = None
    try:
        if not Path(local_imzml).exists():
            raise FileNotFoundError(f"imzML file not found: {local_imzml}")

        Path(local_output).mkdir(parents=True, exist_ok=True)

        dm = MSDataManagerImzML(filepath=local_imzml)
        dm.load_head_data()

        plot_tic_image(dm,
            output_path=os.path.join(local_output, "preview.jpg"),
            cmap=random.choice(["viridis", "inferno", "hot"]),
        )

        _notify_callback(file_id=file_id, is_success=True, image_path=image_dest)
        logger.info("TIC completed: file_id=%s image_dest=%s", file_id, image_dest)
        return _fc_response(200, {"status": "ok", "file_id": file_id, "image_path": image_dest})

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("TIC failed: file_id=%s", file_id)

        try:
            p = Path(local_output)
            if p.exists():
                import shutil
                shutil.rmtree(p)
        except Exception:  # pylint: disable=broad-except
            pass

        _notify_callback(
            file_id=file_id,
            is_success=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        return _fc_response(500, {"status": "error", "file_id": file_id, "error": str(exc)})

    finally:
        if dm is not None:
            try:
                dm.close()
            except Exception:  # pylint: disable=broad-except
                pass


# ======================================================================
# 内部 helpers
# ======================================================================

def _parse_event(event) -> dict:
    import base64

    if isinstance(event, dict) and "statusCode" not in event:
        if "body" not in event and "file_id" in event:
            return event

    if isinstance(event, bytes):
        event = event.decode("utf-8")

    try:
        event_json = json.loads(event) if isinstance(event, str) else event
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
        return json.loads(req_body) if isinstance(req_body, str) else req_body
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid body: not a valid JSON string")


def _fc_response(status_code: int, body) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False) if not isinstance(body, str) else body,
    }


def _notify_callback(
    file_id: int,
    is_success: bool,
    image_path: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    body = {
        "file_id": file_id,
        "is_success": is_success,
        "image_path": image_path,
        "error_type": error_type,
        "error_message": error_message,
    }
    try:
        res = requests.post(
            CALLBACK_URL,
            json=body,
            headers={
                "Content-Type": "application/json",
                "X-FC-Token": CALLBACK_TOKEN,
            },
            timeout=30,
        )
        if res.status_code != 200:
            logger.error("TIC callback failed: status=%s body=%s", res.status_code, res.text)
    except Exception:  # pylint: disable=broad-except
        logger.exception("TIC callback request failed")
