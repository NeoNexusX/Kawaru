# -*- coding: utf-8 -*-
"""
FC 图像分析函数入口。

根据 imzML 数据的 continuous 属性自动选择分析方式：
- continuous (profile 模式): 生成三张图（需要 4GB+ 内存）
    preview.jpg   UMAP + KMeans 特征分析图
    preview_2.jpg TIC 总离子流图
    preview_3.jpg 随机离子图（mz_tolerance=0.1，structure_only 仅采集区域）
- 非 continuous (centroid 模式): 降级生成 TIC 图像 preview.jpg（三种 cmap 随机）

回调上报的 image_path 是图片所在目录（如 images/file_15/）而非具体图片路径，
具体文件名由消费方按 preview.jpg / preview_2.jpg / preview_3.jpg 拼接。

OSS Bucket 通过 OSSFS 挂载到 /home/storage，文件直接本地读写。

小文件（默认 < LOCAL_CACHE_THRESHOLD_BYTES）会先复制到 /tmp 本地盘，
避免 OSSFS 随机读放大；大文件继续走 OSSFS 流式读取。

环境变量（需在 FC 控制台手动配置）:
    FEATURE_FC_CALLBACK_URL    回调主服务 /callback/feature_callback 的完整 URL
    FEATURE_FC_CALLBACK_TOKEN  回调鉴权 token
    STORAGE_ROOT（可选）        OSSFS 挂载根目录，默认 /home/storage
    LOCAL_CACHE_THRESHOLD_BYTES（可选）  本地缓存阈值（字节），默认 8GB
"""

import json
import logging
import os
import random
import shutil
import tempfile
from pathlib import Path

import requests
from massflow.data_manager import MSDataManagerImzML
from massflow.segmentation import plot_umap_image
from massflow.tools.plot_ion_image import plot_ion_image, plot_tic_image

logger = logging.getLogger()

STORAGE_ROOT = Path(os.getenv("STORAGE_ROOT", "/home/storage"))
CALLBACK_URL = os.environ["FEATURE_FC_CALLBACK_URL"]
CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]
LOCAL_CACHE_THRESHOLD_BYTES = int(os.getenv("LOCAL_CACHE_THRESHOLD_BYTES", str(8 * 1024 ** 3)))
LOCAL_CACHE_DIR = Path(os.getenv("LOCAL_CACHE_DIR", "/tmp"))

# 随机离子图的 m/z 容差窗口（±0.1 Da）
RANDOM_ION_MZ_TOLERANCE = 0.1


def handler(event, context):  # pylint: disable=unused-argument
    """
    触发事件（JSON body）:
        file_id:      文件 ID
        imzml_path:   imzML 文件的 OSS 相对路径（如 origin/org_123/sample.imzML）
        image_dest:   输出 OSS 相对目录（如 images/file_15/）

    分析逻辑:
        1. 根据 imzML + ibd 总体积决定走 OSSFS 流式还是先缓存到 /tmp
        2. 根据 data_manager.ms.meta.continuous 判断：
           - continuous=True  → preview.jpg (UMAP) + preview_2.jpg (TIC)
                                + preview_3.jpg (随机离子, tol=0.1)
           - continuous=False → 降级只生成 preview.jpg (TIC，三种 cmap 随机)
        3. 三张图任一失败按整体失败处理：清理输出目录并回调 is_success=False
        4. 回调/数据库记录的 image_path 为输出目录本身（如 images/file_15/）
    """
    logger.info("fc-feature receive event: %s", event)

    body = _parse_event(event)
    if isinstance(body, dict) and "statusCode" in body:
        return body

    file_id = body.get("file_id")
    imzml_path = body.get("imzml_path")
    image_dest = body.get("image_dest")

    if not all([file_id is not None, imzml_path, image_dest]):
        return _fc_response(400, "Missing required fields: file_id, imzml_path, image_dest")

    image_dest_dir = image_dest.rstrip("/")
    # 回调/数据库记录输出目录本身（如 images/file_15/）而非具体图片路径，
    # 具体文件名由消费方按 preview.jpg / preview_2.jpg / preview_3.jpg 拼接
    image_dest = f"{image_dest_dir}/"
    remote_imzml = STORAGE_ROOT / imzml_path
    local_output = STORAGE_ROOT / image_dest_dir

    local_imzml = remote_imzml
    tmp_dir: Path | None = None
    dm = None
    try:
        if not remote_imzml.exists():
            raise FileNotFoundError(f"imzML file not found: {remote_imzml}")

        local_output.mkdir(parents=True, exist_ok=True)

        # 小文件先缓存到本地，大文件保持流式
        local_imzml, tmp_dir = _ensure_local(remote_imzml)

        dm = MSDataManagerImzML(filepath=str(local_imzml))
        dm.load_head_data()

        if dm.ms.meta.continuous:
            # continuous=True: profile 模式，一次会话内连续生成三张图
            logger.info("Data is continuous, running UMAP analysis file_id=%s", file_id)
            plot_umap_image(dm, output_path=str(local_output / "preview.jpg"))

            logger.info("Generating TIC image file_id=%s", file_id)
            plot_tic_image(
                dm,
                output_path=str(local_output / "preview_2.jpg"),
                cmap=random.choice(["viridis", "inferno", "hot"]),
            )

            mz = _pick_random_mz(dm)
            logger.info("Generating random ion image file_id=%s mz=%.4f", file_id, mz)
            plot_ion_image(
                dm,
                mz=mz,
                mz_tolerance=RANDOM_ION_MZ_TOLERANCE,
                output_path=str(local_output / "preview_3.jpg"),
                structure_only=True,
                show=False,
            )
        else:
            logger.info("Data is not continuous, falling back to TIC image file_id=%s", file_id)
            plot_tic_image(
                dm,
                output_path=str(local_output / "preview.jpg"),
                cmap=random.choice(["viridis", "inferno", "hot"]),
            )

        _notify_callback(
            file_id=file_id,
            is_success=True,
            image_path=image_dest,
        )
        logger.info("Feature analysis completed: file_id=%s dest=%s", file_id, image_dest)
        return _fc_response(200, {"status": "ok", "file_id": file_id, "image_path": image_dest})

    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Feature analysis failed: file_id=%s", file_id)

        try:
            if local_output.exists():
                shutil.rmtree(local_output)
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
        if tmp_dir is not None:
            try:
                shutil.rmtree(tmp_dir)
                logger.info("Cleaned local cache: %s", tmp_dir)
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to clean tmp dir %s", tmp_dir)


# ======================================================================
# 随机离子选取
# ======================================================================

def _pick_random_mz(dm: MSDataManagerImzML) -> float:
    """
    从共享 m/z 轴上随机选一个 m/z 作为随机离子图的目标。

    continuous 数据在 load_head_data() 时已把第一条谱的 m/z 轴填入
    ms.shared_mz_list，这里直接复用，无需额外读盘。

    选取轴上真实存在的值（而不是区间内的任意浮点数），可保证
    mz_tolerance=0.1 的窗口内至少包含该点，不会得到全空的离子图。
    """
    mz_axis = dm.ms.shared_mz_list
    if mz_axis is None or len(mz_axis) == 0:
        raise ValueError("shared m/z list is empty; cannot pick a random ion")

    return float(mz_axis[random.randrange(len(mz_axis))])


# ======================================================================
# 本地缓存策略
# ======================================================================

def _pair_size(imzml_p: Path) -> int:
    """统计 imzML + ibd 总体积（OSSFS 上 stat 不产生下载）。"""
    total = imzml_p.stat().st_size if imzml_p.exists() else 0
    ibd_p = imzml_p.with_suffix(".ibd")
    if ibd_p.exists():
        total += ibd_p.stat().st_size
    return total


def _copy_with_retry(src: Path, dst: Path, attempts: int = 3) -> None:
    """OSSFS 偶发 IO 错误，简单重试兜底。"""
    last: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            shutil.copy2(src, dst)
            return
        except OSError as e:
            last = e
            logger.warning("copy %s failed (try %d/%d): %s", src.name, i, attempts, e)
    assert last is not None
    raise last


def _ensure_local(remote_imzml: Path) -> tuple[Path, Path | None]:
    """
    小文件 → 复制到 /tmp；大文件 → 维持流式。
    返回 (处理用的 imzML 路径, 临时目录或 None)。
    """
    total = _pair_size(remote_imzml)
    if total >= LOCAL_CACHE_THRESHOLD_BYTES:
        logger.info(
            "Pair size %d >= threshold %d, keep streaming from OSSFS",
            total, LOCAL_CACHE_THRESHOLD_BYTES,
        )
        return remote_imzml, None

    LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="imzml_local_", dir=LOCAL_CACHE_DIR))
    local_imzml = tmp_dir / remote_imzml.name
    local_ibd = tmp_dir / remote_imzml.with_suffix(".ibd").name

    logger.info("Caching imzML pair to %s (total=%d bytes)", tmp_dir, total)
    _copy_with_retry(remote_imzml, local_imzml)
    ibd_src = remote_imzml.with_suffix(".ibd")
    if ibd_src.exists():
        _copy_with_retry(ibd_src, local_ibd)

    return local_imzml, tmp_dir


# ======================================================================
# 内部 helpers
# ======================================================================

def _parse_event(event) -> dict:
    import base64

    if isinstance(event, dict) and "statusCode" not in event:
        if "body" not in event and "file_id" in event:
            return event

    try:
        event_json = json.loads(event) if isinstance(event, (str, bytes)) else event
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid event: not a valid JSON string")

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
            logger.error("Feature callback failed: status=%s body=%s", res.status_code, res.text)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Feature callback request failed")
