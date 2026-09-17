# -*- coding: utf-8 -*-
"""算法管线执行 + 回调通知。"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path

import requests

from models import ProcessingAlgorithmsConfig

from massflow.data_manager import MSDataManagerImzML
from massflow.preprocess import Preprocessor

logger = logging.getLogger()

# 环境变量
CALLBACK_URL = os.environ["ALGORITHM_FC_CALLBACK_URL"]
CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]

# NAS 挂载根目录。中间产物和最终 Zarr 先写到这里，避免通过 OSSFS
# 进行大量随机写和小对象写；每次运行结束后由 index.py 清理独立临时目录。
NAS_ROOT = Path("/home/app")
TEMP_ROOT = str(NAS_ROOT / "temp")

NUMBA_MAX_THREADS = 4

# 最终 Zarr 完整生成后，通过 OSS SDK 并发上传。
OSS_UPLOAD_WORKERS = int(os.environ.get("OSS_UPLOAD_WORKERS", "8"))
OSS_UPLOAD_SUBMIT_BATCH = 256
_OSS_THREAD_LOCAL = threading.local()


def run_algorithm_pipeline(
    run_id,
    imzml_path: str,
    config: ProcessingAlgorithmsConfig,
    oss_output_path: str,
    temp_dir: str,
    batch_size: int = 256,
    metadata: dict | None = None,
) -> str:
    """
    执行 MassFlow 算法管线，Zarr 先写入 NAS，再并行上传到 OSS。

    temp_dir 是本次运行在 NAS 上的独立目录，由调用方创建并在结束后清理。
    """
    nas_output_dir = str(Path(temp_dir) / "result.zarr")
    logger.info(
        "Pipeline started: run_id=%s output_mode=nas_then_oss "
        "nas_output_dir=%s oss_output_path=%s oss_upload_workers=%s "
        "os_cpu_count=%s numba_max_threads=%s numba_cache_dir=%s "
        "numba_cache_locator=%s",
        run_id,
        nas_output_dir,
        oss_output_path,
        OSS_UPLOAD_WORKERS,
        os.cpu_count(),
        NUMBA_MAX_THREADS,
        os.environ.get("NUMBA_CACHE_DIR", "unset"),
        os.environ.get("NUMBA_CACHE_LOCATOR_CLASSES", "unset"),
    )

    dm = MSDataManagerImzML(filepath=imzml_path)
    dm.load_head_data()

    # 根据输入数据类型和算法配置动态决定输出格式。
    is_processed = getattr(dm.ms.meta, "processed")
    is_continuous = getattr(dm.ms.meta, "continuous")

    task_list = []
    if config.peak_pick is not None:
        task_list.append("peak_pick")
    if config.peak_align is not None:
        task_list.append("peak_align")

    should_output_spectra = (
        is_processed
        and "peak_align" not in task_list
    ) or (
        is_continuous
        and "peak_pick" in task_list
        and "peak_align" not in task_list
    )
    output_format = "spectra_zarr" if should_output_spectra else "ion_image_zarr"

    enabled_algorithms = [
        name
        for name, algorithm_config in (
            ("baseline_correction", config.baseline_correction),
            ("noise_reduction", config.noise_reduction),
            ("normalization", config.normalization),
            ("peak_pick", config.peak_pick),
            ("peak_align", config.peak_align),
        )
        if algorithm_config is not None
    ]
    logger.info(
        "Pipeline configured: run_id=%s input_type=%s profile_spectrum=%s "
        "centroid_spectrum=%s output_format=%s algorithms=%s "
        "batch_size=%s spectrum_count=%s",
        run_id,
        "processed" if is_processed else "continuous",
        getattr(dm.ms.meta, "profile_spectrum", None),
        getattr(dm.ms.meta, "centroid_spectrum", None),
        output_format,
        enabled_algorithms,
        batch_size,
        len(dm.ms),
    )

    preprocessor = Preprocessor(
        dm,
        batch_size=batch_size,
        output_format=output_format,
        output_path=nas_output_dir,
        temp_dir=temp_dir,
        numba_max_threads=NUMBA_MAX_THREADS,
        compute_mean_spectrum=True,
        compute_tic=True,
    )

    if config.baseline_correction is not None:
        preprocessor = preprocessor.baseline_correction(
            **config.baseline_correction.model_dump()
        )
    if config.noise_reduction is not None:
        preprocessor = preprocessor.noise_reduction(
            **config.noise_reduction.model_dump()
        )
    if config.normalization is not None:
        preprocessor = preprocessor.normalization(
            **config.normalization.model_dump()
        )
    if config.peak_pick is not None:
        preprocessor = preprocessor.peak_pick(
            **config.peak_pick.model_dump()
        )
    if config.peak_align is not None:
        preprocessor = preprocessor.peak_align(
            **config.peak_align.model_dump()
        )

    result_dm = preprocessor.start()

    if metadata:
        result_dm.add_metadata(metadata)

    dm.close()
    result_dm.close()

    uploaded_files, uploaded_bytes = _upload_dir_to_oss(
        local_dir=nas_output_dir,
        oss_prefix=oss_output_path,
        max_workers=OSS_UPLOAD_WORKERS,
    )
    logger.info(
        "OSS upload completed: run_id=%s files=%s bytes=%s "
        "workers=%s oss_prefix=%s",
        run_id,
        uploaded_files,
        uploaded_bytes,
        OSS_UPLOAD_WORKERS,
        oss_output_path,
    )
    return oss_output_path


# ---------------------------------------------------------------------------
# OSS output operations
# ---------------------------------------------------------------------------

def _make_oss_bucket():
    """使用 FC 函数角色注入的 STS 临时凭证创建 OSS Bucket。"""
    import oss2

    auth = oss2.StsAuth(
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"],
    )
    return oss2.Bucket(
        auth,
        os.environ["OSS_ENDPOINT_URL"],
        os.environ["OSS_BUCKET_NAME"],
    )


def _get_thread_oss_bucket():
    """每个上传线程复用自己的 Bucket 和 HTTP 连接池。"""
    bucket = getattr(_OSS_THREAD_LOCAL, "bucket", None)
    if bucket is None:
        bucket = _make_oss_bucket()
        _OSS_THREAD_LOCAL.bucket = bucket
    return bucket


def _upload_dir_to_oss(
    local_dir: str,
    oss_prefix: str,
    max_workers: int,
) -> tuple[int, int]:
    """将本地目录中的文件按相对路径并行上传到 OSS。"""
    source_root = Path(local_dir)
    normalized_prefix = oss_prefix.strip("/")

    def upload_one(local_path: Path) -> int:
        relative_key = local_path.relative_to(source_root).as_posix()
        object_key = f"{normalized_prefix}/{relative_key}"
        _get_thread_oss_bucket().put_object_from_file(object_key, str(local_path))
        return local_path.stat().st_size

    uploaded_files = 0
    uploaded_bytes = 0
    file_iterator = (path for path in source_root.rglob("*") if path.is_file())

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            paths = list(islice(file_iterator, OSS_UPLOAD_SUBMIT_BATCH))
            if not paths:
                break
            for file_size in executor.map(upload_one, paths):
                uploaded_files += 1
                uploaded_bytes += file_size

    return uploaded_files, uploaded_bytes


def _dir_size_oss(oss_prefix: str) -> int:
    """通过 OSS SDK 列举对象计算输出目录总大小。"""
    import oss2

    bucket = _make_oss_bucket()
    total = 0
    prefix = oss_prefix.rstrip("/") + "/"
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        total += obj.size
    return total


def _delete_oss_prefix(oss_prefix: str) -> int:
    """删除指定 OSS 前缀下的对象，供上传失败后的清理使用。"""
    import oss2

    bucket = _make_oss_bucket()
    prefix = oss_prefix.rstrip("/") + "/"
    deleted_objects = 0
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        bucket.delete_object(obj.key)
        deleted_objects += 1
    return deleted_objects


def notify_callback(
    run_id: int,
    is_success: bool,
    out_path: str | None = None,
    output_size: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    """向主服务发送处理结果回调。"""
    body = {
        "run_id": run_id,
        "is_success": is_success,
        "out_path": out_path,
        "output_size": output_size,
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
            logger.error(
                "Callback failed: status=%s body=%s",
                res.status_code,
                res.text,
            )
    except Exception:  # pylint: disable=broad-except
        logger.exception("Callback request failed")
