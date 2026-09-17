# -*- coding: utf-8 -*-
"""
FC UMAP 入口：SDK 下载、空间预算、断点复用和结果增量回传。

handler：FC HTTP 触发器入口，输入 run_id/source_zarr_path；最终状态经回调端点通知后端。
failure_handler：部署为独立事件函数，并配置为主函数的最终 onFailure 目标。
主函数的部署 handler 应为 new_index.handler；本文件不会自动修改云端配置。

下载与上传均固定使用 8 个 worker。下载以 64 个对象为一批，完成后先持久化
ETag/大小/mtime 清单，再检查错误；单个对象经条件 GET、大小及 SDK CRC 校验后
原子替换目标文件。失败时保留已下载文件，成功回调确认后才清理工作目录。
断点复用依赖原工作目录仍存在；跨实例只能复用共享 NAS 上的副本。
同一个 run_id 的并发提交、最终回调的幂等及旧任务终态隔离由后端负责。

按本地盘和 NAS 的实际剩余空间、已验证的可复用文件与输出预算选择落盘位置。
FileTooLarge 按完整源大小判断，与下载排除无关；是否采样仅由实际 float32
矩阵大小决定：不超过 UMAP_FULL_MATRIX_MB（默认 1024 MiB）时全量，超过时
训练样本数不超过 UMAP_MAX_FIT_SAMPLES（默认 20000）、训练矩阵不超过
UMAP_SAMPLE_MATRIX_MB（默认 1024 MiB）。

OSS_DOWNLOAD_EXCLUDE_ARRAYS 仅允许 ion_image/intensity 数组：只跳过该数组
的数据 chunk，zarr 元数据与其余所有对象完整下载；禁止排除 spectra/intensity
等必需输入。

预算检查覆盖列举、单对象下载/上传进度及 UMAP 前后。它是协作式检查，无法
中断长时间的 native UMAP 调用；平台硬超时/OOM 仍依赖独立失败目标兜底。
Python 3.12 context.function.timeout 为秒；预算取它与 FC_TIMEOUT_SECONDS 的
较小值，从 handler 开始计时，不包含进入 handler 前的冷启动，需保留余量。

环境变量：
    CLUSTERING_FC_CALLBACK_URL / CALLBACK_TOKEN    主服务回调 URL / Token
    OSS_BUCKET_NAME / OSS_ENDPOINT_URL            OSS bucket / endpoint
    ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET /
    ALIBABA_CLOUD_SECURITY_TOKEN                  FC 注入的临时 STS 凭证
    FC_MAX_FILE_SIZE_MB                           完整源大小限制，默认 20480 MiB
    NAS_STORAGE_PATH                             默认 /home/app
    OSS_DOWNLOAD_EXCLUDE_ARRAYS                  默认 ion_image/intensity；空串为完整下载
    FC_OUTPUT_BUDGET_MB / FC_DISK_HEADROOM_MB      输出预算 / 预留空间，各默认 512 MiB
    FC_TIMEOUT_SECONDS                           默认 900 秒，不覆盖平台超时设置
    FC_UPLOAD_RESERVE_SECONDS                    下载/计算阶段的回传余量，默认 120 秒
    UMAP_FULL_MATRIX_MB                          全量矩阵上限，默认 1024 MiB
    UMAP_SAMPLE_MATRIX_MB / UMAP_MAX_FIT_SAMPLES  抽样上限，默认 1024 MiB / 20000 像素
"""

import errno
import json
import logging
import math
import os
import shutil
import stat
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path

import oss2
import requests

logger = logging.getLogger()

CALLBACK_URL = os.environ["CLUSTERING_FC_CALLBACK_URL"]
CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]
OSS_BUCKET_NAME = os.environ["OSS_BUCKET_NAME"]
OSS_ENDPOINT_URL = os.environ["OSS_ENDPOINT_URL"]

# 超过此阈值的源 zarr FC 不处理，直接跳过
FC_MAX_FILE_SIZE_MB = int(os.environ.get("FC_MAX_FILE_SIZE_MB", "20480"))
# 本地盘空间不足时的 NAS 挂载路径
NAS_STORAGE_PATH = os.environ.get("NAS_STORAGE_PATH", "/home/app")
# 仅按实际 float32 矩阵大小决定是否采样，预算不是进程峰值上限。
UMAP_FULL_MATRIX_MB = int(os.environ.get("UMAP_FULL_MATRIX_MB", "1024"))
UMAP_SAMPLE_MATRIX_MB = int(os.environ.get("UMAP_SAMPLE_MATRIX_MB", "1024"))
UMAP_MAX_FIT_SAMPLES = int(os.environ.get("UMAP_MAX_FIT_SAMPLES", "20000"))
# 下载与回传都固定使用 8 个 worker。
OSS_UPLOAD_MAX_WORKERS = 8
# 每批提交给回传线程池的文件数，避免一次性排队过多任务
OSS_UPLOAD_SUBMIT_BATCH = 500
# 下载源 zarr 的并发线程数（有界，避免一次性打满连接或内存）
OSS_DOWNLOAD_MAX_WORKERS = 8
# 每批提交给下载线程池的对象数，避免一次性排队过多任务
OSS_DOWNLOAD_SUBMIT_BATCH = 64
# 下载时跳过数据 chunk 的数组（zarr 根内相对路径）。
# ion_image/intensity 是与 spectra/intensity 相同数据的 ion-major 副本：UMAP 流程
# 只读 spectra 视图与 axes/mz、axes/coordinates，但
# reader 打开时的布局校验要求 ion_image 元数据完整、并读 ion_image/offsets 首尾值
# （massflow/msi_zarr/reader.py），因此只跳过该数组的 chunk，元数据始终下载。
# 置空环境变量可恢复完整复制。
OSS_DOWNLOAD_EXCLUDE_ARRAYS = frozenset(
    p.strip().strip("/")
    for p in os.environ.get("OSS_DOWNLOAD_EXCLUDE_ARRAYS", "ion_image/intensity").split(",")
    if p.strip()
)
_UNUSED_ARRAYS = frozenset({"ion_image/intensity"})
_ZARR_METADATA_FILES = frozenset({"zarr.json", ".zarray", ".zattrs", ".zgroup", ".zmetadata"})
# 容量决策：缺失下载大小 + 输出/临时预算 + 预留空间 <= 对应磁盘可用空间
FC_OUTPUT_BUDGET_MB = int(os.environ.get("FC_OUTPUT_BUDGET_MB", "512"))
FC_DISK_HEADROOM_MB = int(os.environ.get("FC_DISK_HEADROOM_MB", "512"))
# 时间预算：需与 s.yaml 的 timeout 一致；为回传与回调预留 FC_UPLOAD_RESERVE_SECONDS
FC_TIMEOUT_SECONDS = int(os.environ.get("FC_TIMEOUT_SECONDS", "900"))
FC_UPLOAD_RESERVE_SECONDS = int(os.environ.get("FC_UPLOAD_RESERVE_SECONDS", "120"))
# 上传阶段可以使用上述预留，但仍留出回调时间；SDK 超时约束单次网络停顿。
FC_CALLBACK_RESERVE_SECONDS = 45
OSS_REQUEST_TIMEOUT_SECONDS = 15

# 下载清单文件名（位于 work_root 下、zarr 目录之外），记录每个对象的 ETag/大小/mtime
_DOWNLOAD_MANIFEST = ".fc_download_manifest.json"

# FC 本地临时盘挂载点
LOCAL_DISK_ROOT = "/tmp"

_thread_local = threading.local()


class TimeBudgetExhausted(RuntimeError):
    """协作式时间预算耗尽，提前退出并尝试回调终态。"""


def handler(event, context):
    """FC HTTP 触发器入口"""
    run_started = time.monotonic()
    deadline = run_started + min(FC_TIMEOUT_SECONDS, int(context.function.timeout))

    body = _parse_event(event)
    if isinstance(body, dict) and "statusCode" in body:
        return body

    run_id = body.get("run_id")
    source_zarr_path = body.get("source_zarr_path")    # OSS 相对路径（也是 OSS key 前缀），算法 run 的 out_path

    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        return _fc_response(400, "run_id must be a positive integer")
    if not isinstance(source_zarr_path, str):
        return _fc_response(400, "source_zarr_path must be a relative Zarr prefix")
    oss_prefix = source_zarr_path.rstrip("/")
    if (
        not oss_prefix.endswith(".zarr")
        or any(part in {"", ".", ".."} for part in oss_prefix.split("/"))
        or "\\" in oss_prefix or "\x00" in oss_prefix
    ):
        return _fc_response(400, "Invalid source_zarr_path")
    logger.info("fc-clustering: run_id=%s, source=%s", run_id, oss_prefix)

    dm = None
    work_root = None  # 按 run_id 隔离的工作目录（/tmp/ 或 NAS 下），失败时保留供重试复用
    completed = False
    try:
        # 列举完整源以检查大小限制，随后按实际下载量与剩余空间选择存储。
        objects = _list_source_objects(oss_prefix, deadline)
        if not objects:
            raise FileNotFoundError(f"Source zarr not found: no objects under oss://{OSS_BUCKET_NAME}/{oss_prefix}")
        source_size = sum(o["size"] for o in objects)
        source_size_mb = source_size / 1024 / 1024
        logger.info(
            "source zarr: %d objects, %.2fMB -> oss://%s/%s",
            len(objects), source_size_mb, OSS_BUCKET_NAME, oss_prefix,
        )

        # 数组 chunk 级下载排除：FileTooLarge 仍按完整源大小判断。
        download_objects, skipped_files, skipped_bytes = _split_download_objects(objects, oss_prefix)
        download_size = sum(o["size"] for o in download_objects)
        if skipped_files:
            logger.info(
                "download exclusion: skip %d chunk files %.2fMB of arrays [%s], %d objects %.2fMB to download",
                skipped_files, skipped_bytes / 1024 / 1024,
                ",".join(sorted(OSS_DOWNLOAD_EXCLUDE_ARRAYS)),
                len(download_objects), download_size / 1024 / 1024,
            )

        if source_size_mb > FC_MAX_FILE_SIZE_MB:
            logger.warning(
                "source zarr size %.2fMB exceeds max limit %dMB, skipping: oss://%s/%s",
                source_size_mb, FC_MAX_FILE_SIZE_MB, OSS_BUCKET_NAME, oss_prefix,
            )
            delivered = _notify_callback(
                run_id=run_id,
                is_success=False,
                error_type="FileTooLarge",
                error_message=f"source zarr size {source_size_mb:.2f}MB exceeds max limit {FC_MAX_FILE_SIZE_MB}MB",
            )
            return _fc_response(200 if delivered else 502, {
                "status": "skipped" if delivered else "callback_failed",
                "run_id": run_id, "reason": "FileTooLarge",
            })

        # 按实际可用空间选择本地盘或 NAS（按实际下载大小，而非完整源大小）
        work_root = _choose_work_root(run_id, download_objects, oss_prefix, deadline)
        local_source = f"{work_root}/{Path(oss_prefix).name}"

        # SDK 并发下载（带断点复用），代替 shutil.copytree
        _ensure_time_budget(deadline, "before download")
        _download_objects(download_objects, local_source, oss_prefix, deadline)

        # 下载后对本地副本做快照，作为增量回传的基线
        _ensure_time_budget(deadline, "before umap imports")
        before_snapshot = _snapshot_dir(local_source)

        from massflow.data_manager import MSDataManagerZarr
        from massflow.segmentation import plot_umap_image

        # 按后端.md 用法：预处理输出的 zarr 用 MSDataManagerZarr 打开
        dm = MSDataManagerZarr(filepath=local_source)
        dm.load_head_data()
        sample_ratio = _select_sample_ratio(dm)
        _ensure_time_budget(deadline, "before umap")

        # 执行 UMAP 分析，结果写回源 zarr 内部
        plot_umap_image(
            dm,
            save_matrix="zarr",       # 必须 "zarr" 才把结果写回源 zarr
            sample_ratio=sample_ratio,
            transform_batch_size=1024,
            output_path=str(Path(work_root) / "umap_image.jpg"),
        )

        # 关闭 dm，确保写盘完成后再回传
        _close_dm(dm)
        dm = None

        # 增量回传：只上传新增/内容变化的文件（UMAP 为追加写，通常只有新增的 group）。
        # 上传可以使用为它预留的时间；网络进度检查仍为回调留出预算。
        _ensure_time_budget(deadline, "before upload", FC_CALLBACK_RESERVE_SECONDS)
        after_snapshot = _snapshot_dir(local_source)
        changed_files = _diff_snapshot(before_snapshot, after_snapshot)
        logger.info("zarr changed files: %d (total %d)", len(changed_files), len(after_snapshot))

        if changed_files:
            uploaded_files, uploaded_bytes = _upload_files_to_oss(
                local_source, changed_files, oss_prefix, deadline,
            )
            logger.info(
                "incremental upload done: %d files %.2fMB -> oss://%s/%s",
                uploaded_files, uploaded_bytes / 1024 / 1024, OSS_BUCKET_NAME, source_zarr_path,
            )
        else:
            logger.info("no changed files, skip upload")

        completed = _notify_callback(
            run_id=run_id,
            is_success=True,
        )
        if not completed:
            # 数组已经上传；回调失败不能再发送相反的 is_success=False。
            return _fc_response(502, {"status": "callback_failed", "run_id": run_id})

        logger.info("umap analysis completed: run_id=%s", run_id)
        return _fc_response(200, {"status": "ok", "run_id": run_id})

    except Exception as exc:
        logger.exception("umap analysis failed: run_id=%s", run_id)

        _notify_callback(
            run_id=run_id,
            is_success=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        return _fc_response(500, {"status": "error", "run_id": run_id, "error": str(exc)})

    finally:
        if dm is not None:
            try:
                _close_dm(dm)
            except Exception:
                logger.exception("failed to close data manager: run_id=%s", run_id)
        # 成功才清理工作目录；失败时保留，供下次重试按下载清单断点复用
        if work_root and os.path.exists(work_root):
            if completed:
                shutil.rmtree(work_root, ignore_errors=True)
                logger.info("cleaned up work dir: %s", work_root)
            else:
                logger.info("keeping work dir for retry reuse: %s", work_root)


def failure_handler(event, context):
    """独立事件函数入口：配置为主函数的最终 onFailure 目标，不能指回主 handler。

    接收 FC 原生 destination payload，不是 EventBridge 包装；后端按 run_id
    幂等处理终态。配置此入口的目标函数需有相同回调环境变量和依赖层。
    """
    record = json.loads(event) if isinstance(event, (str, bytes)) else event
    if not isinstance(record, dict) or "requestPayload" not in record:
        raise ValueError("Expected a Function Compute destination payload")
    body = _parse_event(record["requestPayload"])
    if "statusCode" in body:
        raise ValueError(f"Invalid original invocation: {body['body']}")
    run_id = body.get("run_id")
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("Original invocation has no valid run_id")
    request_context = record.get("requestContext", {})
    error_type = request_context.get("condition") or "FunctionFailed"
    message = json.dumps(record.get("responsePayload"), ensure_ascii=False)[:2000]
    if not _notify_callback(run_id, False, error_type, message):
        # 目标函数执行失败，交给平台重试该通知；这里不能吞掉异常并返回成功。
        raise RuntimeError(f"Failed to deliver final failure for run_id={run_id}")
    return {"status": "notified", "run_id": run_id}


# ---------------------------------------------------------------------------
# 源数据列举、工作目录选择与并发下载（P0#1 / P0#2 / P1#3 / P0#4）
# ---------------------------------------------------------------------------

def _list_source_objects(oss_prefix: str, deadline: float) -> list[dict]:
    """按页列举对象并校验本地映射；元数据内存随对象数增长，不读取对象内容。"""
    bucket = _get_thread_oss_bucket()
    objects = []
    for obj in oss2.ObjectIteratorV2(bucket, prefix=f"{oss_prefix}/", max_keys=1000):
        _ensure_time_budget(deadline, "listing source")
        if obj.key.endswith("/"):
            continue
        rel = obj.key[len(oss_prefix) + 1:]
        if (
            not obj.key.startswith(f"{oss_prefix}/")
            or any(part in {"", ".", ".."} for part in rel.split("/"))
            or "\\" in rel or "\x00" in rel
        ):
            raise ValueError(f"Object key cannot be mapped to a local file: {obj.key!r}")
        objects.append({"key": obj.key, "size": obj.size, "etag": obj.etag.strip('"')})
    return objects


def _split_download_objects(objects: list[dict], oss_prefix: str) -> tuple[list[dict], int, int]:
    """只排除已核实不用读取的数组数据，保留 Zarr v2/v3 元数据。"""
    unsupported = OSS_DOWNLOAD_EXCLUDE_ARRAYS - _UNUSED_ARRAYS
    if unsupported:
        raise ValueError(f"Arrays are not verified safe to exclude: {sorted(unsupported)}")
    keys = {obj["key"] for obj in objects}
    has_spectra = any(
        f"{oss_prefix}/spectra/intensity/{name}" in keys
        for name in ("zarr.json", ".zarray")
    )
    if not OSS_DOWNLOAD_EXCLUDE_ARRAYS or not has_spectra:
        return objects, 0, 0
    kept = []
    skipped_files = skipped_bytes = 0
    for obj in objects:
        rel = obj["key"][len(oss_prefix) + 1:]
        excluded = (
            Path(rel).name not in _ZARR_METADATA_FILES
            and any(rel.startswith(f"{array}/") for array in OSS_DOWNLOAD_EXCLUDE_ARRAYS)
        )
        if excluded:
            skipped_files += 1
            skipped_bytes += obj["size"]
        else:
            kept.append(obj)
    return kept, skipped_files, skipped_bytes


def _choose_work_root(run_id: int, objects: list[dict], oss_prefix: str, deadline: float) -> str:
    """分别核算本地/NAS 的缺失下载字节；已校验的复用文件不重复计入。"""
    reserve = (FC_OUTPUT_BUDGET_MB + FC_DISK_HEADROOM_MB) * 1024 * 1024
    total_size = sum(obj["size"] for obj in objects)
    for disk_root in (LOCAL_DISK_ROOT, NAS_STORAGE_PATH):
        _ensure_time_budget(deadline, "selecting work directory")
        os.makedirs(disk_root, exist_ok=True)
        work_root = Path(disk_root) / f"fc-clustering-{run_id}"
        source_root = work_root / Path(oss_prefix).name
        manifest = _load_manifest(work_root / _DOWNLOAD_MANIFEST)
        reused_bytes = 0
        for obj in objects:
            if _can_reuse(source_root, obj, manifest.get(obj["key"]), oss_prefix):
                reused_bytes += obj["size"]
        # 未复用的旧文件仍占用磁盘；新下载先写临时文件，因此不把旧文件算作可用空间。
        required = total_size - reused_bytes + reserve
        free = shutil.disk_usage(disk_root).free
        logger.info(
            "disk=%s free=%.2fMiB need=%.2fMiB reusable=%.2fMiB",
            disk_root, free / 2**20, required / 2**20, reused_bytes / 2**20,
        )
        if free >= required:
            work_root.mkdir(parents=True, exist_ok=True)
            return str(work_root)
    raise OSError(errno.ENOSPC, "Neither local disk nor NAS has enough free space")


def _ensure_time_budget(
    deadline: float, stage: str, reserve_seconds: int = FC_UPLOAD_RESERVE_SECONDS,
) -> None:
    """协作式预算检查；不能中断长时间 native UMAP 调用或保证硬超时回调。"""
    remaining = deadline - time.monotonic()
    if remaining <= reserve_seconds:
        raise TimeBudgetExhausted(
            f"time budget exhausted at '{stage}': {remaining:.1f}s remaining, "
            f"{reserve_seconds}s reserved"
        )


def _can_reuse(source_root: Path, obj: dict, entry: dict | None, oss_prefix: str) -> bool:
    if not isinstance(entry, dict):
        return False
    if entry.get("etag") != obj["etag"] or entry.get("size") != obj["size"]:
        return False
    path = source_root / obj["key"][len(oss_prefix) + 1:]
    try:
        st = path.stat()
    except FileNotFoundError:
        return False
    return stat.S_ISREG(st.st_mode) and st.st_size == obj["size"] and st.st_mtime_ns == entry.get("mtime_ns")


def _select_sample_ratio(dm) -> float:
    """使用 load_head_data 已加载的共享 m/z 轴，不加载完整强度矩阵。"""
    if dm.storage_mode != "continuous":
        raise ValueError("UMAP requires continuous spectra with a shared m/z axis")
    n_pixels = len(dm.ms)
    n_features = len(dm.ms.shared_mz_list)
    if n_pixels < 5 or n_features == 0:
        raise ValueError("3-D UMAP requires at least 5 spectra and a non-empty m/z axis")
    row_bytes = 4 * n_features
    if n_pixels * row_bytes <= UMAP_FULL_MATRIX_MB * 2**20:
        sample_count = n_pixels
    else:
        sample_limit = min(UMAP_MAX_FIT_SAMPLES, UMAP_SAMPLE_MATRIX_MB * 2**20 // row_bytes)
        if sample_limit < 5:
            raise ValueError("UMAP sample memory budget cannot fit 5 spectra")
        sample_count = min(n_pixels, sample_limit)
    ratio = sample_count / n_pixels
    # MassFlow 用 ceil(N * ratio) 计算样本数；避免浮点舍入使预算多出一行。
    if sample_count < n_pixels and math.ceil(n_pixels * ratio) > sample_count:
        ratio = math.nextafter(ratio, 0.0)
    logger.info(
        "UMAP pixels=%d features=%d full=%.2fMiB samples=%d ratio=%.6f sample=%.2fMiB",
        n_pixels, n_features, n_pixels * row_bytes / 2**20,
        sample_count, ratio, sample_count * row_bytes / 2**20,
    )
    return ratio


def _download_objects(objects: list[dict], local_source: str, oss_prefix: str, deadline: float) -> None:
    """8 worker 下载；每批及异常退出时保存已完成对象，部分下载不替换完整文件。"""
    source_root = Path(local_source)
    source_root.mkdir(parents=True, exist_ok=True)
    manifest_path = source_root.parent / _DOWNLOAD_MANIFEST
    manifest = _load_manifest(manifest_path)
    source_keys = {obj["key"] for obj in objects}
    pending = [
        obj for obj in objects
        if not _can_reuse(source_root, obj, manifest.get(obj["key"]), oss_prefix)
    ]
    manifest = {key: entry for key, entry in manifest.items() if key in source_keys}
    _prune_extra_files(source_root, source_keys, oss_prefix)
    # 临时文件位于 Zarr 之外，避免与合法对象名冲突；上次硬超时的残留不参与复用。
    partial_root = source_root.parent / ".download_parts"
    shutil.rmtree(partial_root, ignore_errors=True)
    partial_root.mkdir()
    logger.info("download reuse: %d/%d objects", len(objects) - len(pending), len(objects))

    stop_batch = threading.Event()

    def check_progress(consumed_bytes, total_bytes):
        if stop_batch.is_set():
            raise RuntimeError("Download batch cancelled after an object failed")
        _ensure_time_budget(deadline, "downloading object")

    def download_one(obj: dict) -> tuple[str, dict]:
        check_progress(0, obj["size"])
        dst = source_root / obj["key"][len(oss_prefix) + 1:]
        dst.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=partial_root)
        os.close(fd)
        tmp = Path(temporary)
        try:
            result = _get_thread_oss_bucket().get_object_to_file(
                obj["key"], str(tmp),
                headers={"If-Match": f'"{obj["etag"]}"'},
                progress_callback=check_progress,
            )
            st = tmp.stat()
            if st.st_size != obj["size"] or result.etag != obj["etag"]:
                raise IOError(f"Source changed during download: {obj['key']}")
            os.replace(tmp, dst)
            return obj["key"], {"etag": obj["etag"], "size": st.st_size, "mtime_ns": st.st_mtime_ns}
        finally:
            tmp.unlink(missing_ok=True)

    total_bytes = sum(obj["size"] for obj in pending)
    done_bytes = done_count = 0
    file_iterator = iter(pending)
    with ThreadPoolExecutor(max_workers=OSS_DOWNLOAD_MAX_WORKERS) as executor:
        while True:
            _ensure_time_budget(deadline, "before download batch")
            batch = list(islice(file_iterator, OSS_DOWNLOAD_SUBMIT_BATCH))
            if not batch:
                break
            futures = [executor.submit(download_one, obj) for obj in batch]
            first_error = None
            for future in as_completed(futures):
                try:
                    key, entry = future.result()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                        stop_batch.set()
                else:
                    manifest[key] = entry
                    done_count += 1
                    done_bytes += entry["size"]
            # 在预算检查/错误抛出之前持久化本批已完成文件，最多损失当前一批的复用信息。
            _save_manifest(manifest_path, manifest)
            if first_error is not None:
                raise first_error
            logger.info(
                "download progress: %d/%d objects, %.2f/%.2fMiB",
                done_count, len(pending), done_bytes / 2**20, total_bytes / 2**20,
            )
    _save_manifest(manifest_path, manifest)
    partial_root.rmdir()


def _prune_extra_files(source_root: Path, source_keys: set[str], oss_prefix: str) -> None:
    """清理本地副本中不属于源对象集合的残留文件（如上次失败前 UMAP 写出的结果）"""
    for path in source_root.rglob("*"):
        if not path.is_file():
            continue
        key = f"{oss_prefix}/{path.relative_to(source_root).as_posix()}"
        if key not in source_keys:
            path.unlink()


def _load_manifest(manifest_path: Path) -> dict:
    """读取本 bucket 的清单；损坏的外部文件不能用于跳过下载。"""
    try:
        with manifest_path.open(encoding="utf-8") as stream:
            data = json.load(stream)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        logger.warning("download manifest damaged: %s", manifest_path)
        return {}
    if not isinstance(data, dict) or data.get("bucket") != OSS_BUCKET_NAME:
        return {}
    objects = data.get("objects")
    return objects if isinstance(objects, dict) else {}


def _save_manifest(manifest_path: Path, manifest: dict) -> None:
    """原子保存已验证完成的下载；旧清单在替换完成前保持有效。"""
    tmp_path = manifest_path.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as stream:
        json.dump({"bucket": OSS_BUCKET_NAME, "objects": manifest}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp_path, manifest_path)


# ---------------------------------------------------------------------------
# 事件解析、回调、快照与增量回传
# ---------------------------------------------------------------------------

def _parse_event(event) -> dict:
    """解析 HTTP 包装或直接 JSON 请求；在输入边界返回 400。"""
    import base64

    try:
        event_json = json.loads(event) if isinstance(event, (str, bytes)) else event
        if not isinstance(event_json, dict):
            return _fc_response(400, "Invalid event: expected a JSON object")
        if "body" not in event_json:
            if "run_id" in event_json:
                return event_json
            return _fc_response(400, "Invalid event: missing body")
        body = event_json["body"]
        if event_json.get("isBase64Encoded"):
            body = base64.b64decode(body, validate=True).decode("utf-8")
        if isinstance(body, (str, bytes)):
            body = json.loads(body)
    except (ValueError, TypeError):
        return _fc_response(400, "Invalid event/body encoding or JSON")
    if not isinstance(body, dict):
        return _fc_response(400, "Invalid body: expected a JSON object")
    return body


def _fc_response(status_code: int, body) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": json.dumps(body, ensure_ascii=False) if not isinstance(body, str) else body,
    }


def _notify_callback(
    run_id: int,
    is_success: bool,
    error_type: str | None = None,
    error_message: str | None = None,
) -> bool:
    """回调主服务 clustering_callback 端点（只携带状态，不带路径/大小）"""
    body = {
        "run_id": run_id,
        "is_success": is_success,
        "error_type": error_type,
        "error_message": error_message,
    }
    for attempt in range(2):
        try:
            res = requests.post(
                CALLBACK_URL,
                json=body,
                headers={"Content-Type": "application/json", "X-FC-Token": CALLBACK_TOKEN},
                timeout=(3, 10),
            )
            status = res.status_code
            res.close()
            if status == 200:
                return True
            logger.error("clustering callback failed: attempt=%d status=%s", attempt + 1, status)
        except requests.RequestException:
            logger.exception("clustering callback request failed: attempt=%d", attempt + 1)
    return False


def _close_dm(dm) -> None:
    """关闭 MSDataManagerZarr 的 reader/writer。"""
    dm.close()


def _snapshot_dir(root: str) -> dict:
    """记录目录下所有文件的 {相对路径: (大小, mtime_ns)}，作为增量回传基线。

    在本地副本（/tmp 或 NAS）上执行，mtime 可靠（下载与 zarr 新写/改写
    的文件 mtime 均为本地写入时间，且复用文件在下载时已校验 mtime 未变）。
    """
    snapshot = {}
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            fp = os.path.join(dirpath, name)
            st = os.stat(fp)
            snapshot[os.path.relpath(fp, root)] = (st.st_size, st.st_mtime_ns)
    return snapshot


def _diff_snapshot(before: dict, after: dict) -> list[str]:
    """求出新增或 (大小, mtime) 变化的相对路径。

    同路径被改写的元数据文件（如 .zattrs）也会被捕获；UMAP 为追加写，无需处理删除。
    """
    return sorted(rel for rel, meta in after.items() if before.get(rel) != meta)


def _get_thread_oss_bucket() -> oss2.Bucket:
    """按线程创建并复用 OSS Bucket 实例。

    使用 FC 函数角色注入的 STS 临时凭证（ALIBABA_CLOUD_* 环境变量，由运行时自动注入）。
    """
    credentials = (
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"],
    )
    bucket = getattr(_thread_local, "oss_bucket", None)
    if bucket is None or getattr(_thread_local, "oss_credentials", None) != credentials:
        auth = oss2.StsAuth(*credentials)
        bucket = oss2.Bucket(
            auth, OSS_ENDPOINT_URL, OSS_BUCKET_NAME,
            connect_timeout=OSS_REQUEST_TIMEOUT_SECONDS,
        )
        _thread_local.oss_bucket = bucket
        _thread_local.oss_credentials = credentials
    return bucket


def _upload_files_to_oss(
    local_root: str,
    rel_paths: list[str],
    oss_prefix: str,
    deadline: float,
) -> tuple[int, int]:
    """将本地目录中指定的文件按相对路径并行上传到 OSS（直连 SDK，绕过 OSSFS 挂载层）。"""
    source_root = Path(local_root)
    normalized_prefix = oss_prefix.strip("/")

    def check_progress(consumed_bytes, total_bytes):
        _ensure_time_budget(deadline, "uploading object", FC_CALLBACK_RESERVE_SECONDS)

    def upload_one(rel_path: str) -> int:
        _ensure_time_budget(deadline, "before uploading object", FC_CALLBACK_RESERVE_SECONDS)
        local_path = source_root / rel_path
        object_key = f"{normalized_prefix}/{Path(rel_path).as_posix()}"
        _get_thread_oss_bucket().put_object_from_file(
            object_key, str(local_path), progress_callback=check_progress,
        )
        return local_path.stat().st_size

    uploaded_files = 0
    uploaded_bytes = 0
    file_iterator = iter(rel_paths)

    with ThreadPoolExecutor(max_workers=OSS_UPLOAD_MAX_WORKERS) as executor:
        while True:
            batch = list(islice(file_iterator, OSS_UPLOAD_SUBMIT_BATCH))
            if not batch:
                break
            for file_size in executor.map(upload_one, batch):
                uploaded_files += 1
                uploaded_bytes += file_size

    return uploaded_files, uploaded_bytes
