# -*- coding: utf-8 -*-
"""阿里云 FC：imzML 转 Zarr，使用 oss2 SDK 并发发布输出。

输入仍通过 OSSFS 读取，转换及 NAS staging 沿用现有 MassFlow layer。
输出直接写 OSS：默认 8 个 worker、最多 2 倍 worker 数的待处理任务、
每线程复用独立 HTTP 连接，SDK 校验 CRC64；网络/限流/服务端错误及 CRC 不一致最多尝试 3 次。
output_path 必须是 bucket 内的相对 .zarr 路径；成功回调后下游才能读取。
同一 output_path 必须由后端串行提交，目录级发布不具备原子性。

部署入口固定为 index.handler；依赖由 MassFlow 和 OSS2 layer 提供。
OSS2 layer 配置与 code/ 一致，使用该 layer 的版本 2（不是 OSS SDK V2）。
Python 3.12 使用 ALIBABA_CLOUD_* 环境变量提供的 STS 凭证。
环境变量：OSS_BUCKET_NAME、OSS_ENDPOINT_URL（沿用现有配置）。
PUBLISH_MAX_WORKERS 默认 8；OSS_CONNECT_TIMEOUT_SECONDS 默认 5，
OSS_READWRITE_TIMEOUT_SECONDS 默认 30（连接/读写等待超时，不是总上传时限）。
"""

import json
import logging
import os
import shutil
import threading
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack, contextmanager
from pathlib import Path
from queue import SimpleQueue
from time import perf_counter, sleep

import oss2
import requests
from massflow.data_manager import MSDataManagerImzML
from massflow.preprocess import Preprocessor

logger = logging.getLogger()

STORAGE_ROOT = Path(os.getenv("STORAGE_ROOT", "/home/storage"))
NAS_ROOT = Path("/home/app")
TEMP_ROOT = NAS_ROOT / "temp"
OUTPUT_STAGING_ROOT = NAS_ROOT / "output_staging"

CALLBACK_URL = os.environ["RAW_CONVERT_FC_CALLBACK_URL"]
CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]
PUBLISH_MAX_WORKERS = int(os.getenv("PUBLISH_MAX_WORKERS", "8"))
OSS_BUCKET_NAME = os.environ["OSS_BUCKET_NAME"]
OSS_ENDPOINT_URL = os.environ["OSS_ENDPOINT_URL"]
OSS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("OSS_CONNECT_TIMEOUT_SECONDS", "5"))
OSS_READWRITE_TIMEOUT_SECONDS = float(os.getenv("OSS_READWRITE_TIMEOUT_SECONDS", "30"))

if PUBLISH_MAX_WORKERS < 1:
    raise ValueError("PUBLISH_MAX_WORKERS must be greater than 0")


def handler(event, context):
    """处理 FC HTTP 触发事件。"""
    handler_started = perf_counter()
    body = _parse_event(event)
    if isinstance(body, dict) and "statusCode" in body:
        return body

    run_id = body.get("run_id")
    imzml_path = body.get("imzml_path")
    ibd_path = body.get("ibd_path")
    output_path = body.get("output_path")
    metadata = body.get("metadata", {})

    if not all([run_id, imzml_path, ibd_path, output_path]):
        return _fc_response(400, "Missing required fields")

    local_imzml = STORAGE_ROOT / imzml_path
    local_ibd = STORAGE_ROOT / ibd_path
    try:
        output_prefix = _output_prefix(output_path)
    except ValueError as exc:
        return _fc_response(400, str(exc))

    attempt_id = uuid.uuid4().hex[:8]
    run_temp_dir = TEMP_ROOT / f"{run_id}_{attempt_id}"
    staging_output = OUTPUT_STAGING_ROOT / f"{run_id}_{attempt_id}.zarr"

    logger.info(
        "raw-convert started: run_id=%s attempt_id=%s",
        run_id,
        attempt_id,
    )

    dm = None
    result_dm = None
    success = False
    output_format = None
    output_size = None
    output_file_count = None
    load_head_seconds = 0.0
    conversion_seconds = 0.0
    publish_seconds = 0.0
    publish_mib_per_second = 0.0

    try:
        if not local_imzml.exists():
            raise FileNotFoundError(f"imzML not found: {local_imzml}")
        if not local_ibd.exists():
            raise FileNotFoundError(f"ibd not found: {local_ibd}")

        staging_output.parent.mkdir(parents=True, exist_ok=True)

        load_head_started = perf_counter()
        dm = MSDataManagerImzML(filepath=str(local_imzml))
        dm.load_head_data()
        load_head_seconds = perf_counter() - load_head_started

        is_processed = getattr(dm.ms.meta, "processed")
        is_continuous = getattr(dm.ms.meta, "continuous")
        output_format = "ion_image_zarr" if is_continuous is True else "spectra_zarr"
        logger.info(
            "raw-convert input loaded: run_id=%s attempt_id=%s "
            "is_processed=%s is_continuous=%s output_format=%s "
            "duration_seconds=%.3f",
            run_id,
            attempt_id,
            is_processed,
            is_continuous,
            output_format,
            load_head_seconds,
        )

        conversion_started = perf_counter()
        result_dm = Preprocessor(
            dm,
            output_format=output_format,
            output_path=str(staging_output),
            temp_dir=str(run_temp_dir),
            compute_mean_spectrum=True,
            compute_tic=True,
        ).start()

        if metadata:
            result_dm.add_metadata(metadata)

        result_dm.close()
        result_dm = None
        conversion_seconds = perf_counter() - conversion_started

        output_size, output_file_count = _directory_stats(staging_output)
        logger.info(
            "raw-convert conversion completed: run_id=%s attempt_id=%s "
            "output_format=%s duration_seconds=%.3f size=%d file_count=%d",
            run_id,
            attempt_id,
            output_format,
            conversion_seconds,
            output_size,
            output_file_count,
        )

        publish_started = perf_counter()
        published_file_count = _publish_directory(
            staging_output,
            output_prefix,
            max_workers=PUBLISH_MAX_WORKERS,
        )
        publish_seconds = perf_counter() - publish_started
        publish_mib_per_second = output_size / (1024 * 1024) / publish_seconds
        logger.info(
            "raw-convert publish completed: run_id=%s attempt_id=%s "
            "duration_seconds=%.3f size=%d file_count=%d max_workers=%d "
            "throughput_mib_per_second=%.3f",
            run_id,
            attempt_id,
            publish_seconds,
            output_size,
            published_file_count,
            PUBLISH_MAX_WORKERS,
            publish_mib_per_second,
        )

        _notify_callback(
            run_id=run_id,
            is_success=True,
            out_path=output_path,
            output_size=output_size,
        )
        success = True
        response = _fc_response(200, {"status": "ok", "run_id": run_id})

    except Exception as exc:
        logger.exception(
            "raw-convert failed: run_id=%s attempt_id=%s",
            run_id,
            attempt_id,
        )
        _notify_callback(
            run_id=run_id,
            is_success=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        response = _fc_response(
            500,
            {"status": "error", "run_id": run_id, "error": str(exc)},
        )

    finally:
        cleanup_started = perf_counter()
        _close_data_manager(
            result_dm,
            label="result",
            run_id=run_id,
            attempt_id=attempt_id,
        )
        _close_data_manager(
            dm,
            label="input",
            run_id=run_id,
            attempt_id=attempt_id,
        )
        _remove_directory(
            run_temp_dir,
            label="temp",
            run_id=run_id,
            attempt_id=attempt_id,
        )
        _remove_directory(
            staging_output,
            label="staging",
            run_id=run_id,
            attempt_id=attempt_id,
        )
        cleanup_seconds = perf_counter() - cleanup_started

    total_seconds = perf_counter() - handler_started
    if success:
        logger.info(
            "raw-convert completed: run_id=%s attempt_id=%s output_format=%s "
            "size=%d file_count=%d load_head_seconds=%.3f "
            "conversion_seconds=%.3f publish_seconds=%.3f "
            "publish_workers=%d publish_throughput_mib_per_second=%.3f "
            "cleanup_seconds=%.3f total_seconds=%.3f",
            run_id,
            attempt_id,
            output_format,
            output_size,
            output_file_count,
            load_head_seconds,
            conversion_seconds,
            publish_seconds,
            PUBLISH_MAX_WORKERS,
            publish_mib_per_second,
            cleanup_seconds,
            total_seconds,
        )

    return response


# ---------------------------------------------------------------------------
# Storage operations
# ---------------------------------------------------------------------------

def _output_prefix(output_path: str) -> str:
    """校验外部输入，防止前缀清理扩大到 bucket 根或上级目录。"""
    if not isinstance(output_path, str):
        raise ValueError("output_path must be a relative .zarr path")
    prefix = output_path.rstrip("/")
    if (
        not prefix.endswith(".zarr")
        or prefix.rsplit("/", 1)[-1] == ".zarr"
        or "\\" in prefix
        or "\x00" in prefix
        or any(part in ("", ".", "..") for part in prefix.split("/"))
    ):
        raise ValueError("output_path must be a relative .zarr path without '.' or '..'")
    return prefix + "/"


@contextmanager
def _oss_client():
    """本次调用的 STS 凭证和独立连接池，结束后关闭连接。"""
    auth = oss2.StsAuth(
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        os.environ["ALIBABA_CLOUD_SECURITY_TOKEN"],
    )
    session = oss2.Session(pool_size=1)
    try:
        yield oss2.Bucket(
            auth, OSS_ENDPOINT_URL, OSS_BUCKET_NAME, session=session,
            connect_timeout=(OSS_CONNECT_TIMEOUT_SECONDS, OSS_READWRITE_TIMEOUT_SECONDS),
            enable_crc=True,
        )
    finally:
        session.session.close()


def _oss_request(operation, *args, **kwargs):
    """只重试幂等的列举、删除和覆盖上传，不重试权限等确定性错误。"""
    for attempt in range(3):
        try:
            return operation(*args, **kwargs)
        except (oss2.exceptions.RequestError, oss2.exceptions.InconsistentError):
            if attempt == 2:
                raise
        except oss2.exceptions.ServerError as exc:
            if attempt == 2 or not (exc.status in (408, 429) or 500 <= exc.status < 600):
                raise
        logger.warning("raw-convert OSS request retry: operation=%s next_attempt=%d",
                       operation.__name__, attempt + 2)
        sleep(0.5 * 2**attempt)


def _clear_oss_prefix(client, prefix: str) -> int:
    """每页最多清理 1000 个对象；逐页释放，不保存整个目录清单。"""
    deleted_count = 0
    while True:
        # 删除后重新列举首页，无需依赖已删除对象的 continuation token。
        page = _oss_request(client.list_objects_v2, prefix=prefix, max_keys=1000)
        keys = [item.key for item in page.object_list]
        if not keys:
            return deleted_count
        result = _oss_request(client.batch_delete_objects, keys)
        confirmed = set(result.deleted_keys)
        if confirmed != set(keys):
            raise RuntimeError("OSS prefix cleanup did not confirm all deleted objects")
        deleted_count += len(keys)


def _publish_directory(source: Path, prefix: str, *, max_workers: int) -> int:
    """有界并发上传 NAS 文件，直接访问 OSS；失败后清理不完整输出。"""
    thread_state = threading.local()
    worker_clients = SimpleQueue()
    pending = set()
    uploaded_count = 0
    last_progress = perf_counter()

    def init_worker() -> None:
        thread_state.client = worker_clients.get_nowait()

    def upload_one(local_path: Path) -> None:
        key = prefix + local_path.relative_to(source).as_posix()
        # SDK 从文件流读取并校验 CRC，不把整个 chunk 读入 Python bytes。
        _oss_request(thread_state.client.put_object_from_file, key, str(local_path))

    with ExitStack() as stack:
        control_client = stack.enter_context(_oss_client())
        removed_count = _clear_oss_prefix(control_client, prefix)
        logger.info("raw-convert OSS output prepared: prefix=%s removed_files=%d",
                    prefix, removed_count)
        for _ in range(max_workers):
            worker_clients.put(stack.enter_context(_oss_client()))
        try:
            with ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="oss-sdk-publish",
                initializer=init_worker,
            ) as executor:
                try:
                    for directory, _, filenames in os.walk(source):
                        for filename in filenames:
                            pending.add(executor.submit(upload_one, Path(directory) / filename))
                            if len(pending) >= max_workers * 2:
                                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                                for future in completed:
                                    future.result()
                                    uploaded_count += 1
                                if perf_counter() - last_progress >= 5:
                                    logger.info("raw-convert OSS upload progress: prefix=%s files=%d",
                                                prefix, uploaded_count)
                                    last_progress = perf_counter()
                    while pending:
                        completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                        for future in completed:
                            future.result()
                            uploaded_count += 1
                except Exception:
                    for future in pending:
                        future.cancel()
                    raise
        except Exception:
            # executor 已等待在途 PUT 结束，清理后不会再有 worker 写回对象。
            try:
                _clear_oss_prefix(control_client, prefix)
            except Exception:
                logger.warning("raw-convert OSS failed-output cleanup failed: prefix=%s",
                               prefix, exc_info=True)
            raise

    return uploaded_count


def _directory_stats(path: Path) -> tuple[int, int]:
    """返回目录的总字节数和文件数。"""
    total_size = 0
    file_count = 0
    for directory, _, filenames in os.walk(path):
        for filename in filenames:
            file_path = os.path.join(directory, filename)
            total_size += os.path.getsize(file_path)
            file_count += 1
    return total_size, file_count


def _remove_directory(
    path: Path,
    *,
    label: str,
    run_id: int,
    attempt_id: str,
) -> None:
    """删除目录；失败时记录警告，但不覆盖本次转换结果。"""
    try:
        if path.exists():
            shutil.rmtree(path)
    except Exception:
        logger.warning(
            "raw-convert cleanup failed: run_id=%s attempt_id=%s target=%s path=%s",
            run_id,
            attempt_id,
            label,
            path,
            exc_info=True,
        )


def _close_data_manager(
    data_manager,
    *,
    label: str,
    run_id: int,
    attempt_id: str,
) -> None:
    """关闭 data manager；失败时保留原始转换结果。"""
    if data_manager is None:
        return
    try:
        data_manager.close()
    except Exception:
        logger.warning(
            "raw-convert close failed: run_id=%s attempt_id=%s target=%s",
            run_id,
            attempt_id,
            label,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# HTTP event and callback
# ---------------------------------------------------------------------------

def _parse_event(event) -> dict:
    """解析 FC HTTP 触发事件。"""
    import base64

    if isinstance(event, dict) and "statusCode" not in event:
        if "body" not in event and "run_id" in event:
            return event

    if isinstance(event, bytes):
        event = event.decode("utf-8")

    try:
        event_json = json.loads(event) if isinstance(event, str) else event
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid event: not a valid JSON string")

    if not isinstance(event_json, dict):
        return _fc_response(
            400,
            f"Invalid event: expected dict, got {type(event_json).__name__}",
        )
    if "body" not in event_json:
        return _fc_response(400, "Invalid event: missing 'body' field")

    request_body = event_json["body"]
    if event_json.get("isBase64Encoded"):
        request_body = base64.b64decode(request_body).decode("utf-8")

    try:
        if isinstance(request_body, str):
            return json.loads(request_body)
        return request_body
    except (json.JSONDecodeError, TypeError):
        return _fc_response(400, "Invalid body: not a valid JSON string")


def _fc_response(status_code: int, body) -> dict:
    response_body = (
        json.dumps(body, ensure_ascii=False) if not isinstance(body, str) else body
    )
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "isBase64Encoded": False,
        "body": response_body,
    }


def _notify_callback(
    run_id: int,
    is_success: bool,
    out_path: str | None = None,
    output_size: int | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    """通过主服务回调报告转换结果。"""
    body = {
        "run_id": run_id,
        "is_success": is_success,
        "out_path": out_path,
        "output_size": output_size,
        "error_type": error_type,
        "error_message": error_message,
    }
    try:
        response = requests.post(
            CALLBACK_URL,
            json=body,
            headers={
                "Content-Type": "application/json",
                "X-FC-Token": CALLBACK_TOKEN,
            },
            timeout=30,
        )
        if response.status_code != 200:
            logger.error(
                "raw-convert callback failed: status=%s body=%s",
                response.status_code,
                response.text,
            )
    except Exception:
        logger.exception("raw-convert callback request failed")
