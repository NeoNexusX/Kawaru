"""
辅助函数
"""
import os
import threading
import zipfile
import hashlib
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from httpx import AsyncClient

import oss2
from oss2.models import PartInfo

from config import get_config, Config

config: Config = get_config()

# Constants
CHUNK_BUFFER_SIZE = 1 * 1024 * 1024
# 分片并发上传的线程数：读取与上传重叠，整体耗时 ≈ max(解压, 上传) 而不是两者之和
UPLOAD_CONCURRENCY = max(1, int(os.environ.get("FC_UNZIP_UPLOAD_CONCURRENCY", "8")))
# SDK 分片直传的分片大小（16MB）：并发上传时内存峰值 ≈ 分片大小 × 并发数（16MB × 8 ≈ 128MB）；
# 20GB 级文件约 1280 片，远低于 OSS 10000 片上限
MULTIPART_PART_SIZE = max(1, int(os.environ.get("FC_UNZIP_PART_SIZE_MB", "16"))) * 1024 * 1024
OSS_UPLOAD_CALLBACK_ENDPOINT = os.getenv("OSS_UPLOAD_CALLBACK_ENDPOINT")
GET_EXPECTED_HASH_ENDPOINT = os.getenv("GET_EXPECTED_HASH_ENDPOINT")
PROCESSED_DIR = config.oss.oss_origin_storage_root
# FC 回调验证 Token
FC_CALLBACK_TOKEN = os.environ["CALLBACK_TOKEN"]
if not OSS_UPLOAD_CALLBACK_ENDPOINT:
    raise RuntimeError("OSS_UPLOAD_CALLBACK_ENDPOINT environment variable is not set")
if not GET_EXPECTED_HASH_ENDPOINT:
    raise RuntimeError("GET_EXPECTED_HASH_ENDPOINT environment variable is not set")


# Close the info log printed by the oss SDK
logging.getLogger("oss2.api").setLevel(logging.ERROR)
logging.getLogger("oss2.auth").setLevel(logging.ERROR)

LOGGER = logging.getLogger()

logger = LOGGER


async def get_expected_hash(file_id: int) -> str:
    """按 file_id 向后端查询理论哈希（file_id 从 OSS 对象键解析）"""
    async with AsyncClient(timeout=10) as client:
        res = await client.get(
            GET_EXPECTED_HASH_ENDPOINT,
            params={"file_id": file_id},
            headers={"X-FC-Token": FC_CALLBACK_TOKEN},
        )
        res.raise_for_status()
        expected_hash = res.json().get("expected_hash")
        if not expected_hash:
            raise ValueError("expected_hash is empty")
        return res.json()["expected_hash"]


async def oss_upload_callback(
    file_id: int,
    file_path: str,
    is_success: bool,
    imzml_path: str | None = None,
    ibd_path: str | None = None,
    imzml_size: int | None = None,
    ibd_size: int | None = None,
    act_hash: str | None = None,
    error_type: str | None = None,
):
    """
    上传回调函数，用于处理上传成功或失败的情况

    关联键为 file_id（从 OSS 对象键 data/{file_verify_code}/{file_id}.zip 的文件名解析）
    """
    async with AsyncClient(timeout=10) as client:
        res = await client.post(
            OSS_UPLOAD_CALLBACK_ENDPOINT,
            json={
                "file_id": file_id,
                "file_path": file_path,
                "is_success": is_success,
                "imzml_path": imzml_path,
                "ibd_path": ibd_path,
                "imzml_size": imzml_size,
                "ibd_size": ibd_size,
                "error_type": error_type,
                "act_hash": act_hash,
            },
            headers={"X-FC-Token": FC_CALLBACK_TOKEN},
        )
        res.raise_for_status()


class ConcurrentPartUploader:
    """分片并发上传器。

    数据读取仍由调用线程串行完成（保证 MD5 与 namelist 顺序一致），攒满一个分片即交给
    线程池并发直传 OSS：
    - 信号量把"在飞分片"限制为并发数，读取线程攒够名额后阻塞，内存峰值 ≈ 分片大小 × 并发数；
    - 工作线程不向外抛异常，首个异常记录在案，由读取线程在提交下一个分片前抛出（快速失败），
      读取线程结束后再统一 wait_all()，保证 complete 之前所有分片都已上传成功。
    """

    def __init__(self, bucket: "oss2.Bucket", concurrency: int):
        self._bucket = bucket
        self._pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="oss-upload")
        self._slots = threading.BoundedSemaphore(concurrency)
        self._lock = threading.Lock()
        self._error: Exception | None = None
        self._futures: list[Future] = []
        self._parts: dict[str, list[PartInfo]] = {}

    def upload_part(self, key: str, upload_id: str, part_number: int, data: bytes) -> None:
        """提交一个分片上传任务；在飞分片达到并发上限时阻塞调用线程"""
        self._submit(self._do_upload_part, key, upload_id, part_number, data)

    def put_object(self, key: str, data: bytes) -> None:
        """提交一个整对象上传任务（用于不大于一个分片的小成员）"""
        self._submit(self._do_put_object, key, data)

    def wait_all(self) -> None:
        """等待所有已提交的任务结束，并抛出工作线程的首个异常"""
        for future in self._futures:
            future.result()
        self.raise_if_failed()

    def parts_of(self, upload_id: str) -> list[PartInfo]:
        """返回某个分片上传已完成的分片列表（按 part_number 升序）"""
        with self._lock:
            parts = self._parts.get(upload_id, [])
        return sorted(parts, key=lambda part: part.part_number)

    def close(self) -> None:
        """等待线程池退出（不 wait 会让解释器退出时阻塞在 atexit 上）"""
        self._pool.shutdown(wait=True)

    def raise_if_failed(self) -> None:
        """若有分片上传失败则抛出该异常"""
        with self._lock:
            error = self._error
        if error is not None:
            raise error

    def _submit(self, func, *args) -> None:
        self._slots.acquire()
        try:
            # 已有失败：立即停止读取，不再提交新分片
            self.raise_if_failed()
            self._futures.append(self._pool.submit(self._run, func, *args))
        except BaseException:
            self._slots.release()
            raise

    def _run(self, func, *args) -> None:
        try:
            func(*args)
        except Exception as e:  # pylint: disable=W0718
            with self._lock:
                if self._error is None:
                    self._error = e
        finally:
            self._slots.release()

    def _do_upload_part(self, key: str, upload_id: str, part_number: int, data: bytes) -> None:
        res = self._bucket.upload_part(key, upload_id, part_number, data)
        with self._lock:
            self._parts.setdefault(upload_id, []).append(PartInfo(part_number, res.etag))

    def _do_put_object(self, key: str, data: bytes) -> None:
        self._bucket.put_object(key, data)


def _read_and_put_object(uploader: ConcurrentPartUploader, key: str, src: "zipfile.ZipExtFile",
                         digest) -> int:
    """读取整个成员（不大于一个分片，含空文件）并整对象直传 OSS，返回解压后字节数"""
    data = bytearray()
    while True:
        chunk = src.read(CHUNK_BUFFER_SIZE)
        if not chunk:
            break
        digest.update(chunk)
        data += chunk
    uploader.put_object(key, bytes(data))
    return len(data)


def _read_and_upload_parts(uploader: ConcurrentPartUploader, key: str, upload_id: str,
                           src: "zipfile.ZipExtFile", digest, member_size: int) -> int:
    """流式读取成员并分片并发直传 OSS，返回解压后字节数"""
    logger.info(
        "multipart upload started: key=%s, size=%.2fMB, part_size=%.2fMB, concurrency=%d",
        key, member_size / 1024 / 1024, MULTIPART_PART_SIZE / 1024 / 1024, UPLOAD_CONCURRENCY,
    )
    part_number = 1
    total = 0
    buffer = bytearray()
    while True:
        chunk = src.read(CHUNK_BUFFER_SIZE)
        if not chunk:
            break
        # 哈希必须按成员在 namelist 中的先后顺序逐字节累加，因此留在读取线程里做
        digest.update(chunk)
        buffer += chunk
        total += len(chunk)
        while len(buffer) >= MULTIPART_PART_SIZE:
            data = bytes(buffer[:MULTIPART_PART_SIZE])
            del buffer[:MULTIPART_PART_SIZE]
            uploader.upload_part(key, upload_id, part_number, data)
            part_number += 1
    if buffer:
        # 最后不足一个分片的剩余数据
        uploader.upload_part(key, upload_id, part_number, bytes(buffer))
    return total


def extract_zip_and_compute_hash(
    bucket: "oss2.Bucket", zip_file_path: Path, key_prefix: str
) -> tuple[str, str, str, int, int]:
    """
    流式解压 ZIP 文件，同时计算整体哈希，并通过 OSS SDK 分片直传（不经过 ossfs 挂载盘）。

    数据只读取一遍。在解压流中分块读取数据，同时：
    - 更新 MD5 哈希
    - 攒满一个分片（MULTIPART_PART_SIZE）即交给线程池并发 upload_part 直传 OSS
      （默认 8 线程），读取与上传重叠，整体耗时 ≈ max(解压, 上传)。
      每个分片携带 Content-MD5 并由 OSS 服务端强制校验，失败立即抛异常，
      不存在"写入返回成功但数据未上传"的静默损坏。

    并发不影响正确性：解压、哈希仍由当前线程按 namelist 顺序串行完成，上传线程只是
    把"已经定好序号的分片"发出去；任何分片失败都会 abort 掉所有未完成的分片上传。

    Args:
        bucket: oss2.Bucket 实例（使用内网 endpoint）
        zip_file_path: ZIP 文件路径
        key_prefix: OSS 对象键前缀，如 origin/{file_verify_code}/{file_id}

    Returns:
        (imzml_key, ibd_key, file_verify_code_hex, imzml_size, ibd_size)

    Raise:
        - zipfile.BadZipFile: ZIP 文件无效
        - FileNotFoundError: 缺少 .imzML 或 .ibd 文件
        - ValueError: ZIP 文件包含不安全的路径
        - oss2.exceptions.OssError: 分片上传失败（失败即中断，不会产生坏对象）
    """
    file_verify_code = hashlib.md5()
    imzml_key = None
    ibd_key = None
    imzml_size = 0
    ibd_size = 0

    uploader = ConcurrentPartUploader(bucket, UPLOAD_CONCURRENCY)
    # 已开启但尚未合并的分片上传 (key, upload_id)，任何异常都要 abort，
    # 否则未完成的分片会残留在 OSS 上持续计费
    pending_multiparts: list[tuple[str, str]] = []

    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                if member.endswith("/"):
                    # 目录条目，无数据，跳过
                    continue
                member_name = member.replace("\\", "/")
                # OSS 对象键合法性检查（OSS 键为扁平命名空间，仅防范异常键名）
                if ".." in member_name.split("/"):
                    logger.error("unsafe file path in zip file: %s", member)
                    raise ValueError("unsafe file path in zip file")

                key = f"{key_prefix}/{member_name}"
                member_size = zip_ref.getinfo(member).file_size

                with zip_ref.open(member) as src:
                    if member_size <= MULTIPART_PART_SIZE:
                        # 一个分片就装得下：整对象直传，省掉 init/complete 两次往返
                        file_size = _read_and_put_object(uploader, key, src, file_verify_code)
                    else:
                        upload_id = bucket.init_multipart_upload(key).upload_id
                        pending_multiparts.append((key, upload_id))
                        file_size = _read_and_upload_parts(
                            uploader, key, upload_id, src, file_verify_code, member_size
                        )

                # 记录 imzML 和 ibd 文件的对象键与大小
                suffix = Path(member_name).suffix.lower()
                if suffix == ".imzml":
                    imzml_key = key
                    imzml_size = file_size
                elif suffix == ".ibd":
                    ibd_key = key
                    ibd_size = file_size

            if not imzml_key or not ibd_key:
                raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")

            # 全部成员读完后再等待并合并分片：读取阶段全程不被上传阻塞
            uploader.wait_all()
            while pending_multiparts:
                key, upload_id = pending_multiparts.pop()
                bucket.complete_multipart_upload(key, upload_id, uploader.parts_of(upload_id))
    except Exception:
        while pending_multiparts:
            key, upload_id = pending_multiparts.pop()
            try:
                bucket.abort_multipart_upload(key, upload_id)
            except Exception as abort_err:  # pylint: disable=W0718
                logger.error("abort multipart upload failed, key=%s: %s", key, abort_err)
        raise
    finally:
        uploader.close()

    if not validate_imzml_files(imzml_key, ibd_key):
        raise AssertionError("Invalid imzML or ibd file")

    return imzml_key, ibd_key, file_verify_code.hexdigest(), imzml_size, ibd_size


def validate_imzml_files(imzml_path: Path, ibd_path: Path) -> bool: # pylint: disable=unused-argument
    """
    验证 imzML 和 ibd 文件是否为有效可读的 MSI 实验数据

    Args:
        imzml_path: imzML 文件路径
        ibd_path: ibd 文件路径

    Returns:
        True 表示验证通过，False 表示验证失败

    TODO: 后续扩展验证逻辑
    - 检查文件格式是否正确
    - 验证文件完整性
    - 检查文件大小是否合理
    - 验证 imzML 和 ibd 文件的关联性
    """
    # 目前直接返回 True，后续扩展
    return True

