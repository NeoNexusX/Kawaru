import io
import hashlib
from constants.file_constants import CHUNK_BUFFER_SIZE
from background.celery_app import celery_app
assert celery_app is not None, "celery_app is None"


from tools import get_logger
logger = get_logger("background.file_background")


@celery_app.task
async def compute_upload_file_hash(file_contents: bytes) -> tuple[str, int]:
    """
    计算上传文件元数据的任务队列版本(SHA256哈希和大小)
    其中file_contents必须为bytes类型而非UploadFile类型，方便celery序列化

    分片读取文件内容,避免一次性加载到内存
    读取完成后重置文件指针到开头,供后续写入使用

    """
    sha256 = hashlib.sha256()
    size = 0
    buffer = io.BytesIO(file_contents)
    buffer.seek(0)
    while True:
        chunk = buffer.read(CHUNK_BUFFER_SIZE)
        if not chunk:
            break
        sha256.update(chunk)
        size += len(chunk)
    buffer.seek(0)
    return sha256.hexdigest(), size

