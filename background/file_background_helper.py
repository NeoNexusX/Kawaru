import hashlib
import aiofiles
from constants.file_constants import CHUNK_BUFFER_SIZE
from background.celery_app import celery_app
assert celery_app is not None, "celery_app is None"


from tools import get_logger
logger = get_logger("background.file_background")


@celery_app.task
async def compute_upload_local_file_hash(file_path: str) -> tuple[str, int]:
    """
    计算上传文件元数据的本地版本(SHA256哈希和大小)
    分片读取指定本地路径的文件内容,避免一次性加载到内存
    """
    sha256 = hashlib.sha256()
    size = 0
    async with aiofiles.open(file=file_path, mode='rb') as file:
        await file.seek(0)
        while True:
            chunk = await file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            sha256.update(chunk)
            size += len(chunk)
    logger.info(f"computed hash={sha256.hexdigest()} size={size} for file={file_path}")
    return sha256.hexdigest(), size
