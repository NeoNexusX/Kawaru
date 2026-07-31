from pathlib import Path

from background.file_background_helper import compute_upload_local_file_hash_task
from boot.config import get_config
from celery_runtime.result import wait_for_result
from constants.experiment_constants import ORIGIN_STORAGE_ROOT
from constants.file_constants import LOCAL_STORAGE_ROOT, STORAGE_ROOT, TEMP_PARTS_ROOT
from tools.logger import get_logger

logger = get_logger("storage")

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time


class LocalStorage:
    """本地存储相关操作。"""

    @staticmethod
    def ensure_storage_dirs() -> None:
        """确保本地存储目录存在。"""
        for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_PARTS_ROOT):
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def prepare_storage_path(filename: str, file_type: str) -> Path:
        """为简单上传准备最终保存路径。"""
        LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        file_path = f"{filename}.{file_type}"
        return LOCAL_STORAGE_ROOT / file_path

    @staticmethod
    def create_origin_directories(origin_id: int) -> Path:
        """创建并返回 origin 目录。"""
        processed_dir = ORIGIN_STORAGE_ROOT / f"org_{origin_id}"
        processed_dir.mkdir(parents=True, exist_ok=True)

        return processed_dir

    @staticmethod
    def delete_file(path: str | Path) -> None:
        """删除本地文件。"""
        if not path:
            logger.warning("empty path for deletion, skipping")
            return

        path_str = str(path).strip()

        if not path_str or any(x in path_str for x in ("..", "\\")):
            logger.warning("invalid path for deletion: %r, skipping", path_str)
            return

        path_obj = Path(path_str)

        if path_obj.exists() and path_obj.is_dir():
            logger.warning("cannot delete directory: %s, skipping", path_str)
            return

        path_obj.unlink(missing_ok=True)

    @staticmethod
    async def compute_upload_file_hash(file_path: str) -> tuple[str, int]:
        """分片读取本地文件并计算校验值和大小。"""
        res = compute_upload_local_file_hash_task.delay(file_path)  # type: ignore
        result: tuple[str, int] = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=large_file_poll_time,
        )  # type: ignore
        if result is None:
            logger.error(
                "failed to compute upload file hash due to server timeout: %s",
                file_path,
            )
            return ("", 0)
        return result
