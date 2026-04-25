from pathlib import Path

from celery_runtime.result import wait_for_result
from background.file_background_helper import (
    get_oss_upload_sts_token_task,
    compute_upload_local_file_hash_task,
    check_oss_file_exist_task,
    validate_oss_file_task,
    get_oss_file_size_task,
    delete_oss_file_task,
    get_oss_packup_sts_token_task,
)

from models.auth import OssStsToken
from constants.file_constants import (LOCAL_STORAGE_ROOT, STORAGE_ROOT, TEMP_PARTS_ROOT,
                                      OSS_ZIP_STORAGE_ROOT, OSS_DEFAULT_TIMEOUT_SECONDS,
                                     )
from constants.experiment_constants import EXPERIMENT_STORAGE_ROOT, OSS_EXPERIMENT_STORAGE_ROOT

from tools.logger import get_logger

from boot.config import get_config
logger = get_logger("storage")

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time

class LocalStorage:
    """
    本地存储相关操作
    """
    @staticmethod
    def ensure_storage_dirs() -> None:
        """
        确保本地存储目录存在
        在上传操作前调用,创建必要的存储目录结构
        """
        for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_PARTS_ROOT):
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def prepare_storage_path(filename: str,file_type: str) -> Path:
        """
        为简单上传准备最终保存路径

        文件直接保存在 LOCAL_STORAGE_ROOT 下,不按用户ID分文件夹
        """
        LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        file_path=f"{filename}.{file_type}"
        return LOCAL_STORAGE_ROOT / file_path

    @staticmethod
    def create_experiment_directories(experiment_id: int) -> Path:
        """
        创建并返回 实验目录
        Return:
            processed_dir Path对象
        Raise:
            None
        """
        processed_dir = EXPERIMENT_STORAGE_ROOT / f"exp_{experiment_id}"
        processed_dir.mkdir(parents=True, exist_ok=True)

        return processed_dir

    @staticmethod
    def delete_file(path: str | Path) -> None:
        """
        删除本地文件

        同步操作,因为文件删除只是删除元数据
        使用 missing_ok=True,文件不存在时不会抛出异常

        异常处理：不捕获,让异常传播到调用方

        Args:
            path: 文件路径(字符串或 Path 对象)
        """
        # 验证路径不为空
        if not path:
            logger.warning("empty path for deletion, skipping")
            return


        # 转换为字符串进行验证
        path_str = str(path).strip()

        # 验证路径不是特殊值
        if not path_str or any(x in path_str for x in ('..', '\\')):
            logger.warning("invalid path for deletion: %r, skipping", path_str)
            return

        # 转换为 Path 对象
        path_obj = Path(path_str)

        # 验证路径不是目录
        if path_obj.exists() and path_obj.is_dir():
            logger.warning("cannot delete directory: %s, skipping", path_str)
            return

        # 删除文件
        path_obj.unlink(missing_ok=True)

    @staticmethod
    async def compute_upload_file_hash(file_path: str) -> tuple[str, int]:
        """
        计算上传文件元数据的本地版本(md值和大小)
        分片读取指定本地路径的文件内容,避免一次性加载到内存
        """
        res = compute_upload_local_file_hash_task.delay(file_path) # type: ignore
        result = await wait_for_result(res, poll_interval=0.1, timeout=large_file_poll_time) # type: ignore
        if result is None:
            logger.error(f"failed to compute upload file hash due to server timeout: {file_path}")
            return ("", 0)
        return result


class OssStorage:
    """
    OSS存储相关操作
    """
    @staticmethod
    def prepare_storage_path(filename: str, file_type: str) -> str:
        """
        为OSS存储准备最终保存路径

        文件直接保存在 OSS 存储桶的 OSS_ZIP_STORAGE_ROOT 下
        """
        return f"{OSS_ZIP_STORAGE_ROOT.rstrip('/')}/{filename}.{file_type}"

    @staticmethod
    def create_experiment_directories(experiment_id: int) -> str:
        """
        创建并返回 实验目录
        Return:
            str路径
        Raise:
            None
        """
        processed_dir = f"{OSS_EXPERIMENT_STORAGE_ROOT.rstrip('/')}/exp_{experiment_id}"

        return processed_dir

    @staticmethod
    async def get_upload_sts_token(file_path: str) -> OssStsToken | None:
        """
        获取OSS上传STS Token(任务队列异步执行)

        :param file_path: 文件路径（如"folder/file.zip")
        :return: OSS上传STS Token模型
        """
        res = get_oss_upload_sts_token_task.delay(file_path) # type: ignore
        result = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if result is None:
            logger.error(f"failed to get oss upload sts token due to server timeout: {file_path}")
            return None
        oss_sts_token = OssStsToken(**result)
        return oss_sts_token

    @staticmethod
    async def get_packup_sts_token(packup_folder: str, zip_file_path: str) -> OssStsToken | None:
        """
        获取OSS打包STS Token(任务队列异步执行)

        :param packup_folder: 要打包的oss文件夹路径（如"experiment/aaa/")
        :param zip_file_path: 打包后的zip文件路径（如"data/aaa.zip")
        :return: OSS打包STS Token模型
        """
        res = get_oss_packup_sts_token_task.delay(packup_folder, zip_file_path) # type: ignore
        result = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if result is None:
            logger.error(f"failed to get oss packup sts token due to server timeout: from {packup_folder} to {zip_file_path}")
            return None
        oss_sts_token = OssStsToken(**result)
        return oss_sts_token

    @staticmethod
    async def validate_file(file_path: str, size: int, file_verify_code: str) -> bool:
        """
        检查OSS文件是否存在且大小匹配

        :param file_path: OSS文件路径（如"folder/file.zip")
        :param size: 文件大小（字节）

        :return: 是否存在且大小匹配，否则返回False
        """
        res = validate_oss_file_task.delay(file_path, size, file_verify_code) # type:ignore
        is_file_exist = await wait_for_result(res, poll_interval=0.1, timeout=OSS_DEFAULT_TIMEOUT_SECONDS) # type: ignore # 此处需等待OSS端fc函数自动计算完hash
        if is_file_exist is None:
            logger.error(f"failed to validate oss file due to server timeout: {file_path}")
            return False
        return is_file_exist

    @staticmethod
    async def check_file_exist(file_path: str) -> bool:
        """
        检查OSS文件是否存在

        :param file_path: OSS文件路径（如"folder/file.zip")
        :return: 是否存在，否则返回False
        """
        res = check_oss_file_exist_task.delay(file_path) # type:ignore
        is_file_exist = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if is_file_exist is None:
            logger.error(f"failed to check oss file existence due to server timeout: {file_path}")
            return False
        return is_file_exist

    @staticmethod
    async def get_file_size(file_path: str) -> int:
        """
        获取OSS文件大小

        :param file_path: OSS文件路径（如"folder/file.zip")
        :return: 文件大小，单位字节
        """
        res = get_oss_file_size_task.delay(file_path) # type: ignore
        size = await wait_for_result(res, poll_interval=0.1, timeout=10)
        if size is None:
            logger.error(f"failed to get oss file size due to server timeout: {file_path}")
            return 0
        return size


    @staticmethod
    async def delete_file(file_path: str) -> bool:
        """
        删除OSS文件

        :param file_path: OSS文件路径（如"folder/file.zip")
        """
        res = delete_oss_file_task.delay(file_path) # type: ignore
        is_deleted: bool | None = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if is_deleted is None:
            logger.error(f"failed to delete oss file due to server timeout: {file_path}")
            return False
        return is_deleted
