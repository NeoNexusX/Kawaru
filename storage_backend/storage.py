from pathlib import Path

from background.celery_app import wait_for_result
from background.file_background_helper import (
    get_oss_upload_sts_token_task,
    compute_upload_local_file_hash_task,
    check_oss_file_task,
    get_oss_file_size_task
)
from models.auth import OssStsToken
from constants.file_constants import LOCAL_STORAGE_ROOT, STORAGE_ROOT, TEMP_PARTS_ROOT, OSS_ZIP_STORAGE_ROOT
from constants.experiment_constants import EXPERIMENT_STORAGE_ROOT, OSS_EXPERIMENT_STORAGE_ROOT

from tools.logger import get_logger

logger = get_logger("storage")


def ensure_local_storage_dirs() -> None:
    """
    确保本地存储目录存在
    在上传操作前调用,创建必要的存储目录结构
    """
    for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_PARTS_ROOT):
        path.mkdir(parents=True, exist_ok=True)


def prepare_local_storage_path(filename: str) -> Path:
    """
    为简单上传准备最终保存路径

    文件直接保存在 LOCAL_STORAGE_ROOT 下,不按用户ID分文件夹
    """
    LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    return LOCAL_STORAGE_ROOT / filename


def prepare_oss_storage_path(filename: str) -> str:
    """
    为OSS存储准备最终保存路径

    文件直接保存在 OSS 存储桶的 OSS_ZIP_STORAGE_ROOT 下
    """
    return f"{OSS_ZIP_STORAGE_ROOT.lstrip('/')}/{filename}"


def create_local_experiment_directories(experiment_id: int) -> Path:
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


def create_oss_experiment_directories(experiment_id: int) -> str:
    """
    创建并返回 实验目录
    Return:
        str路径
    Raise:
        None
    """
    processed_dir = f"{OSS_EXPERIMENT_STORAGE_ROOT.lstrip('/')}/exp_{experiment_id}"

    return processed_dir


async def get_oss_upload_sts_token(file_path: str) -> OssStsToken:
    """
    获取OSS上传STS Token(任务队列异步执行)

    :param file_path: 文件路径（如"folder/file.zip")
    :return: OSS上传STS Token模型
    """
    try:
        res = get_oss_upload_sts_token_task.delay(file_path) # type: ignore
        result = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        return OssStsToken(**result)
    except Exception as e: # pylint:disable = broad-exception-caught
        logger.error(f"failed to get oss upload sts token: {e}")
        raise e


async def check_oss_file(file_path: str, size: int) -> bool:
    """
    检查OSS文件是否存在且大小匹配

    :param file_path: OSS文件路径（如"folder/file.zip")
    :param size: 文件大小（字节）

    :return: 是否存在且大小匹配，否则返回False
    """
    res = check_oss_file_task.delay(file_path, size) # type:ignore
    is_file_exist = await wait_for_result(res, poll_interval=0.1, timeout=10)
    return is_file_exist


async def compute_upload_local_file_hash(file_path: str) -> tuple[str, int]:
    """
    计算上传文件元数据的本地版本(md值和大小)
    分片读取指定本地路径的文件内容,避免一次性加载到内存
    """
    res = compute_upload_local_file_hash_task.delay(file_path) # type: ignore
    result = await wait_for_result(res, poll_interval=0.1, timeout=20) # type: ignore # 针对大文件此处timeout需要调大
    return result


async def get_oss_file_size(file_path: str) -> int:
    """
    获取OSS文件大小

    :param file_path: OSS文件路径（如"folder/file.zip")
    :return: 文件大小，单位字节
    """
    res = get_oss_file_size_task.delay(file_path) # type: ignore
    size = await wait_for_result(res, poll_interval=0.1, timeout=10)
    if size is None:
        raise ValueError(f"failed to get oss file size: {file_path}")
    return size
