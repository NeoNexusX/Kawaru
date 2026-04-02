from pathlib import Path
from constants.file_constants import LOCAL_STORAGE_ROOT, STORAGE_ROOT, TEMP_PARTS_ROOT


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

    文件直接保存在 OSS 存储桶下,不按用户ID分文件夹
    """
    return filename