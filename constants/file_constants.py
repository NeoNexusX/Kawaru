"""存储和文件相关的常量定义"""
from pathlib import Path
from envset.config import get_config

# 字节单位常量
BYTES_IN_KB = 1024
BYTES_IN_MB = 1024**2
BYTES_IN_GB = 1024**3
BYTES_IN_TB = 1024**4

# 加载基础存储配置
config = get_config()
if not config:
    raise RuntimeError("Application configuration is not loaded")
storage_config = config.storage

# 存储路径常量
STORAGE_ROOT = Path(storage_config.root)
LOCAL_STORAGE_ROOT = STORAGE_ROOT / storage_config.local_root
TEMP_CHUNKS_ROOT = STORAGE_ROOT / "temp_chunks"
DOWNLOAD_TEMP_ROOT = STORAGE_ROOT / "download_temp"

# 文件大小限制
MAX_FILE_SIZE = int(storage_config.max_file_size) * BYTES_IN_GB
CHUNK_BUFFER_SIZE = int(storage_config.chunk_buffer_size) * BYTES_IN_MB

# 存储类型和文件类型配置
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)

# 文件名限制
MAX_FILENAME_LENGTH = int(storage_config.max_filename_length)

# 用户文件配额
MAX_FILES_PER_USER = int(storage_config.max_files_per_user)
MAX_TOTAL_STORAGE_PER_USER = MAX_FILE_SIZE * MAX_FILES_PER_USER

# 下载次数限制
MAX_DOWNLOADS_PER_USER = int(getattr(storage_config, "max_downloads_per_user", 50))

# 清理相关的 TTL 设置（分钟）
PREFLIGHT_TTL_MINUTES = int(getattr(storage_config, "preflight_ttl_minutes", 30))
CHUNK_TTL_MINUTES = int(getattr(storage_config, "chunk_ttl_minutes", 30))

__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "BYTES_IN_KB",
    "BYTES_IN_MB",
    "BYTES_IN_GB",
    "BYTES_IN_TB",
    "STORAGE_ROOT",
    "LOCAL_STORAGE_ROOT",
    "TEMP_CHUNKS_ROOT",
    "DOWNLOAD_TEMP_ROOT",
    "MAX_FILE_SIZE",
    "CHUNK_BUFFER_SIZE",
    "ALLOWED_STORAGE_TYPES",
    "ALLOWED_FILE_TYPES",
    "MAX_FILENAME_LENGTH",
    "MAX_FILES_PER_USER",
    "MAX_TOTAL_STORAGE_PER_USER",
    "MAX_DOWNLOADS_PER_USER",
    "PREFLIGHT_TTL_MINUTES",
    "CHUNK_TTL_MINUTES",
]
