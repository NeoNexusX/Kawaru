"""存储和文件相关的常量定义"""
from pathlib import Path
from boot.config import get_config

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
TEMP_PARTS_ROOT = STORAGE_ROOT / "temp_chunks"
OSS_ZIP_STORAGE_ROOT = config.oss.oss_zip_storage_root
OSS_ORIGIN_STORAGE_ROOT = config.oss.oss_origin_storage_root
OSS_PACKUP_ZIP_STORAGE_ROOT = config.oss.oss_packup_zip_storage_root


# 文件大小限制
MAX_FILE_SIZE = int(storage_config.max_file_size_gb) * BYTES_IN_GB
CHUNK_BUFFER_SIZE = int(storage_config.chunk_buffer_size_mb) * BYTES_IN_MB

# 单次上传最大允许分片数限制
MAX_PARTS_PER_UPLOAD = int(storage_config.max_chunks_per_upload)

# 存储类型和文件类型配置
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
ALLOWED_EXPERIMENT_TYPES = set(storage_config.allowed_experiment_types)

# 文件名限制
MAX_FILENAME_LENGTH = int(storage_config.max_filename_length)

# 用户文件配额
MAX_FILES_PER_USER = int(storage_config.max_files_per_user)
MAX_TOTAL_STORAGE_PER_USER = MAX_FILE_SIZE * MAX_FILES_PER_USER

# 下载次数限制
MAX_DOWNLOADS_PER_USER = int(getattr(storage_config, "max_downloads_per_user", 50))

# 清理过期预检任务的间隔时间
PREFLIGHT_TTL_MINUTES = int(getattr(storage_config, "preflight_ttl_minutes", 30))
CLEANUP_PREFLIGHTS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_preflights_interval_minutes", 60))

# 下载zip文件清理策略
DOWNLOAD_ZIP_TTL_MINUTES = int(getattr(storage_config, "download_zip_ttl_minutes", 30))
CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_download_zip_interval_minutes", 60))


# OSS相关常量
OSS_BUCKET_NAME = config.oss.bucket_name
OSS_REGION_ID = config.oss.region_id
OSS_DEFAULT_TIMEOUT_SECONDS = config.oss.oss_default_timeout_seconds
FC_LOGSTORE_NAME = config.oss.fc_logstore_name
FC_LOG_PROJECT_NAME = config.oss.fc_log_project_name
FC_UNZIP_TIMEOUT_SECONDS = config.oss.fc_unzip_timeout_seconds
FC_PACKUP_ENDPOINT = config.oss.fc_packup_endpoint
FC_PACKUP_TIMEOUT_SECONDS = config.oss.fc_packup_timeout_seconds


__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "BYTES_IN_KB",
    "BYTES_IN_MB",
    "BYTES_IN_GB",
    "BYTES_IN_TB",
    "STORAGE_ROOT",
    "LOCAL_STORAGE_ROOT",
    "TEMP_PARTS_ROOT",
    "MAX_FILE_SIZE",
    "CHUNK_BUFFER_SIZE",
    "ALLOWED_STORAGE_TYPES",
    "ALLOWED_FILE_TYPES",
    "ALLOWED_EXPERIMENT_TYPES",
    "MAX_FILENAME_LENGTH",
    "MAX_FILES_PER_USER",
    "MAX_TOTAL_STORAGE_PER_USER",
    "MAX_DOWNLOADS_PER_USER",
    "PREFLIGHT_TTL_MINUTES",
    "CLEANUP_PREFLIGHTS_INTERVAL_MINUTES",
    "DOWNLOAD_ZIP_TTL_MINUTES",
    "CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES",
    "OSS_ZIP_STORAGE_ROOT",
    "OSS_ORIGIN_STORAGE_ROOT",
    "FC_UNZIP_TIMEOUT_SECONDS",
    "FC_PACKUP_ENDPOINT",
    "FC_PACKUP_TIMEOUT_SECONDS",
    "OSS_DEFAULT_TIMEOUT_SECONDS",
    "FC_LOGSTORE_NAME",
    "FC_LOG_PROJECT_NAME",
    "OSS_REGION_ID",
    "OSS_BUCKET_NAME",
]
