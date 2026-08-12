"""存储和文件相关的常量定义"""
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

OSS_ZIP_STORAGE_ROOT = config.oss.oss_zip_storage_root
OSS_PACKUP_ZIP_STORAGE_ROOT = config.oss.oss_packup_zip_storage_root
OSS_ORIGIN_STORAGE_ROOT = config.oss.oss_origin_storage_root


# 单次上传最大允许分片数限制
MAX_PARTS_PER_UPLOAD = int(storage_config.max_chunks_per_upload)

# 存储类型和文件类型配置
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
ALLOWED_EXPERIMENT_TYPES = set(storage_config.allowed_experiment_types)

# 文件名限制
MAX_FILENAME_LENGTH = int(storage_config.max_filename_length)

# 清理过期预检任务的间隔时间
PREFLIGHT_TTL_MINUTES = int(getattr(storage_config, "preflight_ttl_minutes", 30))
CLEANUP_PREFLIGHTS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_preflights_interval_minutes", 60))

# 下载zip文件清理策略
DOWNLOAD_ZIP_TTL_MINUTES = int(getattr(storage_config, "download_zip_ttl_minutes", 30))
CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_download_zip_interval_minutes", 60))

# 僵尸 process_run 清理策略（FC 算法/转换超时无回调）
STALE_PROCESS_RUN_TIMEOUT_MINUTES = int(getattr(storage_config, "stale_process_run_timeout_minutes", 60))
CLEANUP_STALE_PROCESS_RUNS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_stale_process_runs_interval_minutes", 15))

# OSS persistent tag 重试策略（上传回调后异步补打标签，见 file_background.ensure_persistent_tags）
# 生命周期规则按 persistent tag 保留对象，打完 tag 前对象不会被回收，因此可安全跨分钟级重试
ENSURE_PERSISTENT_TAG_MAX_ATTEMPTS = storage_config.ensure_persistent_tag_max_attempts
ENSURE_PERSISTENT_TAG_RETRY_SECONDS = storage_config.ensure_persistent_tag_retry_seconds
CLEANUP_PERSISTENT_TAGS_INTERVAL_MINUTES = storage_config.cleanup_persistent_tags_interval_minutes

# 签名URL过期时间 & Redis缓存过期时间（Redis缓存比签名URL早过期5分钟，确保缓存过期后旧URL仍有效）
SIGNED_URL_EXPIRE_SECONDS = int(getattr(storage_config, "signed_url_ttl_minutes", 60)) * 60
PACKUP_CACHE_TTL_SECONDS = max(60, SIGNED_URL_EXPIRE_SECONDS - 300)


# OSS相关常量
OSS_BUCKET_NAME = config.oss.bucket_name
OSS_REGION_ID = config.oss.region_id
OSS_DEFAULT_TIMEOUT_SECONDS = config.oss.oss_default_timeout_seconds
FC_PACKUP_ENDPOINT = config.oss.fc_packup_endpoint
FC_PACKUP_TIMEOUT_SECONDS = config.oss.fc_packup_timeout_seconds
FC_ALGORITHM_ENDPOINT = config.oss.fc_algorithm_endpoint
FC_ALGORITHM_TIMEOUT_SECONDS = config.oss.fc_algorithm_timeout_seconds
FC_TIC_ENDPOINT = config.oss.fc_tic_endpoint
FC_TIC_TIMEOUT_SECONDS = config.oss.fc_tic_timeout_seconds
FC_FEATURE_ENDPOINT = config.oss.fc_feature_endpoint
FC_FEATURE_TIMEOUT_SECONDS = config.oss.fc_feature_timeout_seconds
FC_RAW_CONVERT_ENDPOINT = config.oss.fc_raw_convert_endpoint
FC_RAW_CONVERT_TIMEOUT_SECONDS = config.oss.fc_raw_convert_timeout_seconds
FC_CLUSTERING_ENDPOINT = config.oss.fc_clustering_endpoint
FC_CLUSTERING_TIMEOUT_SECONDS = config.oss.fc_clustering_timeout_seconds


__all__ = [
    "BYTES_IN_KB",
    "BYTES_IN_MB",
    "BYTES_IN_GB",
    "BYTES_IN_TB",
    "ALLOWED_STORAGE_TYPES",
    "ALLOWED_FILE_TYPES",
    "ALLOWED_EXPERIMENT_TYPES",
    "MAX_FILENAME_LENGTH",
    "PREFLIGHT_TTL_MINUTES",
    "CLEANUP_PREFLIGHTS_INTERVAL_MINUTES",
    "DOWNLOAD_ZIP_TTL_MINUTES",
    "CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES",
    "STALE_PROCESS_RUN_TIMEOUT_MINUTES",
    "CLEANUP_STALE_PROCESS_RUNS_INTERVAL_MINUTES",
    "ENSURE_PERSISTENT_TAG_MAX_ATTEMPTS",
    "ENSURE_PERSISTENT_TAG_RETRY_SECONDS",
    "CLEANUP_PERSISTENT_TAGS_INTERVAL_MINUTES",
    "SIGNED_URL_EXPIRE_SECONDS",
    "PACKUP_CACHE_TTL_SECONDS",
    "OSS_ZIP_STORAGE_ROOT",
    "OSS_ORIGIN_STORAGE_ROOT",
    "FC_PACKUP_ENDPOINT",
    "FC_PACKUP_TIMEOUT_SECONDS",
    "FC_ALGORITHM_ENDPOINT",
    "FC_ALGORITHM_TIMEOUT_SECONDS",
    "FC_TIC_ENDPOINT",
    "FC_TIC_TIMEOUT_SECONDS",
    "FC_FEATURE_ENDPOINT",
    "FC_FEATURE_TIMEOUT_SECONDS",
    "FC_RAW_CONVERT_ENDPOINT",
    "FC_RAW_CONVERT_TIMEOUT_SECONDS",
    "FC_CLUSTERING_ENDPOINT",
    "FC_CLUSTERING_TIMEOUT_SECONDS",
    "OSS_DEFAULT_TIMEOUT_SECONDS",
    "OSS_REGION_ID",
    "OSS_BUCKET_NAME",
]
