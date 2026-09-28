"""存储和文件相关的常量定义"""
import math

from boot.config import get_config

# 字节单位常量
BYTES_IN_GB = 1024**3

# 加载基础存储配置
config = get_config()
if not config:
    raise RuntimeError("Application configuration is not loaded")
storage_config = config.storage

OSS_ZIP_STORAGE_ROOT = config.oss.oss_zip_storage_root
OSS_PACKUP_ZIP_STORAGE_ROOT = config.oss.oss_packup_zip_storage_root
OSS_ORIGIN_STORAGE_ROOT = config.oss.oss_origin_storage_root


# 存储类型和文件类型配置
ALLOWED_STORAGE_TYPES = set(storage_config.allowed_storage_types)
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
ALLOWED_EXPERIMENT_TYPES = set(storage_config.allowed_experiment_types)

# 文件名限制
MAX_FILENAME_LENGTH = int(storage_config.max_filename_length)

def _compute_preflight_ttl_minutes() -> int:
    """
    计算预检记录 TTL（分钟）：
        公式值 = max_file_size_gb × 2^30 × 8 ÷ (min_upload_bandwidth_mbps × 10^6) ÷ 60 × safety_factor
        最终值 = max(公式值, preflight_ttl_minutes 手填下限)

    未配置 max_file_size_gb 或 min_upload_bandwidth_mbps 时不启用公式，
    直接使用 preflight_ttl_minutes，保持旧部署行为不变。
    """
    floor = int(getattr(storage_config, "preflight_ttl_minutes", 30))
    max_gb = getattr(storage_config, "max_file_size_gb", None)
    min_mbps = getattr(storage_config, "min_upload_bandwidth_mbps", None)
    if not max_gb or not min_mbps:
        return floor
    factor = float(getattr(storage_config, "upload_ttl_safety_factor", 1.5))
    upload_minutes = max_gb * BYTES_IN_GB * 8 / (min_mbps * 1e6) / 60
    return max(floor, math.ceil(upload_minutes * factor))


# 预检记录过期 TTL：按最大文件与最低带宽公式计算，preflight_ttl_minutes 作为下限
PREFLIGHT_TTL_MINUTES = _compute_preflight_ttl_minutes()
CLEANUP_PREFLIGHTS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_preflights_interval_minutes", 60))

# 单文件大小上限（字节），未配置 max_file_size_gb 时为 None（不限制）
_max_file_size_gb = getattr(storage_config, "max_file_size_gb", None)
MAX_SINGLE_FILE_SIZE_BYTES = int(_max_file_size_gb * BYTES_IN_GB) if _max_file_size_gb else None

# 下载zip文件清理策略
DOWNLOAD_ZIP_TTL_MINUTES = int(getattr(storage_config, "download_zip_ttl_minutes", 30))
CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_download_zip_interval_minutes", 60))

# 僵尸 process_run 清理策略（FC 算法/转换超时无回调）
STALE_PROCESS_RUN_TIMEOUT_MINUTES = int(getattr(storage_config, "stale_process_run_timeout_minutes", 60))
CLEANUP_STALE_PROCESS_RUNS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_stale_process_runs_interval_minutes", 15))
# 僵尸聚类 run 清理策略：UMAP/KMeans 较算法处理更重，默认超时更宽
STALE_CLUSTERING_RUN_TIMEOUT_MINUTES = int(getattr(storage_config, "stale_clustering_run_timeout_minutes", 120))

# 签名URL过期时间 & Redis缓存过期时间
# 要求 SIGNED_URL_EXPIRE_SECONDS >= 360（由 storage.signed_url_ttl_minutes >= 6 保证），
# 这样 PACKUP_CACHE_TTL_SECONDS 才能稳定比签名 URL 早 5 分钟过期，
# 确保缓存过期后旧 URL 仍然有效。
SIGNED_URL_EXPIRE_SECONDS = int(getattr(storage_config, "signed_url_ttl_minutes", 60)) * 60
PACKUP_CACHE_TTL_SECONDS = max(60, SIGNED_URL_EXPIRE_SECONDS - 300)
assert SIGNED_URL_EXPIRE_SECONDS > PACKUP_CACHE_TTL_SECONDS, (
    "signed_url_ttl_minutes 必须保证签名 URL 过期时间严格大于 Redis 缓存 TTL"
)


# OSS相关常量
OSS_BUCKET_NAME = config.oss.bucket_name
OSS_REGION_ID = config.oss.region_id
# STS Token 有效期（单位秒；分钟值上限 240 需对齐阿里云 RAM 角色「最大会话时长」设置）
OSS_UPLOAD_STS_TOKEN_EXPIRE_SECONDS = int(getattr(config.oss, "oss_upload_sts_token_expire_minutes", 60)) * 60
OSS_DOWNLOAD_STS_TOKEN_EXPIRE_SECONDS = int(getattr(config.oss, "oss_download_sts_token_expire_minutes", 60)) * 60
# zarr 读 STS 的 Redis 缓存过期时间（比 Token 早过期5分钟，确保缓存过期后旧 Token 仍有效）
ZARR_STS_CACHE_TTL_SECONDS = max(60, OSS_DOWNLOAD_STS_TOKEN_EXPIRE_SECONDS - 300)
OSS_DEFAULT_TIMEOUT_SECONDS = config.oss.oss_default_timeout_seconds
OSS_DELETE_OBJECT_TIMEOUT_SECONDS = config.oss.oss_delete_object_timeout_seconds
OSS_DELETE_PREFIX_TIMEOUT_SECONDS = config.oss.oss_delete_prefix_timeout_seconds
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


# 中转下载（relay）配置
OSS_INTERNAL_ENDPOINT = getattr(config.oss, "oss_internal_endpoint", None)
# token 有效期与 RAW 直链签名 URL 对齐，保证与缓存 TTL 的既有不变式一致：
# PACKUP_CACHE_TTL_SECONDS 恒早于本值过期，缓存里返回的 URL 不会指向已失效的 token
RELAY_TOKEN_EXPIRE_SECONDS = SIGNED_URL_EXPIRE_SECONDS
RELAY_MAX_CONCURRENCY = int(getattr(config.oss, "relay_max_concurrency", 5))
RELAY_ACQUIRE_TIMEOUT = float(getattr(config.oss, "relay_acquire_timeout_seconds", 10))
RELAY_IDLE_TIMEOUT = float(getattr(config.oss, "relay_idle_timeout_seconds", 30))


__all__ = [
    "BYTES_IN_GB",
    "ALLOWED_STORAGE_TYPES",
    "ALLOWED_FILE_TYPES",
    "ALLOWED_EXPERIMENT_TYPES",
    "MAX_FILENAME_LENGTH",
    "PREFLIGHT_TTL_MINUTES",
    "MAX_SINGLE_FILE_SIZE_BYTES",
    "CLEANUP_PREFLIGHTS_INTERVAL_MINUTES",
    "DOWNLOAD_ZIP_TTL_MINUTES",
    "CLEANUP_DOWNLOAD_ZIP_INTERVAL_MINUTES",
    "STALE_PROCESS_RUN_TIMEOUT_MINUTES",
    "CLEANUP_STALE_PROCESS_RUNS_INTERVAL_MINUTES",
    "STALE_CLUSTERING_RUN_TIMEOUT_MINUTES",
    "SIGNED_URL_EXPIRE_SECONDS",
    "PACKUP_CACHE_TTL_SECONDS",
    "OSS_UPLOAD_STS_TOKEN_EXPIRE_SECONDS",
    "OSS_DOWNLOAD_STS_TOKEN_EXPIRE_SECONDS",
    "ZARR_STS_CACHE_TTL_SECONDS",
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
    "OSS_INTERNAL_ENDPOINT",
    "RELAY_TOKEN_EXPIRE_SECONDS",
    "RELAY_MAX_CONCURRENCY",
    "RELAY_ACQUIRE_TIMEOUT",
    "RELAY_IDLE_TIMEOUT",
    "OSS_DEFAULT_TIMEOUT_SECONDS",
    "OSS_DELETE_OBJECT_TIMEOUT_SECONDS",
    "OSS_DELETE_PREFIX_TIMEOUT_SECONDS",
    "OSS_REGION_ID",
    "OSS_BUCKET_NAME",
]
