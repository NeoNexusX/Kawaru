"""存储和实验相关的常量定义"""
from boot.config import get_config

# 加载配置
config = get_config()
storage_config = config.storage

OSS_PROCESSED_STORAGE_ROOT = config.oss.oss_processed_storage_root

# 失败实验清理策略
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))
CLEANUP_FAILED_EXPERIMENTS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_failed_experiments_interval_minutes", 60))


# 文件路径清理标记常量
SOURCE_FILE_PATH_CLEARED = "<SOURCE_PATH_CLEARED_AFTER_PROCESS>"

__all__ = [
    "FAILED_EXPERIMENT_TTL_MINUTES",
    "CLEANUP_FAILED_EXPERIMENTS_INTERVAL_MINUTES",
    "SOURCE_FILE_PATH_CLEARED",
    "OSS_PROCESSED_STORAGE_ROOT",
]
