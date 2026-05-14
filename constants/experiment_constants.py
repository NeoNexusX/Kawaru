"""存储和实验相关的常量定义"""
from pathlib import Path
from boot.config import get_config

# 加载配置
config = get_config()
storage_config = config.storage

# 存储路径常量
STORAGE_ROOT = Path(storage_config.root)
ORIGIN_STORAGE_ROOT = STORAGE_ROOT / "origin"
OSS_ORIGIN_STORAGE_ROOT = config.oss.oss_origin_storage_root

# 失败实验清理策略
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))
CLEANUP_FAILED_EXPERIMENTS_INTERVAL_MINUTES = int(getattr(storage_config, "cleanup_failed_experiments_interval_minutes", 60))


# 文件路径清理标记常量
SOURCE_FILE_PATH_CLEARED = "<SOURCE_PATH_CLEARED_AFTER_PROCESS>"

__all__ = [
    "STORAGE_ROOT",
    "ORIGIN_STORAGE_ROOT",
    "FAILED_EXPERIMENT_TTL_MINUTES",
    "CLEANUP_FAILED_EXPERIMENTS_INTERVAL_MINUTES",
    "SOURCE_FILE_PATH_CLEARED",
    "OSS_ORIGIN_STORAGE_ROOT",
]
