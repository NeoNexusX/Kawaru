"""存储和实验相关的常量定义"""
from pathlib import Path
from boot.config import get_config

# 加载配置
config = get_config()
storage_config = config.storage

# 存储路径常量
STORAGE_ROOT = Path(storage_config.root)
EXPERIMENT_STORAGE_ROOT = STORAGE_ROOT / "experiments"


# 清理相关的 TTL 设置（分钟）
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))

# 文件路径清理标记常量
SOURCE_FILE_PATH_CLEARED = "<SOURCE_PATH_CLEARED_AFTER_PROCESS>"

__all__ = [
    "STORAGE_ROOT",
    "EXPERIMENT_STORAGE_ROOT",
    "FAILED_EXPERIMENT_TTL_MINUTES",
    "SOURCE_FILE_PATH_CLEARED",
]
