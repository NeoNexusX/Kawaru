"""存储和实验相关的常量定义"""
from pathlib import Path
from boot.config import get_config

# 加载配置
config = get_config()
storage_config = config.storage

# 存储路径常量
STORAGE_ROOT = Path(storage_config.root)
EXPERIMENT_STORAGE_ROOT = STORAGE_ROOT / "experiments"
SOURCE_FILE_PATH_CLEARED = "<SOURCE_PATH_CLEARED_AFTER_PROCESS>" # 占位常量：用于标记已删除但保留占位符的 source_file.path 值

# 清理相关的 TTL 设置（分钟）
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))

__all__ = [
    "STORAGE_ROOT",
    "EXPERIMENT_STORAGE_ROOT",
    "FAILED_EXPERIMENT_TTL_MINUTES",
]
