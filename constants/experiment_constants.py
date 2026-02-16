"""存储和实验相关的常量定义"""
from pathlib import Path
from envset.config import get_config

# 加载配置
config = get_config()
storage_config = config.storage

# 存储路径常量
STORAGE_ROOT = Path(storage_config.root)
EXPERIMENT_STORAGE_ROOT = STORAGE_ROOT / "experiments"
EXPERIMENT_TEMP_ROOT = EXPERIMENT_STORAGE_ROOT / "temp"  # 【临时目录】experiments/temp/exp_{id}
EXPERIMENT_PROCESSED_ROOT = EXPERIMENT_STORAGE_ROOT / "processed"  # 【正式目录】experiments/processed/exp_{id}
SOURCE_FILE_PATH_CLEARED = "<SOURCE_PATH_CLEARED>" # 占位常量：用于标记已删除但保留占位符的 source_file.path 值

# 清理相关的 TTL 设置（分钟）
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))

__all__ = [
    "STORAGE_ROOT",
    "EXPERIMENT_STORAGE_ROOT",
    "EXPERIMENT_TEMP_ROOT",
    "EXPERIMENT_PROCESSED_ROOT",
    "FAILED_EXPERIMENT_TTL_MINUTES",
]
