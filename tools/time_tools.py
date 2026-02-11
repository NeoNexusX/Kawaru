"""Tools relevant to time operation"""

from datetime import datetime, timezone

# 将datetime类型序列化为str或反序列化的统一格式
TIME_STR_FORMAT = r"%Y-%m-%d %H:%M:%S"

def now_local_timezone() -> datetime:
    """返回格林威治时间（无时区信息，便于 SQLite 存储）"""
    return datetime.now(timezone.utc)
