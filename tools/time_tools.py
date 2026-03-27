"""Tools relevant to time operation"""

from datetime import datetime, timezone

# 将datetime类型序列化为str或反序列化的统一格式
TIME_STR_FORMAT = r"%Y-%m-%d %H:%M:%S"

def utc_time() -> datetime:
    """返回UTC时间（带时区信息，便于统一存储）"""
    return datetime.now(timezone.utc)
