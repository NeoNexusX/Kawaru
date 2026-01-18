"""存储和用户相关的常量定义"""
import json
from pathlib import Path
from envset.config import REGION_PATH, get_config

config = get_config()
user_config = config.user

# 允许的用户职位列表
ALLOWED_POSITION_TYPES = set(user_config.allowed_position_types)

# 允许的国家/地区列表
ALLOWED_REGIONS = {
    item['name'] for item in
    json.loads(REGION_PATH.read_text(encoding="utf-8"))
}

__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "ALLOWED_POSITION_TYPES",
    "ALLOWED_REGIONS",

]