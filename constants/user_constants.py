"""存储和用户相关的常量定义"""
import json
from boot.config import REGION_PATH, get_config

config = get_config()
user_config = config.user

# 允许的用户职位列表
ALLOWED_POSITION_TYPES = set(user_config.allowed_position_types)

# 允许的国家/地区列表（ISO 3166-1 alpha-2代码，如CN、US）
ALLOWED_REGIONS = {
    item['iso2'] for item in
    json.loads(REGION_PATH.read_text(encoding="utf-8"))
}

# 无操作后登录自动过期时间
LOGIN_EXPIRE_SECONDS = config.token.login_expire_minutes * 60


__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "ALLOWED_POSITION_TYPES",
    "ALLOWED_REGIONS",
    "LOGIN_EXPIRE_SECONDS",
]
