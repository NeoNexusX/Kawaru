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

# 邮箱验证码有效期（秒），注册/改邮箱/重置密码共用；与邮件正文提示的 5 分钟保持一致（见 api_helpers/email_template.py）
EMAIL_CODE_TTL_SECONDS = 300

# 邮件类别（send_smtp_email_task 的 payload["category"]，缺省视为验证码）：验证码与管理员告警
# 共用同一 SMTP 账号但限流策略不同——验证码走「每邮箱冷却」，告警走额度闸（api_helpers/alert_budget，
# 超限丢弃）；验证码类绝不能加全局额度，否则并发注册这类正常突发会被误伤。
EMAIL_CATEGORY_CODE = "code"
EMAIL_CATEGORY_ALERT = "alert"

# ORCID 校验的时间预算（秒）：任务内 HTTP 超时必须明显小于调用方预算（= HTTP 超时 + 入队/
# worker 取任务余量），否则 20-30s 的慢响应会被调用方先放弃，合法 ORCID 被误判成「不存在」。
# ORCID /person 正常 1-2s 返回，10s 已等价故障；不变式由 tests/test_orcid_timeout_unit.py 钉住。
ORCID_HTTP_TIMEOUT_SECONDS = 10
ORCID_QUEUE_SLACK_SECONDS = 10
ORCID_VALIDATE_TIMEOUT_SECONDS = ORCID_HTTP_TIMEOUT_SECONDS + ORCID_QUEUE_SLACK_SECONDS

# 软删除用户收敛任务的扫描间隔（分钟）：名下资源清完后由它物理删 user 行
CLEANUP_DELETED_USERS_INTERVAL_MINUTES = int(getattr(user_config, "cleanup_deleted_users_interval_minutes", 10))


__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "ALLOWED_POSITION_TYPES",
    "ALLOWED_REGIONS",
    "CLEANUP_DELETED_USERS_INTERVAL_MINUTES",
    "EMAIL_CATEGORY_ALERT",
    "EMAIL_CATEGORY_CODE",
    "EMAIL_CODE_TTL_SECONDS",
    "LOGIN_EXPIRE_SECONDS",
    "ORCID_HTTP_TIMEOUT_SECONDS",
    "ORCID_QUEUE_SLACK_SECONDS",
    "ORCID_VALIDATE_TIMEOUT_SECONDS",
]
