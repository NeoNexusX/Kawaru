#########################
# Alert Budget Helper - 管理员告警邮件额度闸
# 职责：告警与验证码共用同一 SMTP 账号（服务商按账号限流），告警侧在这里做全局额度
# 计数——固定窗口内超过 email.alert_max_per_window 的告警直接丢弃，确保告警洪峰
# （FC 端点挂掉、OSS 批量删除失败）不会把验证码邮件挤掉。只对告警生效，验证码不受
# 影响（它有自己的每邮箱冷却，见 api_services.user_service.enforce_send_cooldown）。
# 无 DB、无 FastAPI 依赖；Redis 查不动（超时/异常）时 fail-open（放行）——告警是旁路
# 能力，任何失败只记日志，绝不影响调用方主流程。
#########################
import time

from boot.config import get_config
from database.redis_client import connect_new_redis
from tools.logger import get_logger

logger = get_logger("api_helpers.alert_budget")


async def alert_budget_available() -> bool:
    """告警发信是否还有额度：固定窗口计数，超限返回 False 并记 WARN。

    每次调用建短连接：worker 里每个任务由 asyncio.run() 新建事件循环执行
    （celery_runtime.app.CeleryAsyncTask），模块级 redis_client 的连接会跨 loop
    复用报错，不能借来用；告警量级很低，短连接开销可忽略。
    """
    config = get_config()
    email_cfg = config.email
    window_seconds = email_cfg.alert_budget_window_seconds
    client = None
    try:
        client = connect_new_redis(db_index=config.redis.default_db_index)
        # key 带窗口序号：即使 expire 没赶上（进程被杀），下个窗口换新 key，不会永久封死
        key = f"email_alert_budget:{int(time.time()) // window_seconds}"
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, window_seconds)
        if count > email_cfg.alert_max_per_window:
            logger.warning(
                "admin alert suppressed by budget: count=%s limit=%s window_seconds=%s",
                count, email_cfg.alert_max_per_window, window_seconds,
            )
            return False
        return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("alert budget check failed, fail-open: %s", exc)
        return True
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("failed to close alert budget redis client: %s", exc)
