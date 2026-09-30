#########################
# Admin Notify Helper - admin failure alert emails (fire-and-forget)
# 职责：面向管理员的失败告警邮件构造与投递，与用户侧验证码邮件
#（email_template.py）相互独立。无 DB、无 FastAPI 依赖，API 层与
# background 任务均可复用。
# 投递只把任务送进 broker，不等待 SMTP 发送结果；收件人取 admin_info.email；
# 复用 send_smtp_email_task（SMTP 密码仍从环境变量 SMTP_PASSWORD 读取）。
# 告警是旁路能力：任何失败只记日志，绝不影响调用方主流程。
#########################
from boot.config import get_config
from background.api_background import send_smtp_email_task
from tools.logger import get_logger

logger = get_logger("api_helpers.admin_notify")


def _send_admin_alert(subject: str, text_content: str) -> bool:
    """Dispatch an admin alert email. Best-effort: any failure is logged only."""
    config = get_config()
    recipient = config.admin_info.email
    if not recipient:
        logger.warning("admin alert skipped: admin_info.email not configured subject=%s", subject)
        return False
    email_config = config.email
    payload = {
        "email": recipient,
        "subject": subject,
        "text_content": text_content,
        "smtp_username": email_config.smtp_username,
        "smtp_from_email": email_config.smtp_from_email,
        "smtp_server": email_config.smtp_server,
        "smtp_port": int(email_config.smtp_port),
    }
    try:
        send_smtp_email_task.delay(payload)
        logger.info("admin alert dispatched subject=%s recipient=%s", subject, recipient)
        return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("admin alert dispatch failed subject=%s error=%s", subject, exc)
        return False


def notify_delete_failed(kind: str, resource_id: int) -> bool:
    """
    Alert admins that an OSS deletion failed and the row was marked DELETE_FAILED.

    调用约束（删除架构现状说明 §8 的通知设计结论）：必须挂在
    「DELETING → DELETE_FAILED」状态跃迁上（见 _mark_delete_failed）——
    重投时行已是 DELETE_FAILED，不再调用本函数，跃迁即天然去重，避免重投轰炸。
    """
    app_name = get_config().server.name
    subject = f"[{app_name}] Deletion failed alert - {kind}#{resource_id} marked DELETE_FAILED"
    text_content = (
        f"{app_name} admin alert\r\n\r\n"
        f"OSS deletion failed. The following resource was marked DELETE_FAILED "
        f"(row kept for retry):\r\n"
        f"  Kind: {kind}\r\n"
        f"  ID: {resource_id}\r\n\r\n"
        "How to handle: re-invoke the corresponding delete API to re-dispatch "
        "(idempotent).\r\n"
        "For persistent failures, check OSS availability and worker logs "
        "(delete_objects_task).\r\n"
    )
    return _send_admin_alert(subject, text_content)


def notify_fc_trigger_failed(
    fc_name: str,
    *,
    run_id: int | None = None,
    file_id: int | None = None,
) -> bool:
    """
    Alert admins that an FC trigger failed (invoke_fc returned None): the run
    stays PROCESSING until cleanup_stale_process_runs marks it FAILED.

    触发任务无自动重试（一次用户操作至多一次投递），无需跃迁去重。
    """
    app_name = get_config().server.name
    target = f"run_id={run_id}" if run_id is not None else f"file_id={file_id}"
    subject = f"[{app_name}] FC trigger failed alert - {fc_name} ({target})"
    text_content = (
        f"{app_name} admin alert\r\n\r\n"
        f"FC function trigger failed (invoke_fc returned None):\r\n"
        f"  FC function: {fc_name}\r\n"
        f"  Target: {target}\r\n\r\n"
        "Impact: the run stays PROCESSING until cleanup_stale_process_runs marks "
        "it FAILED.\r\n"
        "Check the FC endpoint config, token env vars and function logs "
        "(background/processing_background).\r\n"
    )
    return _send_admin_alert(subject, text_content)
