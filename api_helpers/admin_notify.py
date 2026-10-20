#########################
# Admin Notify Helper - admin failure alert emails (fire-and-forget)
# 职责：面向管理员的失败告警邮件构造与投递，与用户侧验证码邮件
#（email_template.py）相互独立。无 DB、无 FastAPI 依赖，API 层与
# background 任务均可复用。
# 投递只把任务送进 broker，不等待 SMTP 发送结果；收件人取 admin_info.email；
# 复用 send_smtp_email_task（SMTP 密码仍从环境变量 SMTP_PASSWORD 读取）。
# payload 带 category=alert：共享发件任务据此过「告警额度闸」（api_helpers.alert_budget，
# 额度见 email.alert_max_per_window / alert_budget_window_seconds），超限丢弃——告警洪峰
# 不得挤掉用户验证码邮件。
# 告警是旁路能力：任何失败只记日志，绝不影响调用方主流程。
#########################
from boot.config import get_config
from background.api_background import send_smtp_email_task
from constants.user_constants import EMAIL_CATEGORY_ALERT
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
        # 类别标记：共享发件任务据此走告警额度闸（验证码不带此键，走默认 code）
        "category": EMAIL_CATEGORY_ALERT,
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


def notify_delete_failed(
    kind: str,
    resource_id: int,
    *,
    public_id: str | None = None,
) -> bool:
    """
    Alert admins that an OSS deletion failed and the row was marked DELETE_FAILED.

    file 类必须同时带上 public_id：对外删除接口走 DELETE /files/{public_id}，
    file_id 只是内部自增标识（可枚举，不对外），只给 file_id 管理员无法照着
    邮件重投。run 类反过来——DELETE /processes/{run_id} 用的就是 run_id；但该接口
    无 admin 旁路（assert_run_ownership 只认 owner），管理员实际无法重投 run 类失败，
    邮件正文已按此写明（不要给出管理员照做会被拒的指令）。

    调用约束：必须挂在「DELETING → DELETE_FAILED」状态跃迁上（见
    _mark_delete_failed）——重投时行已是 DELETE_FAILED，不再调用本函数，
    跃迁即天然去重，避免重投轰炸。
    """
    app_name = get_config().server.name
    if kind == "file":
        identifiers = (
            f"  file_id   (internal, for DB / worker log lookup): {resource_id}\r\n"
            f"  public_id (对外标识，删除接口用这个):              {public_id or '<unknown>'}\r\n"
        )
        api_hint = f"DELETE /files/{public_id or '<public_id>'}"
        subject_target = f"file#{resource_id}"
        if public_id:
            subject_target += f" public_id={public_id}"
        # file 类有 admin 旁路（assert_file_owner 放行 admin），管理员照 api_hint 重投即可
        handling = (
            "How to handle: re-invoke the delete API to re-dispatch (idempotent):\r\n"
            f"  {api_hint}\r\n"
        )
    elif kind == "run":
        identifiers = f"  run_id (删除接口用这个): {resource_id}\r\n"
        api_hint = f"DELETE /processes/{resource_id}"
        subject_target = f"run#{resource_id}"
        # run 类删除无 admin 旁路（assert_run_ownership 只认 owner）：管理员执行
        # api_hint 会被拒，只能请 owner 重投——但兜底任务 retry_stale_run_deletions
        # 会在超时后自动重投，通常无需人工介入
        handling = (
            f"How to handle: {api_hint} is owner-only — run deletion has no admin "
            "bypass, so an admin cannot re-dispatch it. Normally no action is needed: "
            "the fallback task retry_stale_run_deletions re-dispatches stale deletions "
            "automatically. Otherwise ask the run's owner to re-invoke it.\r\n"
        )
    else:
        # 兜底：_mark_delete_failed 目前只会传 file / run，未知 kind 不应走到这里
        identifiers = f"  ID: {resource_id}\r\n"
        api_hint = "the corresponding delete API"
        subject_target = f"{kind}#{resource_id}"
        handling = (
            "How to handle: re-invoke the delete API to re-dispatch (idempotent):\r\n"
            f"  {api_hint}\r\n"
        )

    subject = f"[{app_name}] Deletion failed alert - {subject_target} marked DELETE_FAILED"
    text_content = (
        f"{app_name} admin alert\r\n\r\n"
        f"OSS deletion failed. The following resource was marked DELETE_FAILED "
        f"(row kept for retry):\r\n"
        f"  Kind: {kind}\r\n"
        f"{identifiers}\r\n"
        f"{handling}"
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
