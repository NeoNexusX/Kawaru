import os
import smtplib
import ssl
from email.message import EmailMessage

import httpx

from api_helpers.alert_budget import alert_budget_available
from celery_runtime.state import get_celery_app
from constants.user_constants import EMAIL_CATEGORY_ALERT, ORCID_HTTP_TIMEOUT_SECONDS
from tools.logger import get_logger

logger = get_logger("background.api_background")
celery_app = get_celery_app()


@celery_app.task() # type: ignore
async def validate_orcid_task(orcid: str) -> bool:
    """
    校验 ORCID 的 Celery 任务。
    直接执行异步 HTTP 请求。
    """
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    headers = {'Accept': 'application/json'}

    async with httpx.AsyncClient(timeout=ORCID_HTTP_TIMEOUT_SECONDS) as client:
        try:
            response = await client.head(url, headers=headers)
            if response.status_code == 200:
                return True
            else:
                logger.error(f"ORCID validation failed for {orcid}: HTTP {response.status_code}")
                return False
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(f"ORCID validation failed for {orcid}: {str(e)}")
            return False


@celery_app.task()  # type: ignore
async def send_smtp_email_task(email_payload: dict) -> dict:
    """
    发送 SMTP 邮件的 Celery 任务。
    任务只负责发送邮件，不包含业务验证。
    告警类（payload["category"] == "alert"）先过额度闸（api_helpers.alert_budget），
    超限直接丢弃。
    """
    if email_payload.get("category") == EMAIL_CATEGORY_ALERT and not await alert_budget_available():
        # 有意丢弃：返回 success 表示这是终态、不要重试（告警本身是 fire-and-forget）
        return {"success": True, "suppressed": True}

    try:
        smtp_server = email_payload.get("smtp_server")
        smtp_port = int(email_payload.get("smtp_port", 0))
        smtp_username = email_payload.get("smtp_username")
        # 密码从环境变量读取，不随任务 payload 过 broker/结果存储
        smtp_passwd = os.getenv("SMTP_PASSWORD")
        smtp_from_email = email_payload.get("smtp_from_email")
        recipient = email_payload.get("email")
        subject = email_payload.get("subject")
        text_content = email_payload.get("text_content")

        msg = EmailMessage()
        msg["From"] = smtp_from_email
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.set_content(text_content, charset='utf-8')

        logger.debug(f"SMTP message sent to {recipient} with txt: {text_content}")

        context = ssl.create_default_context()
        if smtp_port == 587:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as server: # type: ignore
                server.starttls(context=context)
                server.login(smtp_username, smtp_passwd) # type: ignore
                server.send_message(msg)
        else:
            with smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=10, context=context) as server: # type: ignore
                server.login(smtp_username, smtp_passwd) # type: ignore
                server.send_message(msg)

        return {"success": True}

    except smtplib.SMTPResponseException as exc:
        # 判定对象应是 SMTP 服务器主机名而非发件人域名（发件人可配自有域名）
        if "qq.com" in (email_payload.get("smtp_server") or "") and getattr(exc, "smtp_code", None) == -1:
            logger.warning("SMTP non-standard quit response suppressed: %s", exc)
            return {"success": True}
        logger.error("SMTP response error: %s", exc)
        return {"success": False, "error": str(exc)}
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("SMTP send failed: %s", exc)
        return {"success": False, "error": str(exc)}
