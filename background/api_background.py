import asyncio
from typing import Dict, Any, Optional
import smtplib
import ssl
from email.message import EmailMessage

import httpx
from httpx import RequestError
from starlette import status

from background.celery_app import celery_app
from tools.logger import get_logger

logger = get_logger("background.api_background")


@celery_app.task()  # type: ignore
async def generic_http_request_task(# TODO 暂时没用上，写成类 ，先全部写出来普通的task，然后再合并
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 3,
) -> Dict[str, Any]:  # pyright: ignore[reportReturnType]
    """
    通用异步 HTTP 请求任务。
    返回 {'success': bool, 'status_code': int, 'data': Any 或 'error': str}
    """
    retry_status_codes = {429, 500, 502, 503, 504}

    for attempt in range(max_retries + 1):
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                if method.upper() == "GET":
                    response = await client.get(url, headers=headers or {})
                elif method.upper() == "POST":
                    response = await client.post(url, headers=headers or {}, json=data or {})
                elif method.upper() == "HEAD":
                    response = await client.head(url, headers=headers or {})
                else:
                    return {"success": False, "error": f"Unsupported method: {method}"}

                if response.status_code == status.HTTP_200_OK:
                    return {"success": True,"status_code": response.status_code,"data": response.text,}

                if response.status_code in retry_status_codes and attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue

                return {
                    "success": False,"status_code": response.status_code,"error": f"HTTP {response.status_code}",}

            except RequestError as e:
                logger.warning(
                    "HTTP request failed for %s (attempt %s/%s): %s",url,attempt + 1,max_retries + 1,
                    e,
                )
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"success": False, "error": str(e)}

            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(f"Unexpected error in HTTP request for {url}: {e}")
                return {"success": False, "error": str(e)}


@celery_app.task() # type: ignore
async def validate_orcid_task(orcid: str) -> bool:
    """
    校验 ORCID 的 Celery 任务。
    直接执行异步 HTTP 请求。
    """
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    headers = {'Accept': 'application/json'}

    async with httpx.AsyncClient(timeout=30) as client:
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
async def send_mailgun_message_task(email_payload: dict) -> dict:
    """
    发送 Mailgun 邮件的 Celery 任务。
    任务只负责发送邮件，不包含业务验证。
    """

    mailgun_api_key = email_payload.get("mailgun_api_key")
    mailgun_api_url = email_payload.get("mailgun_api_url") #TODO 是否考虑直接从env获取？

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                mailgun_api_url, # type: ignore
                auth=("api", mailgun_api_key), # type: ignore
                data={
                    "from": email_payload.get("from_"),
                    "to": email_payload.get("to"),
                    "subject": email_payload.get("subject"),
                    "text": email_payload.get("text"),
                },
            )
            response.raise_for_status()
            return {
                "success": True,
                "data": {
                    "email": email_payload.get("email") or email_payload.get("to"),
                    "send_time": email_payload.get("send_time"),
                    "purpose": email_payload.get("purpose"),
                },
            }
    except httpx.HTTPStatusError as exc:
        logger.error("Mailgun HTTP error: %s", exc)
        return {"success": False, "error": f"HTTP {exc.response.status_code}: {exc.response.text}"}
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("Mailgun send failed: %s", exc)
        return {"success": False, "error": str(exc)}


@celery_app.task()  # type: ignore
async def send_smtp_email_task(email_payload: dict) -> dict:
    """
    发送 SMTP 邮件的 Celery 任务。
    任务只负责发送邮件，不包含业务验证。
    """

    try:
        smtp_server = email_payload.get("smtp_server")
        smtp_port = int(email_payload.get("smtp_port", 0))
        smtp_username = email_payload.get("smtp_username")
        smtp_passwd = email_payload.get("smtp_passwd")
        smtp_from_email = email_payload.get("smtp_from_email")
        recipient = email_payload.get("email")
        subject = email_payload.get("subject")
        text_content = email_payload.get("text_content")

        msg = EmailMessage()
        msg["From"] = smtp_from_email
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.set_content(text_content, charset='utf-8')

        logger.debug("SMTP message to send: %s", msg.as_string())

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

        return {"success": True, "data": email_payload}
    except smtplib.SMTPResponseException as exc:
        if "qq.com" in email_payload.get("from", "") and getattr(exc, "smtp_code", None) == -1:
            logger.warning("SMTP non-standard quit response suppressed: %s", exc)
            return {"success": True, "data": email_payload}
        logger.error("SMTP response error: %s", exc)
        return {"success": False, "error": str(exc)}
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error("SMTP send failed: %s", exc)
        return {"success": False, "error": str(exc)}
