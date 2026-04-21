import os
from typing import Annotated

from fastapi import HTTPException, Depends
from starlette import status

from tools.logger import get_logger
from boot.config import get_config
from models.email_models import (
EmailConfirmResponseBase,
EmailConfirmRequest, EmailSMTPRequest,
TOTP, EmailPurpose, MailgunEmailPayload)
from database.db import AsyncSessionDep
from api.user_api import get_user_by_email
from background.api_background import send_mailgun_message_task, send_smtp_email_task
from background.celery_app import wait_for_result
from exceptions.user_exceptions import email_already_exists_exception
from exceptions.utils_exception import format_exception_detail

logger = get_logger("api.email-api")
config = get_config()
email_config = config.email

email_send_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Send Failed error by server reason")
)

email_server_exception = HTTPException(
    status_code=status.HTTP_502_BAD_GATEWAY,
    detail=format_exception_detail("Upstream service error")
)

# Using MailGun to send an email verify code message
async def send_mailgun_message(email_info: EmailConfirmResponseBase, db: AsyncSessionDep):
    """
    use MailGun API to send email, which is more reliable and faster than SMTP, but may have some limitations 
    """
    if email_info.purpose not in (EmailPurpose.REGISTER, EmailPurpose.UPDATE):
        raise email_send_exception

    # 验证邮箱是否已被注册（仅在注册时）
    if email_info.purpose == EmailPurpose.REGISTER:
        existing_user = await get_user_by_email(email_info.email, db)
        if existing_user:
            raise email_already_exists_exception

    code = TOTP.now() # generate one-time code

    request_data = EmailConfirmRequest.model_validate(
        {**email_info.model_dump(),
         "text_content": f"Welcome\r\n Your code is {code}"}
    )

    mailgun_api_key = os.getenv('MAILGUN_API_KEY')
    if not mailgun_api_key:
        raise email_send_exception

    mailgun_payload = MailgunEmailPayload.model_validate(
        {
            **email_info.model_dump(),
            "from_": f"Mailgun Sandbox <postmaster@{email_config.mailgun_domain}>",
            "to": request_data.email,
            "subject": request_data.subject,
            "text": request_data.text_content,
            "mailgun_api_key": mailgun_api_key,
            "mailgun_api_url": f"https://api.mailgun.net/v3/{email_config.mailgun_domain}/messages"
        }
    )

    task_result = send_mailgun_message_task.delay(mailgun_payload.model_dump()) # type: ignore
    response = await wait_for_result(task_result, poll_interval=0.1)
    if not response or not response.get("success"):
        raise email_send_exception
    return EmailConfirmResponseBase.model_validate(response["data"])
# Using SMTP to send an email verify code message
async def send_smtp_email(email_info: EmailConfirmResponseBase, db: AsyncSessionDep):
    """generate code"""
    if email_info.purpose not in (EmailPurpose.REGISTER, EmailPurpose.UPDATE):
        raise email_send_exception

    # 验证邮箱是否已被注册（仅在注册时）
    if email_info.purpose == EmailPurpose.REGISTER:
        existing_user = await get_user_by_email(email_info.email, db)
        if existing_user:
            raise email_already_exists_exception

    code = TOTP.now()

    # smtp server
    request_data = EmailSMTPRequest.model_validate({
        **email_info.model_dump(),
        "smtp_username": email_config.smtp_username,
        "smtp_passwd": os.getenv('SMTP_PASSWORD'),
        "smtp_from_email": email_config.smtp_from_email,
        "smtp_server": email_config.smtp_server,
        "smtp_port": int(email_config.smtp_port),
        "text_content": f"Welcome\r\n Your code is {code}"
    })

    task_result = send_smtp_email_task.delay(request_data.model_dump()) # type: ignore
    response = await wait_for_result(task_result, poll_interval=0.1)
    if not response or not response.get("success"):
        raise email_send_exception
    return EmailConfirmResponseBase.model_validate(response["data"])

EmailConfirmSMTPDep = Annotated[EmailConfirmResponseBase, Depends(send_smtp_email)]
EmailConfirmDep = Annotated[EmailConfirmResponseBase, Depends(send_mailgun_message)]
