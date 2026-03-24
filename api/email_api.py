import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Annotated

import httpx
from fastapi import HTTPException, Depends
from starlette import status

from tools.logger import get_logger
from boot.config import get_config
from models.email_models import EmailConfirmResponseBase, EmailConfirmRequest, EmailSMTPRequest, TOTP
from database.db import AsyncSessionDep
from api.user_api import get_user_by_email
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
    # 验证邮箱是否已被注册（仅在注册时）
    if email_info.purpose == "register":
        existing_user = await get_user_by_email(email_info.email, db)
        if existing_user:
            raise email_already_exists_exception

    # MailGun environment settings
    mailgun_api_key = os.getenv('MAILGUN_API_KEY')

    if not mailgun_api_key:
        logger.error("MAILGUN_API_KEY environment variable not set")
        raise email_send_exception

    mailgun_domain = email_config.mailgun_domain
    mailgun_api_url = f"https://api.mailgun.net/v3/{mailgun_domain}/messages"
    ##############################

    code = TOTP.now() # generate one-time code

    request_data = EmailConfirmRequest.model_validate(
        {**email_info.model_dump(),
         "text_content": f"Welcome\r\n Your code is {code}"}
    )

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                mailgun_api_url,
                auth=("api", mailgun_api_key),
                # Contents of email
                data={
                    "from": f"Mailgun Sandbox <postmaster@{mailgun_domain}>", # sender(server)'s info
                    "to": request_data.email, # email address of receiver(user)
                    "subject": request_data.subject, # subject of the email
                    "text": request_data.text_content # text content of the email
                }
            )
            response.raise_for_status()
            return request_data

        except httpx.HTTPStatusError as e:
            logger.error(e)
            raise email_server_exception from e

        except Exception as e:
            logger.error(e)
            raise email_send_exception from e

# Using SMTP to send an email verify code message
async def send_smtp_email(email_info: EmailConfirmResponseBase, db: AsyncSessionDep):
    """generate code"""
    # 验证邮箱是否已被注册（仅在注册时）
    if email_info.purpose == "register":
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

    logger.info(f'request_data is {request_data}')

    # 构造邮件内容
    msg = EmailMessage()
    msg["From"] = request_data.smtp_from_email # sender(server)'s email
    msg["To"] = request_data.email # user(receiver)'s email
    msg["Subject"] = request_data.subject # the subject of the email
    msg.set_content(request_data.text_content) # the content of the emai(verify code here)
    context = ssl.create_default_context() # prepare the ssl context for smtp connection

    try:

        # SMTP+STARTTLS protocol:
        if request_data.smtp_port == 587:
            # asny step use thread pool
            with smtplib.SMTP(request_data.smtp_server, request_data.smtp_port, timeout=10) as server:
                logger.info(f'smtp port {request_data.smtp_port}')
                server.starttls(context=context)
                server.login(request_data.smtp_username, request_data.smtp_passwd)
                server.send_message(msg)
            return request_data

        # SMTPS protocol:
        else:
            with smtplib.SMTP_SSL(request_data.smtp_server,
                                  request_data.smtp_port,
                                  timeout=10,
                                  context=context) as server:
                logger.info(f'smtp port {request_data.smtp_port}')
                server.login(request_data.smtp_username, request_data.smtp_passwd)
                server.send_message(msg)
            return request_data

    except smtplib.SMTPResponseException as e:
        if 'qq.com' in request_data.smtp_from_email and e.smtp_code == -1:
            ######################################################################
            # Workaround for QQ Mail: occasionally their SMTP server returns a
            # non-RFC5321 response after QUIT (e.g. empty line, missing 221 code).
            # smtplib fails to parse it and sets smtp_code = -1.
            # The message has already been accepted, so we treat this as a benign
            # exception and suppress the 502 error
            ######################################################################
            logger.error(f"SMTP error: {str(e)}")
            logger.warning("SMTP server returned non-standard response during QUIT.")
            return request_data
        logger.error(f"SMTP error: {str(e)}")

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=format_exception_detail(f"SMTP server error: {str(e)}")
        ) from e

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise email_send_exception from e

EmailConfirmSMTPDep = Annotated[EmailConfirmResponseBase, Depends(send_smtp_email)]
EmailConfirmDep = Annotated[EmailConfirmResponseBase, Depends(send_mailgun_message)]
