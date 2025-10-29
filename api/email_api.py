import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Annotated

import httpx
from fastapi import HTTPException, Depends
from starlette import status

from logger import get_logger
from models.email_models import EmailConfirmResponseBase, EmailConfirmRequest, EmailSMTPRequest, TOTP

logger = get_logger("main.email-api")

email_send_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Send Failed error by server reason",
)


async def send_mailgun_message(email_info: EmailConfirmResponseBase):
    MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")
    MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
    MAILGUN_API_URL = f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages"
    code = TOTP.now()

    request_data = EmailConfirmRequest.model_validate(
        {**email_info.model_dump(),
         "text_content": f"Welcome\r\n Your code is {code}"}
    )
    logger.info(f"Request data: {request_data}")
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                MAILGUN_API_URL,
                auth=("api", MAILGUN_API_KEY),
                data={
                    "from": f"Mailgun Sandbox <postmaster@{MAILGUN_DOMAIN}>",
                    "to": request_data.email,
                    "subject": request_data.subject,
                    "text": request_data.text_content
                }
            )
            response.raise_for_status()
            return request_data

        except httpx.HTTPStatusError as e:
            logger.error(e)
            raise HTTPException(
                status_code=e.response.status_code,
                detail=e.response.text
            )
        except Exception as e:
            logger.error(e)
            raise email_send_exception


async def send_smtp_email(email_info: EmailConfirmResponseBase):
    """generate code"""
    code = TOTP.now()

    """smtp server"""
    request_data = EmailSMTPRequest.model_validate({
        **email_info.model_dump(),
        "last_smtp_username": os.getenv('LAST_SMTP_USERNAME'),
        "last_smtp_passwd": os.getenv('LAST_SMTP_PASSWORD'),
        "last_smtp_from_email": os.getenv('LAST_SMTP_FROM_EMAIL'),
        "last_smtp_server": os.getenv('LAST_SMTP_SERVER'),
        "last_smtp_port": int(os.getenv('LAST_SMTP_PORT')),
        "text_content": f"Welcome\r\n Your code is {code}"
    }
    )
    logger.info(f'request_data is {request_data}')
    # 构造邮件内容
    msg = EmailMessage()
    msg["From"] = request_data.last_smtp_from_email
    msg["To"] = request_data.email
    msg["Subject"] = request_data.subject
    msg.set_content(request_data.text_content)
    context = ssl.create_default_context()

    try:
        if request_data.last_smtp_port == 587:
            # asny step use thread pool
            with smtplib.SMTP(request_data.last_smtp_server, request_data.last_smtp_port, timeout=10) as server:
                logger.info(f'smtp port {request_data.last_smtp_port}')
                server.starttls(context=context)
                server.login(request_data.last_smtp_username, request_data.last_smtp_passwd)
                server.send_message(msg)
            return request_data
        else:
            with smtplib.SMTP_SSL(request_data.last_smtp_server,
                                  request_data.last_smtp_port,
                                  timeout=10,
                                  context=context) as server:
                logger.info(f'smtp port {request_data.last_smtp_port}')
                server.login(request_data.last_smtp_username, request_data.last_smtp_passwd)
                server.send_message(msg)
            return request_data

    except smtplib.SMTPException as e:
        if 'qq.com' in request_data.last_smtp_from_email and e.smtp_code == -1:
            logger.error(f"SMTP error: {str(e)}")
            logger.warning("SMTP server returned non-standard response during QUIT.")
            return request_data
        logger.error(f"SMTP error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"SMTP server error: {str(e)}"
        )

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise email_send_exception

EmailConfirmSMTPDep = Annotated[EmailConfirmResponseBase, Depends(send_smtp_email)]
EmailConfirmDep = Annotated[EmailConfirmResponseBase, Depends(send_mailgun_message)]
