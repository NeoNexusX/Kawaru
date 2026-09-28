from typing import Annotated, Any

from fastapi import HTTPException, Depends
from starlette import status

from tools import utc_time
from tools.logger import get_logger
from boot.config import get_config
from models.email_models import (
EmailConfirmResponseBase,
EmailSMTPRequest,
EmailPurpose)
from database.db import AsyncSessionDep
from api_helpers.email_template import render_email
from api_services.user_service import get_user_by_email, generate_email_code, enforce_send_cooldown
from background.api_background import send_smtp_email_task
from celery_runtime.result import wait_for_result
from exceptions.user_exceptions import email_already_exists_exception
from exceptions.utils_exception import format_exception_detail

logger = get_logger("api.email-api")
config = get_config()
email_config = config.email

email_send_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Send Failed error by server reason")
)

# Using SMTP to send an email verify code message
async def send_smtp_email(email_info: EmailConfirmResponseBase, db: AsyncSessionDep):
    """generate code"""

    # 发送侧统一限流：同一邮箱不区分用途，冷却窗口内只放行一次（置于一切查询/发码之前）
    await enforce_send_cooldown(email_info.email)

    # 验证邮箱是否已被注册（仅在注册时）
    if email_info.purpose == EmailPurpose.REGISTER:
        existing_user = await get_user_by_email(email_info.email, db)
        if existing_user:
            raise email_already_exists_exception

    # 密码重置：邮箱不存在或账号未激活时静默返回成功（不发码）：既防邮箱枚举，又禁止未激活账号重置。
    username: str | None = None
    if email_info.purpose == EmailPurpose.RESET_PASSWORD:
        existing_user = await get_user_by_email(email_info.email, db)
        if existing_user is None or not existing_user.active:
            return EmailConfirmResponseBase(
                email=email_info.email,
                send_time=utc_time(),
                purpose=email_info.purpose,
            )
        username = existing_user.username  # 正文提示用户名，便于忘记用户名的用户重置

    # 注册 / 更新邮箱 / 密码重置统一：验证码与用途 + 邮箱绑定存入 Redis，单次有效
    code = await generate_email_code(email_info.purpose, email_info.email)

    # 按用途渲染主题与正文
    content = render_email(email_info.purpose, code, username)

    # smtp server
    request_data = EmailSMTPRequest.model_validate({
        **email_info.model_dump(),
        "smtp_username": email_config.smtp_username,
        "smtp_from_email": email_config.smtp_from_email,
        "smtp_server": email_config.smtp_server,
        "smtp_port": int(email_config.smtp_port),
        "subject": content.subject,
        "text_content": content.text_content,
    })

    task_result = send_smtp_email_task.delay(request_data.model_dump()) # type: ignore
    response: dict[str, Any] | None = await wait_for_result(task_result, poll_interval=1, timeout=10)
    if not response or not response.get("success"):
        raise email_send_exception
    # 任务只返回发送状态；响应直接用请求参数（含 email/send_time/purpose），不再经任务回传
    return email_info

EmailConfirmSMTPDep = Annotated[EmailConfirmResponseBase, Depends(send_smtp_email)]
