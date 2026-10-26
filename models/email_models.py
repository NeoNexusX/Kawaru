from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class EmailPurpose(str, Enum):
    """邮件发送目的枚举"""
    REGISTER = "register"
    UPDATE = "update"
    RESET_PASSWORD = "reset_password"  # 忘记密码：未登录用户凭邮箱重置密码


class EmailConfirmResponseBase(BaseModel):
    """邮件确认相关的基础模型"""
    email: str
    send_time: datetime
    purpose: EmailPurpose = EmailPurpose.REGISTER


# EmailRequest model
class EmailSMTPRequest(EmailConfirmResponseBase):
    """SMTP邮件请求模型，包含SMTP相关字段"""
    subject: str = "Account Confirmation By Server"
    text_content: str
    smtp_port: int
    smtp_username: str
    smtp_from_email: str
    smtp_server: str
