from datetime import datetime
from enum import Enum

import pyotp
from pydantic import BaseModel

from tools.logger import get_logger

logger = get_logger("models.email_models")


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
    smtp_passwd: str
    smtp_from_email: str
    smtp_server: str


class EmailVerificationCodeTotp:
    """邮件验证码TOTP生成器，使用pyotp库生成基于时间的一次性密码"""

    def __init__(self):
        # create secret and TOTP
        self.secret = pyotp.random_base32() # To set the otp secret for the first time.
        logger.debug(f"Totp init finish secret : {self.secret}")
        self.totp = pyotp.TOTP(self.secret, interval=300, digits=6) # 5mins interval, 6digits verification code

    def get_totp(self):
        """The TOTP code is valid for 5 minutes, and the code consists of 6 digits."""
        return self.totp


TOTP = EmailVerificationCodeTotp().get_totp()
