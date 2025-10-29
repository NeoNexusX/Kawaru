from datetime import datetime

import pyotp
from pydantic import BaseModel

from logger import get_logger

logger = get_logger("main.email_models")


class EmailConfirmResponseBase(BaseModel):
    email: str
    send_time: datetime


# EmailRequest model
class EmailConfirmRequest(EmailConfirmResponseBase):
    subject: str = "Account Confirmation By Server"
    text_content: str


# EmailRequest model
class EmailSMTPRequest(EmailConfirmResponseBase):
    subject: str = "Account Confirmation By Server"
    text_content: str
    last_smtp_port: int
    last_smtp_username: str
    last_smtp_passwd: str
    last_smtp_from_email: str
    last_smtp_server: str


class EmailVerificationCodeTotp:

    def __init__(self):
        # create secret and TOTP
        self.secret = pyotp.random_base32()
        logger.debug(f"Totp init finish secret : {self.secret}")
        self.totp = pyotp.TOTP(self.secret, interval=300, digits=6)

    def get_totp(self) -> pyotp.TOTP:
        return self.totp


TOTP = EmailVerificationCodeTotp().get_totp()
