from datetime import datetime

import pyotp
from pydantic import BaseModel

from tools.logger import get_logger

logger = get_logger("models.email_models")


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
    smtp_port: int
    smtp_username: str
    smtp_passwd: str
    smtp_from_email: str
    smtp_server: str


class EmailVerificationCodeTotp:

    def __init__(self):
        # create secret and TOTP
        self.secret = pyotp.random_base32() # To set the otp secret for the first time.
        logger.debug(f"Totp init finish secret : {self.secret}")
        self.totp = pyotp.TOTP(self.secret, interval=300, digits=6) # 5mins interval, 6digits verification code

    def get_totp(self):
        return self.totp


TOTP = EmailVerificationCodeTotp().get_totp()
