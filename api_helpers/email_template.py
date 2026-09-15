#########################
# Email Template Helper - 纯工具函数
# 职责：按邮件用途(EmailPurpose)渲染主题与正文。无 DB、无 FastAPI 依赖。
# 新增邮件类型：在 EmailPurpose 增加成员后，于 _TEMPLATES 注册一条即可。
#########################
from collections.abc import Callable
from dataclasses import dataclass

from boot.config import get_config
from models.email_models import EmailPurpose

# 验证码有效期（分钟），用于正文提示文案；与 constants.user_constants.EMAIL_CODE_TTL_SECONDS 保持一致
_CODE_VALID_MINUTES = 5


@dataclass(frozen=True)
class EmailContent:
    """渲染后的邮件内容：主题 + 纯文本正文"""
    subject: str
    text_content: str


# purpose -> 根据项目名、验证码、用户名生成邮件内容的构造器
# username 仅对已有账号的场景（如密码重置）有意义，其余模板可忽略
_TEMPLATES: dict[EmailPurpose, Callable[[str, str, str | None], EmailContent]] = {
    EmailPurpose.REGISTER: lambda app, code, username: EmailContent(
        subject=f"Welcome to {app} - Confirm your account",
        text_content=(
            f"Welcome to {app}!\r\n\r\n"
            f"Your verification code is: {code}\r\n"
            f"It is valid for {_CODE_VALID_MINUTES} minutes.\r\n\r\n"
            "If you did not sign up, please ignore this email."
        ),
    ),
    EmailPurpose.UPDATE: lambda app, code, username: EmailContent(
        subject=f"{app} - Confirm your email change",
        text_content=(
            f"You are updating the email bound to your {app} account.\r\n\r\n"
            f"Your verification code is: {code}\r\n"
            f"It is valid for {_CODE_VALID_MINUTES} minutes.\r\n\r\n"
            "If this wasn't you, please ignore this email and secure your account."
        ),
    ),
    EmailPurpose.RESET_PASSWORD: lambda app, code, username: EmailContent(
        subject=f"{app} - Reset your password",
        text_content=(
            f"We received a request to reset the password for your {app} account.\r\n\r\n"
            f"Username: {username}\r\n"
            f"Password reset code: {code}\r\n"
            f"The code is valid for {_CODE_VALID_MINUTES} minutes.\r\n\r\n"
            "If you did not request a password reset, please ignore this email; "
            "your password will remain unchanged."
        ),
    ),
}


def render_email(purpose: EmailPurpose, code: str, username: str | None = None) -> EmailContent:
    """
    按邮件用途渲染主题与正文。项目名取自 config.server.name（默认 SpatialXomics）。
    username 用于已有账号场景（如密码重置）在正文中提示用户名，其余场景可不传。
    purpose 已由 Pydantic 约束为合法枚举，缺少模板时抛 KeyError 以尽早暴露遗漏。
    """
    app_name = get_config().server.name
    return _TEMPLATES[purpose](app_name, code, username)
