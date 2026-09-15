from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail


# ===================== Auth 相关异常 =====================
# 从 api/auth_api.py 迁移至此

# 用户名或密码错误
credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Incorrect username or password"),
    headers={"WWW-Authenticate": "Bearer"},
)

# 登录状态过期
login_expired_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="login status expired or user has logged out",
    headers={"WWW-Authenticate": "Bearer"},
)

# 令牌过期
token_expired_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Token expired"),
    headers={"WWW-Authenticate": "Bearer"},
)

token_invalid_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Invalid token"),
    headers={"WWW-Authenticate": "Bearer"},
)

# ===================== User 相关异常 =====================

user_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,#提示用户已经存在
    detail=format_exception_detail("User already exists")
)

email_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,#提示邮箱已经注册
    detail=format_exception_detail("Email already registered")
)

user_not_active_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#提示用户没激活
    detail=format_exception_detail("Username not activate")
)

user_verification_bad_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#提示用户验证失败
    detail=format_exception_detail("Verification code is wrong")
)

user_name_bad_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,#提示用户操作不一致
    detail=format_exception_detail("User is unauthorized")
)

user_position_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#用户职位不在列表中
    detail=format_exception_detail("User position is not allowed")
)

user_region_not_exist_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#用户地区不存在
    detail=format_exception_detail("User region does not exist")
)

user_orcid_not_exist_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#用户ORCID不存在
    detail=format_exception_detail("User ORCID does not exist")
)

user_homepage_invalid_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#用户主页无效（必须是以http或者https开头的合法URL）
    detail=format_exception_detail("User homepage is invalid")
)

user_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,#用户不存在
    detail=format_exception_detail("User not found")
)

identity_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#identity必须为空，不允许传入
    detail=format_exception_detail("Identity field must be None, it will be set by the API endpoint")
)

reset_code_invalid_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#密码重置验证码错误或已过期
    detail=format_exception_detail("Reset code is invalid or expired")
)

reset_password_too_many_attempts_exception = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,#密码重置验证码尝试次数过多，本轮已锁定
    detail=format_exception_detail("Too many attempts, please request a new reset code")
)

verify_code_too_many_attempts_exception = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,#注册/改邮箱验证码尝试次数过多，本轮验证码已作废
    detail=format_exception_detail("Too many attempts, please request a new verification code")
)

email_send_too_frequent_exception = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,#同一邮箱发送验证码邮件过于频繁，处于冷却期
    detail=format_exception_detail("Verification email requested too frequently, please try again later")
)


__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    # Auth 异常
    "credentials_exception",
    "login_expired_exception",
    "token_expired_exception",
    "token_invalid_exception",
    # User 异常
    "user_already_exists_exception",
    "email_already_exists_exception",
    "user_not_active_exception",
    "user_verification_bad_exception",
    "user_name_bad_exception",
    "user_position_not_allowed_exception",
    "user_region_not_exist_exception",
    "user_orcid_not_exist_exception",
    "user_homepage_invalid_exception",
    "identity_not_allowed_exception",
    "user_not_found_exception",
    "reset_code_invalid_exception",
    "reset_password_too_many_attempts_exception",
    "verify_code_too_many_attempts_exception",
    "email_send_too_frequent_exception",
]
