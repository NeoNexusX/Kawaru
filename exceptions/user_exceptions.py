from starlette import status
from fastapi import HTTPException

user_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,  # 409 Conflict
    detail="Username already exists",  # 明确提示用户已存在
)

user_not_active_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="Username not  activate",  # 明确提示用户没激活
)

user_verification_bad_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="verification code is wrong",  # 明确提示用户验证失败
)

user_name_bad_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,  # 401 Unauthorized
    detail="user is unauthorized",  # 明确提示操作用户不一致
)

user_position_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 400 Bad Request
    detail="user position is not allowed",  # 用户职位不在允许列表内
)

user_region_not_exist_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 400 Bad Request
    detail="user region is not exist",  # 用户地区不存在
)

user_orcid_not_exist_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 400 Bad Request
    detail="user ORCID is not exist",  # 用户ORCID不存在
)

user_homepage_invalid_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 400 Bad Request
    detail="user homepage is invalid",  # 用户主页无效（主页格式须为http或https开头的合法URL）
)

identity_not_allowed_exception = (
    HTTPException(  # 异常定义，identity设为none，不允许传入
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Identity field must be None, it will be set by the API endpoint",
    )
)

admin_create_not_allowed_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Only admin users can create admin accounts",
)

__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
    "user_already_exists_exception",
    "user_not_active_exception",
    "user_verification_bad_exception",
    "user_name_bad_exception",
    "user_position_not_allowed_exception",
    "user_region_not_exist_exception",
    "user_orcid_not_exist_exception",
    "user_homepage_invalid_exception",
    "identity_not_allowed_exception",
    "admin_create_not_allowed_exception",
]