from starlette import status
from fastapi import HTTPException
from exceptions.exception_utils import format_exception_detail


user_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,#提示用户已经存在
    detail=format_exception_detail("Username already exists")
)

email_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,#提示用户没激活
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

identity_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,#identity必须为空，不允许传入
    detail=format_exception_detail("Identity field must be None, it will be set by the API endpoint")
)

admin_create_not_allowed_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("Only admin users can create admin accounts")
)

__all__ = [ # noqa: F401, F403 # pylint: disable=W0401, W0614
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
    "admin_create_not_allowed_exception",
]
