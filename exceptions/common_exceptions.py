from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail

# 非管理员操作不允许异常
non_admin_operation_not_allowed_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("Only admin users can perform this operation")
)


# 内部服务器错误异常 # TODO:可考虑所有其他的 exception 统一抛出该异常
internal_server_error_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("Internal server error")
)


__all__ = [
    "non_admin_operation_not_allowed_exception",
    "internal_server_error_exception",
]
