"""
OSS 相关异常模块
"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail

# OSS 客户端创建失败异常
oss_client_create_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("failed to create oss client"),
)

# OSS 超时异常
oss_timeout_exception = HTTPException(
    status_code=status.HTTP_504_GATEWAY_TIMEOUT,
    detail=format_exception_detail("oss request timeout"),
)

__all__ = [
    "oss_client_create_failed_exception",
    "oss_timeout_exception",
]
