"""
FC 回调异常模块
定义 FC 回调身份验证相关的 HTTP 异常
"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail

# FC 回调 Token 无效或缺失
invalid_fc_callback_token_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Invalid or missing FC callback token"),
)

__all__ = [
    "invalid_fc_callback_token_exception",
]
