"""FC 回调异常模块：回调身份验证等直接返回客户端的 HTTP 异常。"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail

# FC 回调 Token 无效或缺失
invalid_fc_callback_token_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Invalid or missing FC callback token"),
)

# FC 回调目标记录不存在（404：永久失败，FC 停止重试，区别于瞬时故障的 5xx）
fc_callback_target_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("FC callback target not found"),
)

__all__ = [
    "invalid_fc_callback_token_exception",
    "fc_callback_target_not_found_exception",
]
