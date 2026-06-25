"""统计相关的异常定义"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail


invalid_file_classify_field_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Invalid file classify field"),
)

invalid_user_classify_field_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Invalid user classify field"),
)


__all__ = [
    "invalid_file_classify_field_exception",
    "invalid_user_classify_field_exception",
]
