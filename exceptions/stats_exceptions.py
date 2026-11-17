"""统计相关的异常定义"""
from fastapi import HTTPException
from starlette import status

from exceptions.utils_exception import format_exception_detail

invalid_file_classify_field_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Invalid file classify field"),
)

invalid_user_classify_field_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Invalid user classify field"),
)


invalid_visit_history_range_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("Visit history range must be within the retained UTC dates, including today"),
)

visit_stats_unavailable_exception = HTTPException(
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    detail=format_exception_detail("Visit statistics temporarily unavailable"),
)


__all__ = [
    "invalid_file_classify_field_exception",
    "invalid_user_classify_field_exception",
    "invalid_visit_history_range_exception",
    "visit_stats_unavailable_exception",
]
