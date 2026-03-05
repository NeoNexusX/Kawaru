"""存储和实验相关的异常定义"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail


# 实验不存在异常
experiment_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("experiment not found")
)

# 无权限访问实验异常
experiment_permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("permission denied to access this experiment")
)

# 源文件不存在异常
source_file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("source file not found")
)

# ZIP文件格式无效异常
invalid_zip_file_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid zip file format")
)

# ZIP文件路径不安全异常（Zip Slip攻击）
unsafe_zip_path_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("unsafe file path in zip file")
)

# ZIP文件解压失败异常
zip_extraction_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("zip extraction failed")
)

# ZIP文件内容不符合要求异常
invalid_zip_content_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("zip file must contain both .imzML and .ibd files")
)

# imzML文件验证失败异常
imzml_validation_failed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("imzML file validation failed")
)

__all__ = [
    "experiment_not_found_exception",
    "experiment_permission_denied_exception",
    "source_file_not_found_exception",
    "invalid_zip_file_exception",
    "unsafe_zip_path_exception",
    "zip_extraction_failed_exception",
    "invalid_zip_content_exception",
    "imzml_validation_failed_exception",
]
