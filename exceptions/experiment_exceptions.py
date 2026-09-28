"""存储和 origin 相关的异常定义"""
from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail

# origin 类型不支持异常
origin_type_not_supported_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("origin type not supported")
)

# origin 不存在异常
origin_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("origin not found")
)

# 无权限访问 origin 异常
origin_permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("permission denied to access this origin")
)

# 源文件不存在异常
source_file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("source file not found")
)

# process_run 不存在异常
process_run_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("process run not found")
)

# 无权限访问 process_run 异常
process_run_permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("permission denied to access this process run")
)

# 处理请求校验失败异常（文件不存在/无权限/origin未就绪/algorithms无效等）
invalid_process_request_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid process request")
)

# 算法处理尚未完成，无法返回 zarr 文件夹
process_run_not_completed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("processing not completed, zarr folder not available yet")
)

# zarr 文件夹为空或不存在于 OSS
zarr_folder_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("zarr folder is empty or not found on OSS")
)


__all__ = [
    "origin_not_found_exception",
    "origin_permission_denied_exception",
    "source_file_not_found_exception",
    "process_run_not_found_exception",
    "process_run_permission_denied_exception",
    "invalid_process_request_exception",
    "origin_type_not_supported_exception",
    "process_run_not_completed_exception",
    "zarr_folder_not_found_exception",
]
