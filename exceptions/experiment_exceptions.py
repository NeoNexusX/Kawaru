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

# oss上传后自动触发的函数调用ID为空异常
oss_fc_request_id_empty_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("oss fc_request_id is required")
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

# algorithms JSON 格式无效异常
invalid_algorithms_json_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid algorithms JSON format")
)

# origin 尚未完成，无法提交算法处理异常
origin_not_ready_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("origin experiment is not completed yet")
)

# 用户处理配额超限异常
processing_quota_exceeded_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("processing quota exceeded")
)

# 处理请求校验失败异常（文件不存在/无权限/origin未就绪/algorithms无效等）
invalid_process_request_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid process request")
)

# origin 文件在磁盘上不存在或为空（算法处理时）
origin_file_not_found_on_disk_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("origin imzML/ibd file not found on disk")
)


__all__ = [
    "origin_not_found_exception",
    "origin_permission_denied_exception",
    "source_file_not_found_exception",
    "invalid_zip_file_exception",
    "unsafe_zip_path_exception",
    "zip_extraction_failed_exception",
    "invalid_zip_content_exception",
    "imzml_validation_failed_exception",
    "process_run_not_found_exception",
    "process_run_permission_denied_exception",
    "invalid_algorithms_json_exception",
    "origin_not_ready_exception",
    "processing_quota_exceeded_exception",
    "invalid_process_request_exception",
    "origin_file_not_found_on_disk_exception",
    "oss_fc_request_id_empty_exception",
    "origin_type_not_supported_exception",
]
