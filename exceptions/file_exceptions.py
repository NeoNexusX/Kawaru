from starlette import status
from fastapi import HTTPException
from exceptions.utils_exception import format_exception_detail


# 用户文件列表同步异常
invalid_user_for_file_list_sync_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid user for file id list sync")
)

# 文件不存在异常
file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("file not found")
)

# 无权限访问文件异常
permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("permission denied")
)

# 当前存储类型下载未实现异常
download_not_implemented_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail=format_exception_detail("download for this storage type not implemented")
)

# 文件内容缺失异常
file_content_missing_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("file content missing")
)

# 文件写入失败异常
file_persist_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("failed to persist file content")
)

# 用户文件数量限制异常
user_file_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("user file count limit reached")
)

# 文件重复异常
duplicate_file_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("duplicate filename or hash detected")
)

# 用户下载次数达到上限异常
user_download_limit_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("user download limit reached")
)

# 分片相关异常定义

# 分片验证失败异常（哈希或大小不匹配）
part_validation_failed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("uploaded chunk hash or size does not match expected values")
)

# 分片数据格式无效异常
invalid_part_data_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid part_data format")
)

# 分片编号超出范围异常
part_number_out_of_range_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("part number exceeds total parts")
)

# 分片已存在异常
part_already_exists_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("part already uploaded")
)

# 合并后文件hash不匹配异常
merged_file_hash_mismatch_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("merged file hash does not match expected hash")
)

# 分片文件缺失异常
chunk_file_missing_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("chunk file missing from storage")
)

# 分片合并失败异常
chunk_merge_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail=format_exception_detail("failed to merge chunks")
)

# 参数验证失败异常
invalid_request_parameters_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail=format_exception_detail("invalid request parameters")
)


__all__ = [
    "invalid_user_for_file_list_sync_exception",
    "file_not_found_exception",
    "permission_denied_exception",
    "download_not_implemented_exception",
    "file_content_missing_exception",
    "file_persist_failed_exception",
    "user_file_limit_exception",
    "duplicate_file_exception",
    "user_download_limit_exception",
    "part_validation_failed_exception",
    "invalid_part_data_exception",
    "part_number_out_of_range_exception",
    "part_already_exists_exception",
    "merged_file_hash_mismatch_exception",
    "chunk_file_missing_exception",
    "chunk_merge_failed_exception",
    "invalid_request_parameters_exception",
]
