from starlette import status
from fastapi import HTTPException

# 存储类型不在允许列表异常
storage_type_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="storage type is not allowed",
)

# 用户文件列表同步异常
invalid_user_for_file_list_sync_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="invalid user for file id list sync"
)

# 存储类型尚未实现异常
storage_type_not_supported_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="storage type is not supported yet",
)

# 文件超出大小限制异常
file_size_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="file size exceeds limit",
)

# 文件不存在异常
file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="file not found",
)

# 无权限访问文件异常
permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="permission denied",
)

# 分布式存储删除未实现异常
distributed_storage_delete_not_implemented_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="distributed storage delete not implemented",
)

# 不支持的存储类型删除异常
storage_type_delete_unsupported_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="unsupported storage type",
)

# 当前存储类型下载未实现异常
download_not_implemented_exception = HTTPException(
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    detail="download for this storage type not implemented",
)

# 文件内容缺失异常
file_content_missing_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="file content missing",
)

# 文件查询失败异常
file_query_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="failed to query file record",
)

# 数据库会话异常
invalid_db_session_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="database session not available",
)

# 文件写入失败异常
file_persist_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="failed to persist file content",
)

# 文件名不安全异常
filename_invalid_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="filename is invalid or contains unsafe characters",
)

# 文件类型不允许异常
file_type_not_allowed_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="file type is not allowed",
)

# 用户文件数量限制异常
user_file_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="user file count limit reached",
)

# 用户存储空间限制异常
user_storage_limit_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="user storage limit exceeded",
)

# 文件重复异常
duplicate_file_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="duplicate filename or hash detected",
)

# 用户下载次数达到上限异常
user_download_limit_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="user download limit reached",
)

# 预检与上传不一致异常
preflight_size_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded file size does not match preflight size",
)
# 上传的文件hash没有匹配上预检文件hash
preflight_hash_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded file hash does not match preflight hash",
)
# 上传的文件名没有匹配上预检文件名
preflight_filename_mismatch_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="uploaded filename does not match preflight filename",
)
# 分片相关异常定义

# 分片数要大于0
total_parts_must_greater_than_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="total_parts must be greater than 0"
)

# 分片数量太多了
total_part_too_much_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="too many parts for chunked upload"
)

# 分片编号无效异常
invalid_part_number_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="invalid part number"
)

# 分片编号超出范围异常
part_number_out_of_range_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="part number exceeds total parts"
)

# 分片已存在异常
part_already_exists_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="part already uploaded"
)

# 分片序列不完整异常
incomplete_part_sequence_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST, detail="incomplete part sequence for merge"
)

# 合并后文件hash不匹配异常
merged_file_hash_mismatch_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="merged file hash does not match expected hash",
)

# 分片文件缺失异常
chunk_file_missing_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND, detail="chunk file missing from storage"
)

# 分片合并失败异常
chunk_merge_failed_exception = HTTPException(
    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="failed to merge chunks"
)
