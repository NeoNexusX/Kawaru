"""文件集合（Collection）相关的异常定义"""
from fastapi import HTTPException
from starlette import status

from exceptions.utils_exception import format_exception_detail

# 集合不存在异常（公开端非公开集合同样走 404，不暴露存在性）
collection_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail=format_exception_detail("collection not found")
)

# 无权限操作集合异常（修改/删除/移除成员/排序 仅限 owner 或 admin）
collection_permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail=format_exception_detail("permission denied to operate this collection")
)

# 集合成员数量超上限（防灌水护栏）
collection_member_limit_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail=format_exception_detail("collection member limit exceeded")
)

# 集合成员不合法（不存在/未完成/非公开/实验类型不符等，明细见服务端日志）
invalid_members_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail=format_exception_detail("invalid collection members")
)

# 调序冲突（全量数组含重复或与成员全集不一致，前端刷新成员列表后重试）
member_order_conflict_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail=format_exception_detail("collection member order conflict")
)


__all__ = [
    "collection_not_found_exception",
    "collection_permission_denied_exception",
    "collection_member_limit_exception",
    "invalid_members_exception",
    "member_order_conflict_exception",
]
