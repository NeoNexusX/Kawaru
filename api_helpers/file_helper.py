#########################
# File Helper - 底层工具函数
# 职责：纯工具函数/存储IO,无业务逻辑,无数据库会话依赖
# 包含：文件校验
# 注意：OSS 删除统一走 OssStorage.delete_file（单 key / 多 key / 目录前缀均可），
#      本层不再提供删除包装函数
#########################
from __future__ import annotations

from models.user_models import UserInDB
from constants.file_constants import (
    MAX_FILENAME_LENGTH,
    ALLOWED_FILE_TYPES,
)

from tools.logger import get_logger
logger = get_logger("api_helpers.file_helper")


########
# file 查
########
def check_user_quota(user: UserInDB, file_size: int) -> bool:  #TODO 风险，后面加锁
    """
    检查用户配额是否足够

    综合检查：
    1. 文件数量是否超过限制
    2. 单个文件大小是否超过总限额
    3. 总存储空间是否超过限制

    Returns:
        True 表示所有配额检查通过,False 表示至少有一项超出限制
    """
    # 检查文件数量,单个文件大小,总存储空间（均从用户 DB 记录读取）
    # 注意：max_total_file_size 以 GB 存储，需转换为字节后与 file_size / total_file_size 比较
    BYTES_IN_GB = 1024**3 #type: ignore
    max_total_bytes = user.max_total_file_size * BYTES_IN_GB

    current_count = user.file_count
    projected_total_size = user.total_file_size + file_size
    if (current_count >= user.max_file_count
        or file_size > max_total_bytes
        or projected_total_size > max_total_bytes):
        logger.error(f"user quota exceeded user={user.id}:\n"
                       f"current_count={current_count}: limit={user.max_file_count}\n"
                       f"file_size={file_size}: limit={max_total_bytes}\n"
                       f"projected={projected_total_size}: limit={max_total_bytes}"
                       )
        return False

    return True


def validate_filename(filename: str) -> bool:
    """
    校验文件名安全性

    检查项：
    - 文件名非空
    - 长度不超过限制
    - 不包含路径遍历攻击字符(../, ..\\ 等)
    - 不包含危险字符(<, >, :, ", |, ?, *, 空字符)
    """
    if not filename or not filename.strip():
        return False

    if len(filename) > MAX_FILENAME_LENGTH:
        return False

    # 检查路径遍历攻击
    unsafe_patterns = ["../", "..\\", "/", "\\", ".."]
    for pattern in unsafe_patterns:
        if pattern in filename:
            return False

    # 检查危险字符
    dangerous_chars = ["<", ">", ":", '"', "|", "?", "*", "\x00"]
    for char in dangerous_chars:
        if char in filename:
            return False

    return True


def validate_file_type(file_type: str) -> bool:
    """
    校验文件类型是否在允许列表中

    直接检查 file_type 是否在 ALLOWED_FILE_TYPES 中
    """
    if not file_type:
        return False

    if file_type not in ALLOWED_FILE_TYPES:
        return False

    return True


def validate_file_metadata(filename: str, file_type: str) -> bool:
    """
    验证文件元数据(文件名和类型)

    Args:
        filename: 文件名（无后缀）
        file_type: 文件类型

    Returns:
        True 表示验证通过,False 表示验证失败
    """
    return validate_filename(filename) and validate_file_type(file_type)
