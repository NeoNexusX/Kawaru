#########################
# Auth Helper - 纯工具函数
# 职责：密码哈希/校验、JWT 生成。无 DB、无 FastAPI 依赖。
#########################
from typing import Union
from datetime import timedelta, datetime, timezone
import jwt

from models.auth import PWD_CONTEXT, SECRET_KEY
from boot.config import get_config


def hashed_password(password: str) -> str:
    """将明文密码加密为哈希值。"""
    return PWD_CONTEXT.hash(password)


def verify_password(plain_pwd: str, hashed_pwd: str) -> bool:
    """校验密码与哈希密码是否匹配。"""
    return PWD_CONTEXT.verify(plain_pwd, hashed_pwd)


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    """生成带有过期时间的 JWT 访问令牌。"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=get_config().token.algorithm)
    return encoded_jwt
