from typing import Union
from datetime import timedelta, datetime, timezone
import jwt
from starlette import status
from fastapi import HTTPException
from models.auth import PWD_CONTEXT, SECRET_KEY
from envset.config import get_config
from exceptions.exception_utils import format_exception_detail

#########################
# http 异常声明
#########################
# 用户名或密码错误
credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Incorrect username or password"),
    headers={"WWW-Authenticate": "Bearer"},
)

# 令牌过期
token_expired_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Token expired"),
    headers={"WWW-Authenticate": "Bearer"},
)

token_invalid_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail=format_exception_detail("Invalid token"),
    headers={"WWW-Authenticate": "Bearer"},
)
#########################

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
