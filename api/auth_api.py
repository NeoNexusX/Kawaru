from typing import Union
import jwt
from starlette import status
from models.auth import PWD_CONTEXT, SECRET_KEY, ALGORITHM
from fastapi import HTTPException
from datetime import timedelta, datetime, timezone

#########################
# http 异常声明
#########################
# 用户名或密码错误
credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Incorrect username or password",
    headers={"WWW-Authenticate": "Bearer"},
)

# 令牌过期
token_expired_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Token expired",
    headers={"WWW-Authenticate": "Bearer"},
)

token_invalid_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid token",
    headers={"WWW-Authenticate": "Bearer"},
)
#########################
# 明文密码转换成哈希后的密码
def hashed_password(password: str) -> str:
    return PWD_CONTEXT.hash(password)

# 验证明文密码是否匹配哈希后的密码
def verify_password(plain_pwd: str, hashed_pwd: str) -> bool:
    return PWD_CONTEXT.verify(plain_pwd, hashed_pwd)

# 根据设定的加密算法，将用户数据生成具有指定有效期（默认15分钟）的 jwt token
def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
