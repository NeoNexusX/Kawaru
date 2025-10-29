from typing import Union
import jwt
from starlette import status
from models.auth import PWD_CONTEXT, SECRET_KEY, ALGORITHM
from fastapi import HTTPException
from datetime import timedelta, datetime, timezone

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Incorrect username or password",
    headers={"WWW-Authenticate": "Bearer"},
)

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


def hashed_password(password: str) -> str:
    return PWD_CONTEXT.hash(password)


def verify_password(plain_pwd: str, hashed_pwd: str) -> bool:
    return PWD_CONTEXT.verify(plain_pwd, hashed_pwd)


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
