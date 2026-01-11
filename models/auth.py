from typing import Union
import secrets
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from pydantic import BaseModel

SECRET_KEY = secrets.token_hex(32)  # 每次启动服务时随机生成 32 字节的对称密钥用于签名
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login/")  # 获取登录令牌入口


class Token(BaseModel):
    """登录成功后返回的令牌模型。"""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """存储登录后用户的用户名信息。"""
    username: Union[str, None] = None


class FormData(BaseModel):
    """登录表单字段，仅允许用户名和密码。"""
    username: str
    password: str
    model_config = {"extra": "forbid"}
