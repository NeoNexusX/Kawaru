import os
import secrets
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from pydantic import BaseModel

SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)  # 优先使用环境变量，便于多实例共享
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login/")  # 获取登录令牌入口
# 可选登录：未携带 Authorization 头时返回 None 而非直接 401（用于公开/私有混合访问的接口）
optional_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login/", auto_error=False)


class Token(BaseModel):
    """登录成功后返回的令牌模型。"""
    access_token: str
    token_type: str

class RedisLoginData(BaseModel):
    """用于Redis存储的登录信息字段，包括用户名和最近访问时间"""
    username: str
    last_access_time: str # 用户上次访问时间，用于登录信息滑动过期操作, str类型方便Redis存储

class OssStsToken(BaseModel):
    """OSS上传STS Token模型"""
    AccessKeyId: str
    AccessKeySecret: str
    SecurityToken: str
    Expiration: str
