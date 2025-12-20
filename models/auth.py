from typing import Union
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from pydantic import BaseModel
import secrets


SECRET_KEY = secrets.token_hex(32) # 每次启动服务时随机生成 32 字节的对称密钥用于签名
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # 令牌过期时间30分钟
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto") 
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login/") # 获取登录令牌入口

# 登录成功后返回的令牌模型
class Token(BaseModel):
    access_token: str
    token_type: str

# 寄存登录后的用户信息
class TokenData(BaseModel):
    username: Union[str, None] = None

# 登录表单字段，配置为禁止用户传额外字段
class FormData(BaseModel):
    username: str
    password: str
    model_config = {"extra": "forbid"}