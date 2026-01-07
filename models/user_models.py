from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel
from logger import get_logger
from envset.config import get_config

# 获取服务器状态模块的日志器
logger = get_logger("main.user_control")

# 模型字段配置来自 config
config = get_config()
user_config = config.user

# 基本用户信息模型
class UserBase(SQLModel):
    username: str = Field(max_length=user_config.max_username_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)

# 存储于数据库中的用户信息模型
class UserInDB(UserBase, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    email: str = Field(max_length=user_config.max_useremail_length)
    hashed_password: str = Field(max_length=256)
    file_ids: list[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))#文件ID列表JSON格式
    file_count: int = Field(default=0)
    total_file_size: int = Field(default=0)
    download_used: int = Field(default=0)

# 用于返回响应信息的用户模型
class UserPublic(UserBase):
    email: str

# 创建用户时需要提供的用户信息模型，包括邮箱、密码、验证码
class UserCreate(UserBase):
    email: str
    password: str
    verify_code: str
    identity: str | None = Field(default=None)  # 传入的时候必须为None

# 更新用户信息，需要提供邮箱和密码
class UserUpdate(UserBase):
    email: str = Field(default=None)
    password: str | None = None

