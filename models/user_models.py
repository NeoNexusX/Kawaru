from pydantic import BaseModel
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

class UserBase(SQLModel):
    """
    基本用户信息模型
    """
    username: str = Field(max_length=user_config.max_username_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)
    email: str = Field(max_length=user_config.max_useremail_length, description="用户邮箱")

class UserInDB(UserBase, table=True):
    """
    存储于数据库中的用户信息模型，包含加密密码和文件相关信息
    """
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    hashed_password: str = Field(max_length=256, description="加密后的密码")
    file_ids: list[int] = Field(default_factory=list,
                                sa_column=Column(JSON, nullable=False),
                                description="用户的文件ID列表")  # 文件ID列表JSON格式
    file_count: int = Field(default=0, description="用户文件数量")
    total_file_size: int = Field(default=0, description="用户文件总大小（字节）")
    download_used: int = Field(default=0, description="用户已使用的下载次数")

class UserCreate(UserBase):
    """新用户创建模型，包括邮箱、密码、验证码"""
    password: str
    verify_code: str
    identity: str | None = Field(default=None)  # 传入的时候必须为None


class UserPublic(BaseModel):
    """用于返回响应信息的用户模型"""
    username: str = Field(max_length=user_config.max_username_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)


class UserUpdate(BaseModel):
    """
    新用户信息模型，需要提供邮箱和密码，可选更新字段，不允许更新身份，名字字段
    """
    username: str = Field(max_length=user_config.max_username_length) #强制提供
    email: str = Field(default=None,max_length=user_config.max_useremail_length, description="用户邮箱") #不强制提供
    password: str | None = None #不强制提供
