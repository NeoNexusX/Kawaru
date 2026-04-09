from typing import TYPE_CHECKING
from pydantic import BaseModel
from sqlmodel import Field, SQLModel, Relationship,Column, String,BigInteger
from tools.logger import get_logger
from boot.config import get_config
from models.link_models import UserFileLink

# 防止循环引用报错
if TYPE_CHECKING:
    from models.file_models import FileInDB

# 获取服务器状态模块的日志器
logger = get_logger("models.user_models")

# 模型字段配置来自 config
config = get_config()
user_config = config.user

class UserBase(SQLModel):
    """
    基本用户信息模型
    """
    username: str = Field(max_length=user_config.max_username_length)
    institution: str = Field(default="", max_length=user_config.max_institution_length)
    position: str = Field(default="", max_length=user_config.max_position_length)
    research_field: str | None = Field(max_length=user_config.max_research_field_length)
    region: str | None = Field(max_length=user_config.max_region_length)
    orcid: str | None = Field(default=None, max_length=user_config.max_orcid_length)
    homepage: str | None = Field(max_length=user_config.max_homepage_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)
    email: str = Field(sa_column=Column(String(user_config.max_useremail_length), unique=True), description="用户邮箱")


class UserInDB(UserBase, table=True):
    """
    存储于数据库中的用户信息模型，包含加密密码和文件相关信息
    """
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    hashed_password: str = Field(max_length=256, description="加密后的密码")

    # 与 FileInDB 的多对多关系
    files: list["FileInDB"] = Relationship(back_populates="owners", link_model=UserFileLink) # 每次访问需考虑性能开销，按需访问
    file_count: int = Field(default=0, description="用户文件数量")
    total_file_size: int = Field(default=0,sa_column=Column(BigInteger, nullable=False, default=0), description="用户文件总大小（字节）")
    download_used: int = Field(default=0, description="用户已使用的下载次数")


class UserCreate(UserBase):
    """新用户创建模型，包括邮箱、密码、验证码"""
    password: str
    verify_code: str
    identity: str = Field(
        default=None,  # 默认创建时仍是 None
        exclude=True,  # 序列化/入库时都排除，代码中再给UserInDB显式设置身份
        max_length=user_config.max_identity_length,
    )


class UserPublic(BaseModel):
    """用于返回响应信息的用户模型"""
    username: str = Field(max_length=user_config.max_username_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)


class UserDetailPublic(BaseModel):
    """用于返回响应信息的用户模型"""
    username: str = Field(max_length=user_config.max_username_length)
    active: bool = Field(default=True)
    identity: str = Field(default="user", max_length=user_config.max_identity_length)
    institution: str = Field(default="", max_length=user_config.max_institution_length)
    position: str = Field(default="", max_length=user_config.max_position_length)
    research_field: str | None = Field(max_length=user_config.max_research_field_length)
    region: str | None = Field(max_length=user_config.max_region_length)
    orcid: str | None = Field(default=None, max_length=user_config.max_orcid_length)
    homepage: str | None = Field(max_length=user_config.max_homepage_length)
    email: str = Field(max_length=user_config.max_useremail_length, description="用户邮箱")

class UserUpdate(BaseModel):
    """
    新用户信息模型，需要提供邮箱和密码，可选更新字段，不允许更新身份，名字字段
    """
    username: str = Field(max_length=user_config.max_username_length) #强制提供
    email: str | None= Field(default=None,max_length=user_config.max_useremail_length, description="用户邮箱") #不强制提供
    password: str | None = None #不强制提供
    verify_code: str | None = None #邮箱验证码
    institution: str | None = Field(default=None, max_length=user_config.max_institution_length)
    position: str | None = Field(default=None, max_length=user_config.max_position_length)
    research_field: str | None = Field(default=None, max_length=user_config.max_research_field_length)
    region: str | None = Field(default=None, max_length=user_config.max_region_length)
    orcid: str | None = Field(default=None, max_length=user_config.max_orcid_length)
    homepage: str | None = Field(default=None, max_length=user_config.max_homepage_length)
