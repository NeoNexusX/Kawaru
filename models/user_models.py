from sqlalchemy import Column
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel
from logger import get_logger

# 获取服务器状态模块的日志器
logger = get_logger("main.user_control")


class UserBase(SQLModel):
    username: str = Field()
    active: bool = Field(default=True)
    identity: str = Field(default="user")


class UserInDB(UserBase, table=True):
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    email: str = Field()
    hashed_password: str = Field()
    file_ids: list[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))#文件ID列表JSON格式
    file_count: int = Field(default=0)
    total_file_size: int = Field(default=0)
    download_used: int = Field(default=0)


class UserPublic(UserBase):
    email: str


class UserCreate(UserBase):
    email: str
    password: str
    verify_code: str
    identity: str | None = Field(default=None)  # 传入的时候必须为None


class UserUpdate(UserBase):
    email: str = Field(default=None)
    password: str | None = None

