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
    password: str = Field()
    hashed_password: str = Field()
    # token: str = Field(default=None)


class UserPublic(UserBase):
    email: str


class UserCreate(UserBase):
    email: str
    password: str
    verify_code: str


class UserUpdate(UserBase):
    email: str = Field(default=None)
    password: str | None = None

