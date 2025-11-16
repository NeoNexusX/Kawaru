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
                            #此处做了修改，删掉了password这个字段
    hashed_password: str = Field()
    # token: str = Field(default=None)


class UserPublic(UserBase):
    email: str


# class UserCreate(UserBase):
#     email: str
#     password: str
#     verify_code: str

class UserCreate(SQLModel):#没有继承UserBaase,自己定义字段，将identity定义为None
    username: str = Field()
    active: bool = Field(default=True)
    email: str
    password: str
    verify_code: str
    identity: str | None = Field(default=None)  # 必须为None


class UserUpdate(UserBase):
    email: str = Field(default=None)
    password: str | None = None

