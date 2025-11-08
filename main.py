import uvicorn
from contextlib import asynccontextmanager
from typing import Annotated
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.email_api import EmailConfirmDep, EmailConfirmSMTPDep
from api.user_api import UserLoginDep, token_authen, UserCreateDep, UserUpdateDep, UserDeleDep, create_admin_user
from database.db import create_db_and_tables
from envset.config import get_config
from envset.envset import EnvSet
from logger import get_logger
from models.auth import Token
from models.email_models import EmailConfirmRequest
from models.user_models import UserInDB, UserPublic


@asynccontextmanager
async def lifespan(app: FastAPI):
    # start run
    create_db_and_tables()
    EnvSet()
    # admin create
    await create_admin_user()
    yield


app = FastAPI(lifespan=lifespan)

# get the main logger
logger = get_logger("main")

# get config
config = get_config()

# CORSMiddleware
origins = ["*", ]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


#########################
# API
#########################

@app.post("/login")
async def login(token: UserLoginDep) -> Token:
    try:
        return token
    except Exception as e:
        raise e


@app.get("/user", response_model=UserPublic)
async def get_current_usr(user: Annotated[UserInDB, Depends(token_authen)]):
    return user


@app.post("/user_signup", response_model=UserPublic)
async def create_user(user: UserCreateDep):
    return user


@app.post("/user_change", response_model=UserPublic)
async def update_user(user: UserUpdateDep):
    return user


@app.delete("/user_delete")
async def del_user(user: UserDeleDep):
    return user


@app.post("/emailrequest", response_model=EmailConfirmRequest)
async def create_user_email_server(email: EmailConfirmDep):
    return email


@app.post("/emailsmtprequest", response_model=EmailConfirmSMTPDep)
async def create_user_smtp_email_server(email: EmailConfirmSMTPDep):
    return email


# update ：uvicorn run config
if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # 模块名:FastAPI 实例名
        host=config.server.host,  # 监听所有网络接口
        port=config.server.port,  # 端口号
        log_level=config.server.log_level,  # 日志级别
        reload=True,  # 开发时自动重载
    )