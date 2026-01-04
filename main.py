'''
Author: xiongqiurong 1513401658@qq.com
Date: 2025-12-21 22:43:01
LastEditors: xiongqiurong 1513401658@qq.com
LastEditTime: 2025-12-28 14:33:15
FilePath: /Kawaru/main.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import uvicorn
from contextlib import asynccontextmanager
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.user_api import  create_admin_user
from database.db import create_db_and_tables, SessionDep
from envset.config import get_config
from envset.envset import EnvSet
from logger import get_logger
from routers.user import user_router
from routers.file import file_router
from routers.experiment import experiment_router

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
# get .env
EnvSet()

# CORSMiddleware跨域请求
origins = ["*", ] # 接受所有来源请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(user_router)
app.include_router(file_router)
app.include_router(experiment_router)


# update ：uvicorn run config
if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # 模块名:FastAPI 实例名
        host=config.server.host,  # 监听所有网络接口
        port=config.server.port,  # 端口号
        log_level=config.server.log_level,  # 日志级别
        reload=True,  # 开发时自动重载
    )