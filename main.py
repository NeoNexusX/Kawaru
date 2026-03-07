import os
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from celery import Celery
from boot.bootstrap import bootstrap
bootstrap()
# pylint: disable=wrong-import-position
from boot.config import get_config
from database.db import create_db_and_tables
from api.user_api import create_admin_user
from routers.user import user_router
from routers.file import file_router
from routers.experiment import experiment_router
# pylint: enable=wrong-import-position

@asynccontextmanager
async def lifespan(app: FastAPI): # pylint: disable=unused-argument,redefined-outer-name
    """
    FastAPI lifespan context manager.
    initialize resources before the application starts
    """
    await create_db_and_tables()
    # admin create
    await create_admin_user()
    yield


app = FastAPI(lifespan=lifespan)

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

# Celery 配置
celery_app = Celery('celery_app')
celery_app.config_from_object('boot.celeryconfig')
# TODO: 此时celery还没启动，需要通过终端运行
# celery -A main.celery_app worker -l info
# 或通过在lifespan中创建subprocess等方式启动

# update ：uvicorn run config
if __name__ == "__main__":
    # get config
    config = get_config()
    uvicorn.run(
        "main:app",  # 模块名:FastAPI 实例名
        host=config.server.host,  # 监听所有网络接口
        port=config.server.port,  # 端口号
        log_level=os.getenv("LOG_LEVEL", "info"),  # 日志级别
        reload=True,  # 开发时自动重载
        reload_excludes=[
            "tests/**" # 排除测试文件夹导致的自动重载
        ]
    )
