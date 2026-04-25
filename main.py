from contextlib import asynccontextmanager
import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from boot.bootstrap import bootstrap
bootstrap()
# pylint: disable=wrong-import-position
from boot.config import get_config
from celery_runtime.launcher import start_embedded_celery, stop_embedded_celery,enable_embedded_celery
from database.db import create_db_and_tables
from api.user_api import create_admin_user
from routers.user import user_router
from routers.file import file_router
from routers.experiment import experiment_router

@asynccontextmanager
async def lifespan(app: FastAPI): # pylint: disable=unused-argument,redefined-outer-name
    """
    FastAPI lifespan context manager.
    initialize resources before the application starts
    """
    # create database
    await create_db_and_tables()
    # admin create
    await create_admin_user()
    #celery worker/beat subprocess
    celery_worker_proc, celery_beat_proc = await start_embedded_celery() if enable_embedded_celery() else (None, None)
    # save process object so other parts of app can inspect/terminate if needed
    app.state.celery_worker_proc = celery_worker_proc
    app.state.celery_beat_proc = celery_beat_proc

    yield

    # shutdown: terminate celery worker if started
    celery_worker_proc = getattr(app.state, 'celery_worker_proc', None)
    celery_beat_proc = getattr(app.state, 'celery_beat_proc', None)
    if celery_worker_proc is not None or celery_beat_proc is not None:
        await stop_embedded_celery(celery_worker_proc, celery_beat_proc)


app = FastAPI(lifespan=lifespan)

# CORSMiddleware跨域请求
config = get_config() # 获取全局配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors.allow_origins,
    allow_credentials=config.cors.allow_credentials,
    allow_methods=config.cors.allow_methods,
    allow_headers=config.cors.allow_headers,
    expose_headers=config.cors.expose_headers,
)

app.include_router(user_router)
app.include_router(file_router)
app.include_router(experiment_router)

# update ：uvicorn run config
if __name__ == "__main__":

    reload_enabled = os.getenv("UVICORN_RELOAD", "false").lower() == "true"
    uvicorn.run(
        "main:app",  # 模块名:FastAPI 实例名
        host=config.server.host,  # 监听所有网络接口
        port=config.server.port,  # 端口号
        log_level=os.getenv("LOG_LEVEL", "info"),  # 日志级别
        reload=reload_enabled,  # 容器默认关闭，开发环境可通过环境变量开启
        reload_excludes=[
            "tests/**" # 排除测试文件夹导致的自动重载
        ]
    )
