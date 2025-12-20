import uvicorn
from contextlib import asynccontextmanager
from typing import Annotated
from fastapi import APIRouter, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.email_api import EmailConfirmDep, EmailConfirmSMTPDep
from models.file_models import FileStreamingResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep, 
    CHUNK_BUFFER_SIZE, DownloadFileDep, PreflightDep,
    ChunkedPreflightDep, ChunkUploadDep, ChunkedStatusDep
)
from api.user_api import UserLoginDep, token_authen, UserCreateDep, AdminCreateDep,UserUpdateDep, UserDeleDep, create_admin_user
from database.db import create_db_and_tables, SessionDep
from envset.config import get_config
from envset.envset import EnvSet
from logger import get_logger
from models.auth import Token
from models.email_models import EmailConfirmRequest
from models.user_models import UserInDB, UserPublic
from models.file_models import FilePublic, ChunkedUploadStatus



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

# CORSMiddleware跨域请求
origins = ["*", ]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

user_router = APIRouter(tags=["用户管理"])
file_router = APIRouter(tags=["文件管理"])


#########################
# 用户管理 API
#########################

@user_router.post("/login")
async def login(token: UserLoginDep) -> Token:
    try:
        return token
    except Exception as e:
        raise e


@user_router.get("/user", response_model=UserPublic)
async def get_current_usr(user: Annotated[UserInDB, Depends(token_authen)]):
    return user


@user_router.post("/user_signup", response_model=UserPublic)
async def create_user(user: UserCreateDep):
    return user


@user_router.post("/admin_signup", response_model=UserPublic)#新增管理员用户接口
async def create_admin_user_endpoint(user: AdminCreateDep):
    return user


@user_router.post("/user_change", response_model=UserPublic)
async def update_user(user: UserUpdateDep):
    return user


@user_router.delete("/user_delete")
async def del_user(user: UserDeleDep):
    return user

@user_router.post("/emailrequest", response_model=EmailConfirmRequest)#MAILGUN邮箱服务
async def create_user_email_server(email: EmailConfirmDep):
    return email


@user_router.post("/emailsmtprequest", response_model=EmailConfirmSMTPDep)#SMTP邮箱服务
async def create_user_smtp_email_server(email: EmailConfirmSMTPDep):
    return email

#########################
# 文件管理 API
#########################

@file_router.post("/files/preflight", response_model=FilePublic)  # 上传预检
async def preflight_file(file_record: PreflightDep):
    return file_record
    
@file_router.post("/files/upload_simple", response_model=FilePublic)#简单上传
async def upload_simple_file(file_record: SimpleUploadDep):
    return file_record


@file_router.get("/files", response_model=list[FilePublic])#查询用户文件
async def list_user_files(files: ListFilesDep):
    return files


@file_router.get("/files/{file_id}/download")#下载文件
async def download_file(file_meta: DownloadFileDep):#使用依赖注入的元数据创建文件流，创建文件流响应
    return FileStreamingResponse(file_meta.path, file_meta, CHUNK_BUFFER_SIZE)


@file_router.delete("/files/{file_id}")#删除文件
async def delete_file(result: DeleteFileDep):
    return result


@file_router.post("/files/chunked/preflight", response_model=FilePublic)  # 分片上传预检
async def preflight_chunked_file(file_record: ChunkedPreflightDep):
    return file_record

@file_router.post("/files/chunked/upload")  # 分片上传
async def upload_chunk(chunk_result: ChunkUploadDep):
    return chunk_result

@file_router.get("/files/chunked/{file_id}/status", response_model=ChunkedUploadStatus)  # 查询分片上传状态
async def get_upload_status(status: ChunkedStatusDep):
    return status

app.include_router(user_router)
app.include_router(file_router)


# update ：uvicorn run config
if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # 模块名:FastAPI 实例名
        host=config.server.host,  # 监听所有网络接口
        port=config.server.port,  # 端口号
        log_level=config.server.log_level,  # 日志级别
        reload=True,  # 开发时自动重载
    )