from fastapi import APIRouter
from models.file_models import FileStreamingResponse
from models.file_models import FilePublic, ChunkedUploadStatus
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep,
    DownloadFileDep, PreflightDep,PartPublic,
    ChunkedPreflightDep, ChunkUploadDep, ChunkedStatusDep
)
from constants import CHUNK_BUFFER_SIZE

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 文件管理 API
#########################

@file_router.post("/preflight", response_model=FilePublic)  # 上传预检
async def preflight_file(file_record: PreflightDep):
    return file_record

@file_router.post("/upload_simple", response_model=FilePublic)#简单上传
async def upload_simple_file(file_record: SimpleUploadDep):
    return file_record


@file_router.get("")#查询用户文件
async def list_user_files(files: ListFilesDep):
    return files


@file_router.get("/{file_id}/download")#下载文件
async def download_file(file_meta: DownloadFileDep):#使用依赖注入的元数据创建文件流，创建文件流响应
    return FileStreamingResponse(file_meta.path, file_meta, CHUNK_BUFFER_SIZE)


@file_router.delete("/{file_id}")#删除文件
async def delete_file(result: DeleteFileDep):
    return result


@file_router.post("/chunked/preflight", response_model=FilePublic)  # 分片上传预检
async def preflight_chunked_file(file_record: ChunkedPreflightDep):
    return file_record

@file_router.post("/chunked/upload", response_model=PartPublic)  # 分片上传
async def upload_chunk(chunk_result: ChunkUploadDep):
    return chunk_result

@file_router.get("/chunked/{file_id}/status", response_model=ChunkedUploadStatus)  # 查询分片上传状态
async def get_upload_status(status: ChunkedStatusDep):
    return status
