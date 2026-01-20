from models.file_models import FileStreamingResponse
from models.file_models import FilePublic, ChunkedUploadStatus
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep, 
    CHUNK_BUFFER_SIZE, DownloadFileDep, PreflightDep,
    ChunkedPreflightDep, ChunkUploadDep, ChunkedStatusDep
)
from fastapi import APIRouter

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 文件管理 API
#########################

@file_router.post("/preflight", response_model=FilePublic)
async def preflight_file(file_record: PreflightDep):
    """上传预检"""
    return file_record
    
@file_router.post("/upload_simple", response_model=FilePublic)
async def upload_simple_file(file_record: SimpleUploadDep):
    """简单上传"""
    return file_record


@file_router.get("", response_model=list[FilePublic])#查询用户文件
async def list_user_files(files: ListFilesDep):
    """查询用户文件"""
    return files


@file_router.get("/{file_id}/download")
async def download_file(file_meta: DownloadFileDep):#使用依赖注入的元数据创建文件流，创建文件流响应
    """下载文件"""
    return FileStreamingResponse(file_meta.path, file_meta, CHUNK_BUFFER_SIZE)


@file_router.delete("/{file_id}")
async def delete_file(result: DeleteFileDep):
    """删除文件"""
    return result


@file_router.post("/chunked/preflight", response_model=FilePublic)
async def preflight_chunked_file(file_record: ChunkedPreflightDep):
    """分片上传预检"""
    return file_record

@file_router.post("/chunked/upload")
async def upload_chunk(chunk_result: ChunkUploadDep):
    """分片上传"""
    return chunk_result

@file_router.get("/chunked/{file_id}/status", response_model=ChunkedUploadStatus)
async def get_upload_status(status: ChunkedStatusDep):
    """查询分片上传状态"""
    return status