from fastapi import APIRouter
from models.file_models import FileStreamingResponse
from models.file_models import FilePublic, PartUploadStatus
from models.filter_models import ListDataResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep,
    DownloadFileDep, PreflightDep,PartPublic,
    PartUploadDep, PartStatusDep, ListPublicFilesDep,
    OssUploadCompleteDep
)
from constants.file_constants import CHUNK_BUFFER_SIZE

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 文件管理 API
#########################

@file_router.post("/preflight", response_model=FilePublic)
async def preflight_file(file_record: PreflightDep):
    """上传预检"""
    return file_record


@file_router.post("/upload_simple", response_model=FilePublic)#简单上传
async def upload_simple_file(file_record: SimpleUploadDep):
    """简单上传"""
    return file_record


@file_router.post("/list_user_files", response_model=ListDataResponse)#查询用户文件
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


@file_router.post("/chunked/upload", response_model=PartPublic)  # 分片上传
async def upload_chunk(chunk_result: PartUploadDep):
    """分片上传"""
    return chunk_result


@file_router.get("/chunked/{file_id}/status", response_model=PartUploadStatus)
async def get_upload_status(status: PartStatusDep):
    """查询分片上传状态"""
    return status


@file_router.post("/list_files", response_model=ListDataResponse)
async def list_files(list_file_response: ListPublicFilesDep):
    """查询文件列表"""
    return list_file_response


@file_router.post("/oss_upload_complete")
async def oss_upload_complete(file_record: OssUploadCompleteDep):
    """OSS上传完成时，前端调用该接口通知后端"""
    return file_record
