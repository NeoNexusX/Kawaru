import asyncio

from fastapi import APIRouter
from models.file_models import FileStreamingResponse
from models.file_models import FilePublic, PartUploadStatus, FileOssUploadResponse
from models.filter_models import ListDataResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep,
    DownloadFileDep, RawDownloadDep, RawDownloadNoauthDep, PreflightDep,PartPublic,
    PartUploadDep, PartStatusDep, ListPublicFilesDep,ValidateAndMergeDep,
    OssUploadDep, ImageUrlDep, FileMetadataDep, SetFilePublicDep
)
from constants.file_constants import CHUNK_BUFFER_SIZE
from database.visit_stats import incr_visit

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 文件管理 API
#########################

@file_router.post("/preflight", response_model=FilePublic)
async def preflight_file(file_record: PreflightDep):
    """上传预检"""
    asyncio.create_task(incr_visit())
    return file_record


@file_router.post("/upload_simple", response_model=FileOssUploadResponse)#简单上传
async def upload_simple_file(file_record: SimpleUploadDep):
    """简单上传"""
    return file_record


@file_router.post("/list_user_files", response_model=ListDataResponse)#查询用户文件
async def list_user_files(files: ListFilesDep):
    """查询用户文件"""
    return files


@file_router.get("/{file_id}/download_raw")
async def download_file_raw(file_meta: RawDownloadDep):
    """RAW 直链下载（需鉴权）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    asyncio.create_task(incr_visit())
    return file_meta


@file_router.get("/{file_id}/download_raw_noauth")
async def download_file_raw_noauth(file_meta: RawDownloadNoauthDep):
    """RAW 直链下载（免登录）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    asyncio.create_task(incr_visit())
    return file_meta


@file_router.get("/{file_id}/download")
async def download_file(file_meta: DownloadFileDep):#使用依赖注入的元数据创建文件流，创建文件流响应
    """下载文件"""
    asyncio.create_task(incr_visit())
    if file_meta.storage_type == "oss":
        return file_meta
    return FileStreamingResponse(file_meta.path, file_meta, CHUNK_BUFFER_SIZE)


@file_router.delete("/{file_id}")
async def delete_file(result: DeleteFileDep):
    """删除文件"""
    return result


@file_router.post("/chunked/upload", response_model=PartPublic)  # 分片上传
async def upload_chunk(chunk_result: PartUploadDep):
    """分片上传"""
    return chunk_result


@file_router.post("/chunked/{file_id}/validate_and_merge")
async def validate_and_merge_parts(result: ValidateAndMergeDep):
    """显式触发分片合并"""
    return result


@file_router.get("/chunked/{file_id}/status", response_model=PartUploadStatus)
async def get_upload_status(status: PartStatusDep):
    """查询分片上传状态"""
    return status


@file_router.post("/list_files", response_model=ListDataResponse)
async def list_files(list_file_response: ListPublicFilesDep):
    """查询文件列表"""
    asyncio.create_task(incr_visit())
    return list_file_response


@file_router.post("/upload", response_model=FileOssUploadResponse)
async def upload_file(file_record: OssUploadDep):
    """上传文件至OSS存储"""
    return file_record


@file_router.get("/{file_id}/images")
async def get_file_images(image_urls: ImageUrlDep):
    """获取文件图像 OSS 下载 URL（公开接口，根据 spectrum_mode 返回不同数量的 URL）"""
    return image_urls


@file_router.get("/{file_id}/metadata", response_model=FilePublic)
async def get_file_metadata(metadata: FileMetadataDep):
    """根据文件ID查询文件元数据"""
    return metadata


@file_router.put("/{file_id}/set_public")
async def set_file_public(result: SetFilePublicDep):
    """将文件设为公开"""
    return result


# @file_router.post("/test_api")
# async def test_api(request_id: str):
#     """测试接口仅供开发过程中快速测试功能使用"""
#     from background.experiment_background import get_fc_invoke_instance_id_by_request_id # pylint: disable=import-outside-toplevel
#     from background.experiment_background import get_fc_invoke_logs_by_request_id # pylint: disable=import-outside-toplevel
#     from background.experiment_background import get_fc_invoke_status  # pylint: disable=import-outside-toplevel
#     from background.experiment_background import parse_fc_invoke_logs # pylint: disable=import-outside-toplevel

#     instance_id = await get_fc_invoke_instance_id_by_request_id(request_id)
#     invoke_logs = await get_fc_invoke_logs_by_request_id(request_id)
#     invoke_status = await get_fc_invoke_status(request_id)
#     final_imzml_path, final_ibd_path = parse_fc_invoke_logs(invoke_logs)
#     return {"instance_id": instance_id, "invoke_logs": invoke_logs, "invoke_status": invoke_status, "final_imzml_path": final_imzml_path, "final_ibd_path": final_ibd_path}
