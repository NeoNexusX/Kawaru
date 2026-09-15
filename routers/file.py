import asyncio

from fastapi import APIRouter
from models.file_models import FilePublic, FileOssUploadResponse
from models.filter_models import ListDataResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep,
    DownloadFileDep, RawDownloadDep, RawDownloadNoauthDep, PreflightDep,
    ListPublicFilesDep, GetPublicFileDep,
    OssUploadDep, FileMetadataDep, SetFilePublicDep, PatchFileMetadataDep
)
from database.visit_stats import incr_visit

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 公开端（免登录）——先于 /{file_id} 声明，避免路径被动态段吞掉
########################

@file_router.get("/public/{public_id}", response_model=FilePublic)
async def get_public_file(file_record: GetPublicFileDep):
    """公开文件详情（免登录，凭 public_id；非公开文件返回 404）"""
    return file_record

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
    """OSS 直传；FC 未回调的无 persistent tag 对象由 OSS 生命周期自动清理。"""
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
    return file_meta


@file_router.delete("/{file_id}")
async def delete_file(result: DeleteFileDep):
    """删除文件"""
    return result


@file_router.post("/list_files", response_model=ListDataResponse)
async def list_files(list_file_response: ListPublicFilesDep):
    """查询文件列表"""
    asyncio.create_task(incr_visit())
    return list_file_response


@file_router.post("/upload", response_model=FileOssUploadResponse)
async def upload_file(file_record: OssUploadDep):
    """上传文件至OSS存储"""
    return file_record


@file_router.get("/{file_id}/metadata", response_model=FilePublic)
async def get_file_metadata(metadata: FileMetadataDep):
    """根据文件ID查询文件元数据"""
    return metadata


@file_router.put("/{file_id}/set_public")
async def set_file_public(result: SetFilePublicDep):
    """将文件设为公开"""
    return result


@file_router.patch("/{file_id}", response_model=FilePublic)
async def patch_file_metadata(metadata: PatchFileMetadataDep):
    """修改文件元数据（持有者；部分更新；公开状态走 set_public 专用接口）"""
    return metadata
