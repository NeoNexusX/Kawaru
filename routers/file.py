import asyncio

from fastapi import APIRouter
from models.file_models import FilePublic, FileOssUploadResponse
from models.filter_models import ListDataResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep, SimpleUploadDep,
    DownloadFileDep, RawDownloadDep, RawDownloadNoauthDep, PreflightDep,
    ListPublicFilesDep,
    OssUploadDep, FileMetadataDep, SetFilePublicDep, PatchFileMetadataDep,
    FilePublicIdDep
)
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
    """OSS 直传；FC 未回调的无 persistent tag 对象由 OSS 生命周期自动清理。"""
    return file_record


@file_router.post("/list_user_files", response_model=ListDataResponse)#查询用户文件
async def list_user_files(files: ListFilesDep):
    """查询用户文件"""
    return files


@file_router.get("/{public_id}/download_raw")#直接下载两个文件
async def download_file_raw(file_meta: RawDownloadDep):
    """RAW 直链下载（需鉴权）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    asyncio.create_task(incr_visit())
    return file_meta


@file_router.get("/{public_id}/download_raw_noauth")#免登录下载两个文件
async def download_file_raw_noauth(file_meta: RawDownloadNoauthDep):
    """RAW 直链下载（免登录）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    asyncio.create_task(incr_visit())
    return file_meta


@file_router.get("/{public_id}/download")#下载文件
async def download_file(file_meta: DownloadFileDep):#使用依赖注入的元数据创建文件流，创建文件流响应
    """下载文件"""
    asyncio.create_task(incr_visit())
    return file_meta


@file_router.delete("/{public_id}")#删除文件
async def delete_file(result: DeleteFileDep):
    """删除文件"""
    return result


@file_router.post("/list_files", response_model=ListDataResponse)#列出文件文件
async def list_files(list_file_response: ListPublicFilesDep):
    """查询文件列表"""
    asyncio.create_task(incr_visit())
    return list_file_response


@file_router.post("/upload", response_model=FileOssUploadResponse)#上传接口
async def upload_file(file_record: OssUploadDep):
    """上传文件至OSS存储"""
    return file_record


@file_router.get("/{public_id}/metadata", response_model=FilePublic)#只获取特定元素的信息
async def get_file_metadata(metadata: FileMetadataDep):
    """根据 public_id 查询文件元数据（需登录；不区分公开/私有，任意登录用户可看，匿名 401）"""
    return metadata


@file_router.get("/id/{file_id}/public_id", deprecated=True)#file_id 转 public_id(仅 80-95 历史数据兼容)
async def get_file_public_id_by_id(result: FilePublicIdDep):
    """根据内部 file_id 查询 public_id（兼容接口：需登录，仅 80-95 号历史数据，匿名 401）"""
    asyncio.create_task(incr_visit())
    return result


@file_router.put("/{public_id}/set_public")#设置文件为公开
async def set_file_public(result: SetFilePublicDep):
    """将文件设为公开"""
    return result


@file_router.patch("/{public_id}", response_model=FilePublic)#修改文件元数据
async def patch_file_metadata(metadata: PatchFileMetadataDep):
    """修改文件元数据（持有者；部分更新；公开状态走 set_public 专用接口）"""
    return metadata
