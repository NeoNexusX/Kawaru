from fastapi import APIRouter, Depends
from models.file_models import FilePublic, FileOssUploadResponse
from models.filter_models import ListDataResponse
from api.file_api import (
    DeleteFileDep, ListFilesDep,
    RawDownloadDep, RawDownloadNoauthDep, PreflightDep,
    ListPublicFilesDep,
    UploadFileDep, FileMetadataDep, SetFilePublicDep, PatchFileMetadataDep,
    RelayDownloadDep, relay_download_file
)
from database.visit_stats import count_visit

file_router = APIRouter(prefix="/files", tags=["file manager"]) # 挂接了文件管理相关的api

########################
# 文件管理 API
#########################

@file_router.post("/preflight", response_model=FilePublic, dependencies=[Depends(count_visit)])
async def preflight_file(file_record: PreflightDep):
    """上传预检"""
    return file_record


@file_router.post("/list_user_files", response_model=ListDataResponse)#查询用户文件
async def list_user_files(files: ListFilesDep):
    """查询用户文件"""
    return files


@file_router.get("/{public_id}/download_raw", dependencies=[Depends(count_visit)])#直接下载两个文件
async def download_file_raw(file_meta: RawDownloadDep):
    """RAW 直链下载（需鉴权）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    return file_meta


@file_router.get("/{public_id}/download_raw_noauth", dependencies=[Depends(count_visit)])#免登录下载两个文件
async def download_file_raw_noauth(file_meta: RawDownloadNoauthDep):
    """RAW 直链下载（免登录）——返回 imzML + ibd 两个预签名 URL，毫秒级、零轮询"""
    return file_meta


@file_router.get("/{public_id}/download_relay", dependencies=[Depends(count_visit)])#中转下载
async def download_file_relay(file_meta: RelayDownloadDep):
    """中转下载（需登录）——返回两个指向后端的中转 URL，经服务器流式透传，免 OSS 公网流量"""
    return file_meta


# 中转下载流式透传：返回文件字节流（StreamingResponse），FastAPI 不允许对 Response 子类
# 使用 Depends 注入，故直接用 add_api_route 注册带完整签名的处理函数。
# HEAD 需显式声明：FastAPI 的 APIRoute 不会像 Starlette 那样在有 GET 时自动补 HEAD。
file_router.add_api_route(
    "/{public_id}/relay/download",
    relay_download_file,
    methods=["GET", "HEAD"],
)


@file_router.delete("/{public_id}")#删除文件
async def delete_file(result: DeleteFileDep):
    """删除文件"""
    return result


@file_router.post("/list_files", response_model=ListDataResponse, dependencies=[Depends(count_visit)])#列出文件文件
async def list_files(list_file_response: ListPublicFilesDep):
    """查询文件列表"""
    return list_file_response


@file_router.post("/upload", response_model=FileOssUploadResponse)#上传接口
async def upload_file(file_record: UploadFileDep):
    """按预检记录的存储类型签发上传凭证（当前仅 OSS：返回 STS，前端直传；FC 未回调的无 persistent tag 对象由 OSS 生命周期自动清理）"""
    return file_record


@file_router.get("/{public_id}/metadata", response_model=FilePublic)#只获取特定元素的信息
async def get_file_metadata(metadata: FileMetadataDep):
    """根据 public_id 查询文件元数据（需登录；不区分公开/私有，任意登录用户可看，匿名 401）"""
    return metadata


@file_router.put("/{public_id}/set_public")#设置文件为公开
async def set_file_public(result: SetFilePublicDep):
    """将文件设为公开"""
    return result


@file_router.patch("/{public_id}", response_model=FilePublic)#修改文件元数据
async def patch_file_metadata(metadata: PatchFileMetadataDep):
    """修改文件元数据（持有者；部分更新；公开状态走 set_public 专用接口）"""
    return metadata
