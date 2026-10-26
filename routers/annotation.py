from fastapi import APIRouter, Depends

from api.annotation_api import (
    SiDbDownloadDep,
    SiDbListDep,
    si_db_relay_download,
)
from database.visit_stats import count_visit
from models.filter_models import ListDataResponse
from routers.stream_route import add_stream_download_route

annotation_router = APIRouter(prefix="/annotations", tags=["annotation db"])  # 挂接了 SI 注释数据库相关的 api

########################
# SI 注释数据库 API
#########################

@annotation_router.post("/si-db/list", response_model=ListDataResponse, dependencies=[Depends(count_visit)])  # 注释库分页列表
async def list_si_db_entries(result: SiDbListDep):
    """SI 注释库分页列表（需登录）：单值模糊筛选 + 后端分页，返回完整行（含 InChI）"""
    return result


@annotation_router.get("/si-db/download", dependencies=[Depends(count_visit)])  # 注释库下载
async def download_si_db(file_meta: SiDbDownloadDep):
    """SI 注释库下载（需登录）——返回 1 条中转 URL（内嵌 token），经服务器流式透传"""
    return file_meta


# 注释库中转流式透传：返回 CSV 字节流，注册细节（GET/HEAD 双注册的原因）见 add_stream_download_route
add_stream_download_route(annotation_router, "/si-db/relay/download", si_db_relay_download)
