from fastapi import APIRouter, Depends

from api.annotation_api import (
    SiDbDownloadDep,
    SiDbListDep,
    si_db_relay_download,
)
from database.visit_stats import count_visit
from models.filter_models import ListDataResponse

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


# 注释库中转流式透传：返回文件字节流（StreamingResponse），FastAPI 不允许对 Response 子类
# 使用 Depends 注入，故直接用 add_api_route 注册带完整签名的处理函数（同 routers/file.py）。
# GET/HEAD 拆成两条路由注册：共用一条（methods=["GET","HEAD"]）会让两个 operation 共享
# 同一个 operationId（FastAPI 的 unique_id 只取 methods[0]），触发 Duplicate Operation ID 警告；
# HEAD 仅供下载工具探测用，不进 OpenAPI 文档（include_in_schema=False）。
annotation_router.add_api_route(
    "/si-db/relay/download",
    si_db_relay_download,
    methods=["GET"],
)
annotation_router.add_api_route(
    "/si-db/relay/download",
    si_db_relay_download,
    methods=["HEAD"],
    include_in_schema=False,
)
