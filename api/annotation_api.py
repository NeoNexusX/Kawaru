#########################
# Annotation API - SI 注释数据库接口层
# 职责: 参数声明与校验、登录闸、Redis 缓存中转 URL、调用 service/relay
# 说明: 浏览三个入口 + 取下载 URL 的入口均需登录；流式端点靠 URL 内嵌 token
#########################
from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Depends, HTTPException, Query, Request, Response

from api.user_api import TokenDep
from api_services.annotation_service import get_annotation_list
from api_services.relay_service import (
    get_si_db_oss_key,
    head_si_db_size,
    issue_si_db_relay_token,
    resolve_relay_base_url,
    stream_si_db_csv,
)
from constants.file_constants import DOWNLOAD_CACHE_TTL_SECONDS
from database.db import AsyncSessionDep
from database.redis_client import redis_client
from models.annotation_models import AnnotationFilter
from models.file_models import OssDownloadFile
from models.filter_models import ListDataResponse
from tools.logger import get_logger

logger = get_logger("api.annotation")


async def get_si_db_list(
    filters: AnnotationFilter,
    async_session: AsyncSessionDep,
    current_user: TokenDep,
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    sort_by: str = Query("entry_key", pattern="^(entry_key|name|mass|origin)$"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
) -> ListDataResponse:
    """【SI 注释库分页列表（需登录）】——查询侧唯一端点

    流程：
    1. 登录闸（current_user 仅作门槛，不参与查询）
    2. 筛选：每字段单值模糊（LIKE '%v%'），不同字段 AND；Mass 可选区间
    3. 分页：后端算总数/总页数，恒定附加 id ASC tiebreak 保证翻页稳定

    注意：
    - 分页参数 page≥1、1≤size≤100；排序白名单 sort_by/order，
      越界由 FastAPI 返回 422（与 get_file_list 口径一致）
    - filters 为必填 body（与文件列表一致）：不筛选时前端传空对象 {}
    - 返回完整一行（含 InChI）：单接口即满足浏览，前端无需再拉详情/meta
    - Origin 下拉的 4 个取值由前端写死（endogenous/exogenous/expected/unknown）
    """
    logger.debug(f"si_db list request user={current_user.id} page={page} size={size}")
    return await get_annotation_list(async_session, filters, page, size, sort_by, order)


async def get_si_db_download(
    current_user: TokenDep,
    request: Request,
) -> OssDownloadFile:
    """【SI 注释库下载（需登录）】返回 1 条指向后端的中转 URL（内嵌短时 token）

    流程：
    1. 登录闸 + 配置闸：oss_si_db_key 未配置 → 503（浏览不受影响）
    2. Redis 缓存命中 → 直接返回；未命中 → head_object 拿真实大小（兼验证对象存在）
       → 拼 URL → 写缓存
    3. token 由本接口签发，浏览器直链下载不带 Authorization 头，靠 URL 内嵌 token 鉴权

    注意：
    - 缓存不区分用户：TTL 内所有登录用户拿到同一条 URL（与 raw_dl_relay 同构），
      TTL 取 DOWNLOAD_CACHE_TTL_SECONDS，恒早于 token 有效期
    - 缓存键带 oss_key（si_db_dl_relay:<key>）：改配置换路径即换键，重启后
      不会命中旧文件的 size/filename；原地覆盖同一路径时需运维删该键（§15 第 6 条）
    - 缓存值内嵌 base_url：配置了 public_base_url 时无影响；回退模式下
      首次请求的 host 会被固化进缓存（与文件中转一致）
    """
    oss_key = get_si_db_oss_key()
    if not oss_key:
        raise HTTPException(status_code=503, detail="注释库文件未配置，请联系管理员")

    cache_key = f"si_db_dl_relay:{oss_key}"
    cached = await redis_client.get(cache_key)
    if cached:
        return OssDownloadFile.model_validate_json(cached)

    size = await head_si_db_size()
    if size is None:
        logger.error("si_db download metadata failed: object missing or head failed key=%s", oss_key)
        raise HTTPException(status_code=404, detail="注释库文件不存在，请联系管理员检查配置")

    base_url = resolve_relay_base_url(str(request.base_url))
    token = issue_si_db_relay_token()
    url = f"{base_url}/annotations/si-db/relay/download?token={token}"

    response = OssDownloadFile(filename=Path(oss_key).name, url=url, size=size)
    await redis_client.set(cache_key, response.model_dump_json(), ex=DOWNLOAD_CACHE_TTL_SECONDS)

    logger.info(f"si_db download issued user={current_user.id} filename={response.filename} size={size}")
    return response


async def si_db_relay_download(
    request: Request,
    token: str = Query(...),
) -> Response:
    """【SI 注释库中转流式透传】

    功能：验 token → 读配置 oss_si_db_key → 内网拉取 → 边拉边发（不落盘）。
    时机：前端/浏览器拿到中转 URL 后直接 GET 本接口。

    注意：
    - 返回的是 CSV 字节流（OSS 原对象原样，含 BOM），不是 JSON
    - Range 头原样透传给 OSS，支持断点续传与多线程下载器
    - HEAD 只返回头部信息（不拉正文）
    - token 由 /download 接口签发，只发给已登录用户
    """
    return await stream_si_db_csv(
        token,
        range_header=request.headers.get("range"),
        method=request.method,
    )


SiDbListDep = Annotated[ListDataResponse, Depends(get_si_db_list)]  # 注释库分页列表依赖
SiDbDownloadDep = Annotated[OssDownloadFile, Depends(get_si_db_download)]  # 注释库下载依赖
