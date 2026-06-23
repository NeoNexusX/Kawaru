"""
Stats API — 统计 API 入口层
"""
from typing import Annotated

from fastapi import Depends, Query

from api_services.stats_service import (
    get_processing_status_counts,
    get_file_classification_counts,
    get_user_classification_counts,
    get_overview_stats,
)
from api.user_api import token_authen
from constants.stats_constants import ALLOWED_FILE_CLASSIFY_FIELDS, ALLOWED_USER_CLASSIFY_FIELDS
from database.db import AsyncSessionDep
from database.visit_stats import get_visit_stats
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.stats_exceptions import invalid_file_classify_field_exception, invalid_user_classify_field_exception
from models.user_models import UserInDB
from tools.logger import get_logger

logger = get_logger("api.stats_api")


#########################
# 处理任务统计
#########################

async def get_processing_stats(
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict[str, int]:
    """获取当前用户处理任务各状态的数量统计"""
    try:
        assert current_user.id is not None
        return await get_processing_status_counts(async_session, user_id=current_user.id)
    except Exception as e:
        logger.error("failed to get_processing_stats: %s", e)
        raise internal_server_error_exception from e


#########################
# 文件分类统计
#########################

async def get_classification_stats(
    async_session: AsyncSessionDep,
    field: str = Query(..., description="分类字段名，可选值: " + ", ".join(sorted(ALLOWED_FILE_CLASSIFY_FIELDS))),
) -> dict:
    """按指定字段统计文件分类数量"""
    if field not in ALLOWED_FILE_CLASSIFY_FIELDS:
        raise invalid_file_classify_field_exception
    try:
        return await get_file_classification_counts(async_session, field)
    except Exception as e:
        logger.error("failed to get_classification_stats: %s", e)
        raise internal_server_error_exception from e


#########################
# 用户分类统计
#########################

async def get_user_classification_stats(
    async_session: AsyncSessionDep,
    field: str = Query(..., description="分类字段名，可选值: " + ", ".join(sorted(ALLOWED_USER_CLASSIFY_FIELDS))),
) -> dict:
    """按指定字段统计用户分类数量"""
    if field not in ALLOWED_USER_CLASSIFY_FIELDS:
        raise invalid_user_classify_field_exception
    try:
        return await get_user_classification_counts(async_session, field)
    except Exception as e:
        logger.error("failed to get_user_classification_stats: %s", e)
        raise internal_server_error_exception from e


#########################
# 访问统计
#########################

async def get_visit_stats_endpoint() -> dict[str, int]:
    """获取全站访问统计（当日/当月/总计）"""
    try:
        return await get_visit_stats()
    except Exception as e:
        logger.error("failed to get_visit_stats: %s", e)
        raise internal_server_error_exception from e


#########################
# 概览统计
#########################

async def get_overview_stats_endpoint(
    async_session: AsyncSessionDep,
) -> dict[str, int]:
    """获取平台概览统计（用户总数、文件总数、下载总次数）"""
    try:
        return await get_overview_stats(async_session)
    except Exception as e:
        logger.error("failed to get_overview_stats: %s", e)
        raise internal_server_error_exception from e


#########################
# 依赖注入类型别名
#########################

ProcessingStatsDep = Annotated[dict[str, int], Depends(get_processing_stats)]
ClassificationStatsDep = Annotated[dict, Depends(get_classification_stats)]
UserClassificationStatsDep = Annotated[dict, Depends(get_user_classification_stats)]
VisitStatsDep = Annotated[dict[str, int], Depends(get_visit_stats_endpoint)]
OverviewStatsDep = Annotated[dict[str, int], Depends(get_overview_stats_endpoint)]
