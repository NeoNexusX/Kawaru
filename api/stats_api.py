"""
Stats API — 统计 API 入口层
"""
from typing import Annotated

from fastapi import Depends, Query

from api_services.stats_service import (
    get_processing_status_counts,
    get_total_download_count,
    get_file_classification_counts,
    get_user_classification_counts,
)
from constants.stats_constants import ALLOWED_FILE_CLASSIFY_FIELDS, ALLOWED_USER_CLASSIFY_FIELDS
from database.db import AsyncSessionDep
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.stats_exceptions import invalid_file_classify_field_exception, invalid_user_classify_field_exception

from tools.logger import get_logger

logger = get_logger("api.stats_api")


async def get_processing_stats(
    async_session: AsyncSessionDep,
) -> dict[str, int]:
    """获取处理任务各状态的数量统计"""
    try:
        return await get_processing_status_counts(async_session)
    except Exception as e:
        logger.error("failed to get_processing_stats: %s", e)
        raise internal_server_error_exception from e


async def get_download_stats(
    async_session: AsyncSessionDep,
) -> dict[str, int]:
    """获取所有文件的总下载次数"""
    try:
        return await get_total_download_count(async_session)
    except Exception as e:
        logger.error("failed to get_download_stats: %s", e)
        raise internal_server_error_exception from e


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


# 依赖注入类型别名
ProcessingStatsDep = Annotated[dict[str, int], Depends(get_processing_stats)]
DownloadStatsDep = Annotated[dict[str, int], Depends(get_download_stats)]
ClassificationStatsDep = Annotated[dict, Depends(get_classification_stats)]
UserClassificationStatsDep = Annotated[dict, Depends(get_user_classification_stats)]
