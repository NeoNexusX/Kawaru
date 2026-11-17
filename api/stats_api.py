"""
Stats API — 统计 API 入口层
"""
from datetime import date, timedelta
from typing import Annotated

from fastapi import Depends, Query
from redis.exceptions import RedisError

from api.user_api import token_authen
from api_services.stats_service import (
    get_file_classification_counts,
    get_overview_stats,
    get_processing_status_counts,
    get_user_classification_counts,
)
from constants.stats_constants import (
    ALLOWED_FILE_CLASSIFY_FIELDS,
    ALLOWED_USER_CLASSIFY_FIELDS,
    VISIT_HISTORY_MAX_DAYS,
)
from database.db import AsyncSessionDep
from database.visit_stats import get_visit_history_counts, get_visit_stats
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.stats_exceptions import (
    invalid_file_classify_field_exception,
    invalid_user_classify_field_exception,
    invalid_visit_history_range_exception,
    visit_stats_unavailable_exception,
)
from models.stats_models import VisitHistoryPoint, VisitHistoryResponse
from models.user_models import UserInDB
from tools.logger import get_logger
from tools.time_tools import utc_time

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


async def get_visit_history_endpoint(
    days: Annotated[
        int,
        Query(ge=1, le=VISIT_HISTORY_MAX_DAYS, description="查询天数，包含结束日期"),
    ] = 30,
    end_date: Annotated[
        date | None,
        Query(description="结束日期（YYYY-MM-DD），默认 UTC 当天；整个区间须在最近 90 天内"),
    ] = None,
) -> VisitHistoryResponse:
    """公开读取 UTC 日计数；只采样一次当前日期，避免请求跨午夜时口径不一致。"""
    today = utc_time().date()
    end_date = end_date or today
    earliest_date = today - timedelta(days=VISIT_HISTORY_MAX_DAYS - 1)
    # 先拒绝过早的结束日期，再做减法，避免 date.min 等输入触发下溢。
    if not earliest_date <= end_date <= today:
        raise invalid_visit_history_range_exception
    start_date = end_date - timedelta(days=days - 1)
    if start_date < earliest_date:
        raise invalid_visit_history_range_exception

    try:
        counts = await get_visit_history_counts(start_date, days)
        items = [
            VisitHistoryPoint(
                date=start_date + timedelta(days=offset),
                count=count if count is not None else 0,
                missing=count is None,
                is_partial=start_date + timedelta(days=offset) == today,
            )
            for offset, count in enumerate(counts)
        ]
        return VisitHistoryResponse(
            start_date=start_date,
            end_date=end_date,
            days=days,
            range_total=sum(item.count for item in items),
            missing_days=sum(item.missing for item in items),
            items=items,
        )
    except RedisError as exc:
        logger.error("failed to get_visit_history: %s", exc)
        raise visit_stats_unavailable_exception from exc
    except Exception as exc:
        logger.error("failed to get_visit_history: %s", exc)
        raise internal_server_error_exception from exc


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
VisitHistoryDep = Annotated[VisitHistoryResponse, Depends(get_visit_history_endpoint)]
OverviewStatsDep = Annotated[dict[str, int], Depends(get_overview_stats_endpoint)]
