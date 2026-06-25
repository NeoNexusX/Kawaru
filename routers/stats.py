"""
统计路由
"""
from fastapi import APIRouter

from api.stats_api import (
    ProcessingStatsDep,
    ClassificationStatsDep,
    UserClassificationStatsDep,
    VisitStatsDep,
    OverviewStatsDep,
)

stats_router = APIRouter(prefix="/stats", tags=["stats"])


@stats_router.get("/overview", response_model=dict[str, int])
async def get_overview_stats(result: OverviewStatsDep):
    """获取平台概览统计（用户总数、文件总数、下载总次数）"""
    return result


@stats_router.get("/processing", response_model=dict[str, int])
async def get_processing_stats(result: ProcessingStatsDep):
    """获取当前用户处理任务各状态的数量统计（processing/completed/failed）"""
    return result


@stats_router.get("/files/classification", response_model=dict)
async def get_classification_stats(result: ClassificationStatsDep):
    """按指定字段统计文件分类数量（前端通过 field 参数指定字段）"""
    return result


@stats_router.get("/users/classification", response_model=dict)
async def get_user_classification_stats(result: UserClassificationStatsDep):
    """按指定字段统计用户分类数量（前端通过 field 参数指定字段）"""
    return result


@stats_router.get("/visits", response_model=dict[str, int])
async def get_visit_stats(result: VisitStatsDep):
    """获取全站访问统计（当日/当月/总计）"""
    return result
