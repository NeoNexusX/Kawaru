"""
统计路由
"""
from fastapi import APIRouter

from api.stats_api import (
    ClassificationStatsDep,
    OverviewStatsDep,
    ProcessingStatsDep,
    UserClassificationStatsDep,
    VisitHistoryDep,
    VisitStatsDep,
)
from models.stats_models import VisitHistoryResponse

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


@stats_router.get(
    "/visits/history",
    response_model=VisitHistoryResponse,
    responses={
        400: {"description": "查询区间超出最近 90 个 UTC 自然日（含当天）"},
        500: {"description": "统计数据损坏或内部错误"},
        503: {"description": "Redis 统计暂不可用，请稍后重试"},
    },
)
async def get_visit_history(result: VisitHistoryDep) -> VisitHistoryResponse:
    """获取历史访问请求次数，免登录，按 UTC 日期升序返回。

    仅统计接入 count_visit 的接口请求，不代表独立访客数 UV 或页面 PV。
    缺少计数的日期补 0 并标记 missing；UTC 当天标记 is_partial。
    读取不增加访问计数，不刷新 Redis TTL。
    """
    return result
