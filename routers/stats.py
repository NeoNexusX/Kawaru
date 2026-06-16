"""
统计路由
"""
from fastapi import APIRouter

from api.stats_api import (
    ProcessingStatsDep,
    DownloadStatsDep,
    ClassificationStatsDep,
    UserClassificationStatsDep,
)

stats_router = APIRouter(prefix="/stats", tags=["stats"])


@stats_router.get("/processing", response_model=dict[str, int])
async def get_processing_stats(result: ProcessingStatsDep):
    """获取处理任务各状态的数量统计（processing/completed/failed）"""
    return result


@stats_router.get("/downloads", response_model=dict[str, int])
async def get_download_stats(result: DownloadStatsDep):
    """获取所有文件的总下载次数"""
    return result


@stats_router.get("/files/classification", response_model=dict)
async def get_classification_stats(result: ClassificationStatsDep):
    """按指定字段统计文件分类数量（前端通过 field 参数指定字段）"""
    return result


@stats_router.get("/users/classification", response_model=dict)
async def get_user_classification_stats(result: UserClassificationStatsDep):
    """按指定字段统计用户分类数量（前端通过 field 参数指定字段），同时返回 total_users"""
    return result
