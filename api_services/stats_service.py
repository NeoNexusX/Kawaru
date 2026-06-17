"""
Stats Service — 统计业务逻辑层
职责：各类数据统计查询。无 FastAPI 依赖。
"""
from __future__ import annotations

from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus
from models.file_models import FileInDB
from models.user_models import UserInDB
from tools.logger import get_logger

logger = get_logger("api_services.stats_service")


#########################
# 处理任务统计
#########################

async def get_processing_status_counts(
    async_session: AsyncSession,
) -> dict[str, int]:
    """统计各状态的处理任务数量"""
    result = {}
    for status in ProcessingStatus:
        count = (await async_session.exec(
            select(func.count()).where(ExperimentProcessRunInDB.status == status))).one() # pylint: disable=not-callable
        result[status.value] = count
    return result


#########################
# 文件分类统计
#########################

async def get_file_classification_counts(
    async_session: AsyncSession,
    field: str,
) -> dict:
    """
    按指定字段统计文件分类数量

    Returns:
        {字段值: 文件数量} 的字典
    """
    # 按字段分组统计
    classify_column = getattr(FileInDB, field)
    rows = (await async_session.exec(
        select(classify_column, func.count()) # pylint: disable=not-callable
        .where(classify_column != "")
        .group_by(classify_column))).all()

    return {row[0]: row[1] for row in rows}


#########################
# 用户统计
#########################

async def get_user_classification_counts(
    async_session: AsyncSession,
    field: str,
) -> dict:
    """
    按指定字段统计用户分类数量

    Returns:
        {字段值: 用户数量} 的字典
    """
    # 按字段分组统计
    classify_column = getattr(UserInDB, field)
    rows = (await async_session.exec(
        select(classify_column, func.count()) # pylint: disable=not-callable
        .where(classify_column != "")
        .group_by(classify_column))).all()

    return {row[0]: row[1] for row in rows}


#########################
# 概览统计
#########################

async def get_overview_stats(
    async_session: AsyncSession,
) -> dict[str, int]:
    """获取平台概览统计：用户总数、文件总数、下载总次数"""
    total_users = (await async_session.exec(
        select(func.count()).select_from(UserInDB))).one()  # pylint: disable=not-callable

    total_files = (await async_session.exec(
        select(func.count()).select_from(FileInDB))).one()  # pylint: disable=not-callable

    total_downloads = (await async_session.exec(
        select(func.sum(FileInDB.download_count)))).one()

    return {
        "total_users": total_users or 0,
        "total_files": total_files or 0,
        "total_downloads": total_downloads or 0,
    }
