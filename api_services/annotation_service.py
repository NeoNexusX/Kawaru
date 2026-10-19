#########################
# Annotation Service - SI 注释数据库查询
# 职责: 单值模糊筛选 → SQL 子句、排序 tiebreak、分页查询
# 说明: 全部只读；表里只有"当前那份 CSV"的数据，无任何版本逻辑（无 join、无子查询）
#########################
from __future__ import annotations

from typing import Any

from sqlmodel import and_, col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from api_services.user_service import count_items
from exceptions.common_exceptions import internal_server_error_exception
from models.annotation_models import (
    AnnotationEntryInDB,
    AnnotationEntryPublic,
    AnnotationFilter,
)
from models.filter_models import ListDataResponse, PaginationMeta
from tools.logger import get_logger

logger = get_logger("api_services.annotation")

# 单值模糊字段：统一 LIKE '%v%'（库排序规则 utf8mb4_unicode_ci，大小写不敏感）
FUZZY_FILTER_FIELDS = ("entry_key", "name", "formula", "origin")

# 排序白名单（取值合法性由接口层 Query pattern 约束，这里兜底映射到默认列）
_SORT_COLUMNS = {
    "entry_key": AnnotationEntryInDB.entry_key,
    "name": AnnotationEntryInDB.name,
    "mass": AnnotationEntryInDB.mass,
    "origin": AnnotationEntryInDB.origin,
}


def build_annotation_filters(filters: AnnotationFilter) -> list[Any]:
    """筛选子句：4 个单值模糊（LIKE '%v%'）+ Mass 区间；空串/None 不产生条件

    不同字段之间的 AND 由调用方 and_(*clauses) 完成（无字段匹配时调用方不加 filter）。
    """
    clauses: list[Any] = []
    for field_name in FUZZY_FILTER_FIELDS:
        value = getattr(filters, field_name)
        if value and value.strip():
            clauses.append(col(getattr(AnnotationEntryInDB, field_name)).like(f"%{value.strip()}%"))
    if filters.mass_min is not None:
        clauses.append(col(AnnotationEntryInDB.mass) >= filters.mass_min)
    if filters.mass_max is not None:
        clauses.append(col(AnnotationEntryInDB.mass) <= filters.mass_max)
    return clauses


def build_annotation_order(sort_by: str, order: str) -> tuple[Any, Any]:
    """排序子句：sort_by/order 映射为排序列 + id 同向兜底

    同值行之间按 id 排序，保证翻页不重不漏（name/mass 存在并列值）。
    兜底方向必须与主排序一致：InnoDB 二级索引是 (col, id) 升序结构，
    只有全 ASC 或全 DESC 才能整体正向/反向扫描；方向相反会退化为 filesort。
    """
    column = _SORT_COLUMNS.get(sort_by, AnnotationEntryInDB.entry_key)
    descending = order != "asc"
    primary = col(column).desc() if descending else col(column).asc()  # pylint: disable=no-member
    tiebreak = (
        col(AnnotationEntryInDB.id).desc()  # pylint: disable=no-member
        if descending
        else col(AnnotationEntryInDB.id).asc()  # pylint: disable=no-member
    )
    return (primary, tiebreak)


async def get_annotation_list(
    async_session: AsyncSession,
    filters: AnnotationFilter,
    page: int,
    size: int,
    sort_by: str,
    order: str,
) -> ListDataResponse:
    """分页查询注释条目：筛选 → 精确总数 → offset/limit 取页（范式照抄 get_file_list）"""
    try:
        query = select(AnnotationEntryInDB)
        clauses = build_annotation_filters(filters)
        if clauses:
            query = query.filter(and_(*clauses))

        total_records = await count_items(async_session, query, AnnotationEntryInDB)
        total_pages = (total_records + size - 1) // size
        offset = (page - 1) * size

        data = (
            await async_session.exec(
                query.order_by(*build_annotation_order(sort_by, order)).offset(offset).limit(size)
            )
        ).all()

        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(data),
            total_pages=total_pages,
            total_records=total_records,
        )
        return ListDataResponse(
            meta=pagination_meta,
            data=[AnnotationEntryPublic(**row.model_dump()) for row in data],
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"failed to get_annotation_list: {e}")
        raise internal_server_error_exception from e
