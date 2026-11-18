"""访问统计的公开响应模型。"""

from datetime import date as Date
from typing import Literal

from pydantic import BaseModel, Field

from constants.stats_constants import VISIT_HISTORY_MAX_DAYS


class VisitHistoryPoint(BaseModel):
    date: Date = Field(description="UTC 自然日，YYYY-MM-DD")
    count: int = Field(ge=0, description="接入访问统计的接口请求次数；缺失时补 0")
    missing: bool = Field(description="Redis 未保存该日计数；不能区分无访问和数据缺失")
    is_partial: bool = Field(description="是否为尚未结束的 UTC 当天")


class VisitHistoryResponse(BaseModel):
    timezone: Literal["UTC"] = "UTC"
    start_date: Date = Field(description="起始日期，包含当日")
    end_date: Date = Field(description="结束日期，包含当日")
    days: int = Field(ge=1, le=VISIT_HISTORY_MAX_DAYS)
    range_total: int = Field(ge=0, description="本次查询区间的计数之和，不是全站历史累计")
    missing_days: int = Field(ge=0, description="区间内缺少 Redis 日计数的天数")
    items: list[VisitHistoryPoint] = Field(
        min_length=1,
        max_length=VISIT_HISTORY_MAX_DAYS,
        description="按日期升序排列，固定返回 days 条，含缺失日期",
    )
