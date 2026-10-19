#########################
# Annotation Models - SI 注释数据库
# 单表 annotation_entry：CSV 导入后的可查询副本
# 说明：CSV 原文件在 OSS（路径见 config.yaml 的 oss_si_db_key），本表不存版本/路径信息
#########################
from __future__ import annotations

from sqlalchemy import BigInteger, Column, Double, Text
from sqlmodel import Field, SQLModel


class AnnotationEntryInDB(SQLModel, table=True):
    """SI 注释库条目（表名 annotation_entry）

    - 只存"当前那份 CSV"的内容；更新 = 导入工具在单事务内清空重写（tools/annotation_import.py）
    - mass 必须用 DOUBLE：MySQL 的 FLOAT 是单精度，会丢掉源 CSV 的 10 位小数
    - entry_key 唯一：源 CSV 的 HMDB ID 全不重复，唯一索引同时服务详情查询
    """

    __tablename__ = "annotation_entry"  # type: ignore

    id: int | None = Field(
        default=None,
        sa_column=Column(BigInteger, primary_key=True, autoincrement=True, nullable=False),
        description="自增主键（排序的稳定 tiebreak）",
    )
    entry_key: str = Field(
        max_length=16, index=True, unique=True, description="原始 ID（HMDB0000001）"
    )
    name: str = Field(max_length=255, index=True, description="化合物名称")
    formula: str = Field(max_length=16, description="分子式")
    mass: float = Field(
        sa_column=Column(Double, nullable=False, index=True),
        description="质量（源 CSV 最多 10 位小数，必须用 DOUBLE 而非 FLOAT）",
    )
    origin: str = Field(
        max_length=16,
        index=True,
        description="来源类别（endogenous / exogenous / expected / unknown）",
    )
    inchi: str = Field(
        sa_column=Column(Text, nullable=False),
        description="InChI 结构标识（最长 2133 字符）",
    )


#########################
# 请求 / 响应模型
#########################
class AnnotationFilter(SQLModel):
    """SI 注释库筛选模型：每字段单值 + 模糊匹配（LIKE '%v%'），空串/None = 不筛

    不继承 MultiValueFilter：本功能明确不需要多值归一化（先例：UserFilter）。
    mass 是数值无法模糊匹配，保留可选区间；不同字段之间 AND 由查询组装保证。
    """

    entry_key: str | None = Field(default=None, max_length=64, description="HMDB ID（模糊匹配）")
    name: str | None = Field(default=None, max_length=255, description="化合物名称（模糊匹配）")
    formula: str | None = Field(default=None, max_length=64, description="分子式（模糊匹配）")
    origin: str | None = Field(default=None, max_length=16, description="来源类别（模糊匹配）")
    mass_min: float | None = Field(default=None, description="质量区间下限（含）")
    mass_max: float | None = Field(default=None, description="质量区间上限（含）")


class AnnotationEntryPublic(SQLModel):
    """列表行模型：完整一行（含 InChI）——用户要求单接口返回全字段，前端不再单拉详情"""

    entry_key: str
    name: str
    formula: str
    mass: float
    origin: str
    inchi: str
