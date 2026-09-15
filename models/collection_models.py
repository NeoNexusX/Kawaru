#########################
# Collection Models - 文件集合（Collection）数据模型
# 两张表：
#   DataCollectionInDB  集合主表（含学术信息与样本/仪器汇总多值字段）
#   CollectionMember    集合-文件链接表（position 表达集合内顺序）
# 写法参照 models/link_models.py 的 UserFileLink（联合主键 + ondelete="CASCADE"）
#########################
from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, Column, Text
from sqlmodel import Field, SQLModel

from models.file_models import FilePublic
from tools import generate_public_id, utc_time


class DataCollectionInDB(SQLModel, table=True):
    """集合主表：一张表存集合元数据（表名 datacollection）"""

    __tablename__ = "datacollection" # type: ignore

    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    # 公开页 URL 标识：base62 随机串 16 位（62^16 ≈ 2^95），不可枚举（禁止用自增 id 对外）
    public_id: str = Field(default_factory=generate_public_id, max_length=16, index=True, unique=True)
    # 删除用户时级联删除其集合（成员行随集合级联删除），否则删除建过集合的用户会被外键约束拦截
    owner_id: int = Field(foreign_key="userindb.id", index=True, ondelete="CASCADE")

    name: str = Field(max_length=128)
    description: str = Field(default="", max_length=512)
    is_public: bool = Field(default=True, description="集合公开状态（第一期恒 True 且不可修改）")
    member_type: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, default=list),
        description="集合包含的数据类型（多值，如 MSI、H&E、ST）",
    )
    collection_type: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, default=list),
        description="集合的构成方式（多值，如 Serial sections、Time series）",
    )

    # 学术信息（全部可空/默认空）
    title: str = Field(default="", max_length=500, description="数据集标题")
    doi: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, default=list),
        description="DOI 列表；JSON 列无法建唯一索引，唯一性由应用层校验（任一 DOI 不得属于其他集合）",
    )
    access: list[str] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, default=list),
        description="DOI 访问链接列表",
    )
    journal_name: str = Field(default="", max_length=255, description="期刊名")
    publish_time: datetime | None = Field(default=None, description="文章发表时间（数据集对应文章；可空，纯展示不参与筛选）")
    abstract: str = Field(default="", sa_column=Column(Text, nullable=False, default=""), description="摘要")
    cite_information: str = Field(default="", sa_column=Column(Text, nullable=False, default=""), description="引用信息")

    # 样本信息汇总（多值 → JSON 列，成员文件取值的汇总，与 FileBase 的单值字段只对齐清单不复用代码）
    organism: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    organism_part: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    sample_stabilization: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    sample_growth_conditions: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    tissue_modification: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))

    # 仪器信息汇总（多值 → JSON 列；数值型仪器参数不汇总，需要时由成员文件实时聚合）
    polarity: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    ionisation_source: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))
    analyzer: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, default=list))

    created_at: datetime = Field(default_factory=utc_time, index=True)
    updated_at: datetime = Field(default_factory=utc_time)

    # 注意：成员数与总大小不落库，由成员实时聚合


class CollectionMember(SQLModel, table=True):
    """链接表：集合-文件多对多关系（表名 collectionmember）

    - position 放在链接表上——顺序是"文件 × 集合"关系的属性（同一文件在不同集合顺序不同）；
      语义为相对顺序，允许空洞（移除成员不重排）与重复（并发追加撞号），无唯一约束
    - added_by 记录添加人（开放添加后追溯来源）
    """

    collection_id: int | None = Field(
        default=None, foreign_key="datacollection.id", primary_key=True, ondelete="CASCADE"
    )
    file_id: int | None = Field(
        default=None, foreign_key="fileindb.file_id", primary_key=True, ondelete="CASCADE"
    )
    position: int = Field(default=0, description="集合内顺序：创建按序 / 追加末尾 / 调序全量重写")
    added_by: int | None = Field(
        default=None, foreign_key="userindb.id", ondelete="SET NULL", description="添加人（追溯用；添加人被删除时置空）"
    )
    added_at: datetime = Field(default_factory=utc_time)


#########################
# 请求 / 响应模型
#########################
class CollectionCreate(SQLModel):
    """创建集合请求：元数据 + 初始成员（file_ids 数组顺序即初始 position 1..n）"""

    name: str = Field(max_length=128)
    description: str = Field(default="", max_length=512)
    member_type: list[str] = Field(default_factory=list)
    collection_type: list[str] = Field(default_factory=list)
    title: str = Field(default="", max_length=500)
    doi: list[str] = Field(default_factory=list)
    access: list[str] = Field(default_factory=list)
    journal_name: str = Field(default="", max_length=255)
    publish_time: datetime | None = Field(default=None, description="文章发表时间")
    abstract: str = Field(default="")
    cite_information: str = Field(default="")
    organism: list[str] = Field(default_factory=list)
    organism_part: list[str] = Field(default_factory=list)
    sample_stabilization: list[str] = Field(default_factory=list)
    sample_growth_conditions: list[str] = Field(default_factory=list)
    tissue_modification: list[str] = Field(default_factory=list)
    polarity: list[str] = Field(default_factory=list)
    ionisation_source: list[str] = Field(default_factory=list)
    analyzer: list[str] = Field(default_factory=list)
    file_ids: list[int] = Field(default_factory=list, description="初始成员文件 ID，数组顺序即初始顺序")


class CollectionPatch(SQLModel):
    """部分更新集合元数据（仅 owner；只改请求中出现的字段；成员不在 PATCH 中修改）"""

    name: str | None = Field(default=None, max_length=128)
    description: str | None = Field(default=None, max_length=512)
    member_type: list[str] | None = Field(default=None)
    collection_type: list[str] | None = Field(default=None)
    title: str | None = Field(default=None, max_length=500)
    doi: list[str] | None = Field(default=None)
    access: list[str] | None = Field(default=None)
    journal_name: str | None = Field(default=None, max_length=255)
    publish_time: datetime | None = Field(default=None, description="文章发表时间")
    abstract: str | None = Field(default=None)
    cite_information: str | None = Field(default=None)
    organism: list[str] | None = Field(default=None)
    organism_part: list[str] | None = Field(default=None)
    sample_stabilization: list[str] | None = Field(default=None)
    sample_growth_conditions: list[str] | None = Field(default=None)
    tissue_modification: list[str] | None = Field(default=None)
    polarity: list[str] | None = Field(default=None)
    ionisation_source: list[str] | None = Field(default=None)
    analyzer: list[str] | None = Field(default=None)


class CollectionMembersOperation(SQLModel):
    """成员批量操作请求体（加成员 / 调序共用：全量 [file_id] 数组）"""

    file_ids: list[int] = Field(default_factory=list)


class CollectionPublic(SQLModel):
    """集合公开返回骨架（公开端与管理端共用字段；成员数与总大小由成员实时聚合）

    不含内部数字 id：公开页禁止暴露自增 id（可枚举），对外标识一律用 public_id；
    管理端响应在此基础上扩展 CollectionOwnedPublic / CollectionOwnedDetail。
    """

    public_id: str
    owner_id: int
    owner_username: str = Field(default="", description="owner 用户名（公开页发起加文件时前端展示用）")
    name: str
    description: str
    member_type: list[str]
    collection_type: list[str]
    title: str
    doi: list[str]
    access: list[str]
    journal_name: str
    publish_time: datetime | None = Field(default=None, description="文章发表时间（数据集对应文章）")
    abstract: str
    cite_information: str
    organism: list[str]
    organism_part: list[str]
    sample_stabilization: list[str]
    sample_growth_conditions: list[str]
    tissue_modification: list[str]
    polarity: list[str]
    ionisation_source: list[str]
    analyzer: list[str]
    member_count: int = Field(default=0, description="成员数（实时聚合）")
    total_size: int = Field(default=0, description="成员文件总大小（实时聚合）")
    created_at: datetime
    updated_at: datetime


class CollectionDetail(CollectionPublic):
    """公开页集合详情响应：公开骨架 + 成员文件列表（按 position 排序，不含数字 id）"""

    members: list[FilePublic] = Field(default_factory=list)


class CollectionOwnedPublic(CollectionPublic):
    """管理端列表项：公开骨架 + 内部数字 id（前端用数字 id 调管理端接口；登录态接口，无枚举风险）"""

    id: int


class CollectionOwnedDetail(CollectionOwnedPublic):
    """管理端集合详情响应：管理端骨架 + 成员文件列表（按 position 排序）"""

    members: list[FilePublic] = Field(default_factory=list)
