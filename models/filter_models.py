from typing import Any
from sqlmodel import Field, SQLModel
from models.user_models import UserDetailPublic


class PaginationMeta(SQLModel):
    """分页元数据模型"""
    current_page: int = Field(description="当前页码")
    current_records: int = Field(description="当前页记录数")
    total_pages: int = Field(description="总页数")
    total_records: int = Field(description="总记录数")


class UserFilter(SQLModel):
    """用户筛选模型"""
    username: str | None = Field(default=None, description="用户名")
    active: bool | None = Field(default=None, description="是否激活")
    institution: str | None = Field(default=None, description="所属机构")
    region: str | None = Field(default=None, description="所属地区")


class FileFilter(SQLModel):
    """
    文件/实验筛选模型
    """
    filename: str | None = Field(default=None, description="文件名")
    experiment_type: str | None = Field(default=None, description="实验类型")
    username: str | None = Field(default=None, description="所属用户名")
    organism: str | None = Field(default=None, description="样本所属物种")
    organism_part: str | None = Field(default=None, description="样本来源组织")
    condition: str | None = Field(default=None, description="样本生长或处理条件")
    sample_stabilization: str | None = Field(default=None, description="样本稳定方式")
    tissue_modification: str | None = Field(default=None, description="组织修饰方式")
    maldi_matrix: str | None = Field(default=None, description="MALDI 基质")
    maldi_matrix_application: str | None = Field(default=None, description="基质施加方式")
    solvent: str | None = Field(default=None, description="使用溶剂")


class ListDataResponse(SQLModel):
    """数据列表响应模型"""
    meta: PaginationMeta = Field(description="分页元数据")
    data: list[Any] = Field(description="数据列表")
