"""
实验数据模型
"""
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlmodel import Field, SQLModel, Relationship, Column, Integer, ForeignKey, BigInteger
from tools import utc_time
from boot.config import get_config

if TYPE_CHECKING:
    from models.processing_models import ExperimentProcessRunInDB

config = get_config()
storage_config = config.storage


#########################
# 实验状态枚举
#########################

class ExperimentStatus(str, Enum):
    """实验处理状态枚举"""
    PROCESSING = "processing"  # 处理中
    COMPLETED = "completed"    # 已完成
    FAILED = "failed"          # 失败


#########################
# 实验基类(所有实验类型继承此类)
#########################

class ExperimentBase(SQLModel):
    """
    实验公共基类
    所有实验类型(imzML、raw 等)都应继承此类
    注意：实验通过 source_file_id 关联文件，多用户共享同一文件时共享同一实验记录
    """
    experiment_name: str = Field(min_length=1, max_length=255, description="实验名称")
    created_at: datetime = Field(default_factory=utc_time, description="创建时间")
    status: str = Field(default=ExperimentStatus.PROCESSING, description="处理状态: processing, completed, failed")
    error_message: str = Field(default="", description="错误信息(失败时)")


class ExperimentPublic(SQLModel):
    """
    实验公共返回模型
    用于 API 返回，包含所有实验类型的公共字段
    """
    id: int
    experiment_name: str
    source_file_id: int
    created_at: datetime
    status: str
    error_message: str


#########################
# ImzML 原始数据模型(算法处理前的原始落库记录)
#########################

class OriginImzMLInDB(ExperimentBase, table=True):
    """ImzML 原始数据数据库模型(存放上传/解压后的 imzML/ibd 路径，供后续算法处理使用)"""
    __tablename__ = "origin_imzml" #type:ignore
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})

    # 外键关联 FileInDB，数据库层面配置级联删除
    source_file_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("fileindb.file_id", ondelete="CASCADE"),  # 数据库层：级联删除
            nullable=False,
            index=True,
            unique=True
        ),
        description="原始ZIP文件ID(外键)"
    )

    # 核心文件信息
    imzml_path: str = Field(default="", description=".imzML 文件的绝对存储路径", max_length=getattr(storage_config, 'max_path_length', 255))
    imzml_size: int = Field(default=0, ge=0,sa_column=Column(BigInteger, nullable=False, default=0),description=".imzML 文件大小(字节)")
    ibd_path: str = Field(default="", description=".ibd 文件的绝对存储路径", max_length=getattr(storage_config, 'max_path_length', 255))
    ibd_size: int = Field(default=0, ge=0,sa_column=Column(BigInteger, nullable=False, default=0), description=".ibd 文件大小(字节)")
    # 元数据
    created_at: datetime = Field(default_factory=utc_time, description="创建时间")
    status: str = Field(default="processing", description="处理状态: processing, completed, failed", max_length=255)
    error_message: str = Field(default="", description="错误信息(失败时)", max_length=255)

    # ORM 层级联删除：删除 origin 时自动级联删除关联的 process_run
    process_runs: list["ExperimentProcessRunInDB"] = Relationship(
        back_populates="experiment",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",  # ORM 层：删除父记录时级联标记子记录
            "passive_deletes": True            # 告知 ORM：DB 已配置 CASCADE，无需逐行 DELETE
        }
    )

class OriginImzMLPublic(ExperimentPublic):
    """ImzML 原始数据公开模型(继承公共字段，添加 imzML 特有字段)"""
    imzml_size: int
    ibd_size: int
    status: str


#########################
# 预留：RAW 原始数据模型(示例)
#########################

# class OriginRawInDB(ExperimentBase, table=True):
#     """RAW 原始数据数据库模型"""
#     __tablename__ = "origin_raw"
#     id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
#     # RAW 特有字段
#     raw_path: str = Field(default="", description=".raw 文件的绝对存储路径")
#     raw_size: int = Field(default=0, ge=0, description=".raw 文件大小(字节)")

# class OriginRawPublic(ExperimentPublic):
#     """RAW 原始数据公开模型"""
#     raw_size: int
