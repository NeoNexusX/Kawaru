"""
ImzML 实验数据模型
"""
from datetime import datetime
from sqlmodel import Field, SQLModel
from models.file_models import now_local_timezone


class ExperimentImzMLBase(SQLModel):
    """ImzML 实验基础模型"""
    experiment_name: str = Field(min_length=1, max_length=255, description="实验名称")
    user_id: int = Field(gt=0, description="关联用户ID", index=True)
    source_file_id: int = Field(gt=0, description="原始ZIP文件ID", index=True)


class ExperimentImzMLInDB(ExperimentImzMLBase, table=True):
    """ImzML 实验数据库模型"""
    __tablename__ = "experiment_imzml"
    
    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    
    # 核心文件信息
    imzml_path: str = Field(default="", description=".imzML 文件的绝对存储路径")
    imzml_size: int = Field(default=0, ge=0, description=".imzML 文件大小（字节）")
    ibd_path: str = Field(default="", description=".ibd 文件的绝对存储路径")
    ibd_size: int = Field(default=0, ge=0, description=".ibd 文件大小（字节）")
    
    # 元数据
    created_at: datetime = Field(default_factory=now_local_timezone, description="创建时间")
    status: str = Field(default="processing", description="处理状态: processing, completed, failed")
    error_message: str = Field(default="", description="错误信息（失败时）")


class ExperimentImzMLPublic(ExperimentImzMLBase):
    """ImzML 实验公开模型"""
    id: int
    imzml_size: int
    ibd_size: int
    created_at: datetime
    status: str
    error_message: str


class ExperimentImzMLCreate(SQLModel):
    """创建 ImzML 实验请求模型"""
    experiment_name: str
    source_file_id: int