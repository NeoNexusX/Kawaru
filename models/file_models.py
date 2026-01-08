'''
Author: xiongqiurong 1513401658@qq.com
Date: 2025-12-02 16:44:24
LastEditors: xiongqiurong 1513401658@qq.com
LastEditTime: 2026-01-07 13:41:41
FilePath: \Kawaru\models\file_models.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncGenerator
import aiofiles
from sqlmodel import Field, SQLModel
from fastapi.responses import StreamingResponse
from logger import get_logger
from sqlalchemy import Column
from sqlalchemy.types import JSON
from envset.config import get_config

# 获取服务器状态模块的日志器
logger = get_logger("main.file_control")

# 模型字段配置来自 config
config = get_config()
storage_config = config.storage
sample_config = config.sample_info

LOCAL_TIME_OFFSET = timedelta(hours=8)


def now_local_timezone() -> datetime:
    """返回东八区本地时间（无时区信息，便于 SQLite 存储）"""
    return datetime.now(timezone.utc) + LOCAL_TIME_OFFSET


class FileBase(SQLModel):
    """公共字段，供数据库与返回模型复用"""
    filename: str = Field(max_length=storage_config.max_filename_length)
    file_type: str = Field(default="", max_length=storage_config.max_file_type_length)
    # 【实验类型字段】用于定位对应的实验表（如 imzML/raw 等），由前端上传时指定
    experiment_type: str = Field(default="", description="实验类型（imzml/raw等），用于定位实验表", max_length=sample_config.max_experiment_type_length)
    size: int = Field(default=0)
    storage_type: str = Field(default="local", max_length=64)
    hash_sha256: str | None = Field(default=None, max_length=256)
    
    # 样本信息字段
    organism: str = Field(default="", description="样本所属物种", max_length=sample_config.max_organism_length)
    organism_part: str = Field(default="", description="样本来源组织", max_length=sample_config.max_organism_part_length)
    condition: str = Field(default="", description="样本状态", max_length=sample_config.max_condition_length)
    sample_growth_conditions: str = Field(default="", description="样本生长或处理条件（选填）", max_length=sample_config.max_sample_growth_conditions_length)
    sample_stabilisation: str = Field(default="", description="样本稳定方式", max_length=sample_config.max_sample_stabilisation_length)
    tissue_modification: str = Field(default="", description="组织修饰方式", max_length=sample_config.max_tissue_modification_length)
    maldi_matrix: str = Field(default="", description="MALDI 基质", max_length=sample_config.max_maldi_matrix_length)
    maldi_matrix_application: str = Field(default="", description="基质施加方式", max_length=sample_config.max_maldi_matrix_application_length)
    solvent: str = Field(default="", description="使用溶剂（选填）", max_length=sample_config.max_solvent_length)


class FilePublic(FileBase):
    """对外返回的文件信息"""
    file_id: int
    uploaded_at: datetime
    download_count: int


class FileInDB(FileBase, table=True):
    """数据库模型"""
    file_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})#文件编号（自增主键）
    path: str = Field(max_length=getattr(storage_config, 'max_path_length', 255)) #存储路径(长度限制取ext4文件系统限制路径最大长度255字符)
    owners: list[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))#文件所属用户ID列表JSON格式
    uploaded_at: datetime = Field(default_factory=now_local_timezone, index=True)#上传时间
    download_count: int = Field(default=0)#下载次数
    total_parts: int | None = Field(default=1)  # 分片上传的总分片数（仅用于分片上传）


class FileStreamingResponse(StreamingResponse):#是响应类模型 不能使用依赖注入
    """文件流式响应，继承自 StreamingResponse，使用 FileBase 的字段"""

    def __init__(self, file_path: Path, file_base: FileBase, chunk_size: int, media_type: str = "application/octet-stream"):
        
        async def file_generator() -> AsyncGenerator[bytes, None]:
            """
            Asynchronous generator that streams a file in fixed-size chunks.

            Opens the file at `file_path` in binary mode, reads it piece-by-piece
            using `aiofiles`, and yields each chunk as soon as it is fetched.
            The loop terminates automatically when EOF is reached (empty read).

            Usage
            -----
            async for chunk in file_generator():
                await upload(chunk)  # memory-friendly streaming

            Yields
            ------
            bytes
                Up to `chunk_size` bytes of the file; the last chunk may be smaller.
            """
            async with aiofiles.open(file_path, "rb") as f:
                while chunk := await f.read(chunk_size):
                    yield chunk


        super().__init__(
            file_generator(),
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{file_base.filename}"',
                "Content-Length": str(file_base.size),
            }
        )


class PartInDB(SQLModel, table=True):
    """分片记录数据库模型"""
    part_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    file_hash: str = Field(index=True, max_length=256)  # 关联的文件总哈希值
    part_number: int = Field()  # 分片编号（从1开始）
    part_hash: str = Field(max_length=256)  # 分片哈希值
    part_size: int = Field()  # 分片大小
    temp_path: str = Field(max_length=getattr(storage_config, 'max_path_length', 255))  # 临时存储路径(长度限制取ext4文件系统限制路径最大长度255字符)
    owner_id: int = Field(index=True)  # 分片所属用户ID
    uploaded_at: datetime = Field(default_factory=now_local_timezone, index=True)
    file_id: int = Field(index=True)  # 关联的文件记录ID
    total_file_size: int = Field()  # 总文件大小
    total_parts: int = Field()  # 分片总数


class ChunkedUploadStatus(SQLModel): #todo  
    """分片上传状态响应模型"""
    file_hash: str
    filename: str
    total_parts: int
    uploaded_parts: list[int]
    total_size: int
    is_complete: bool


class FileDownloadMeta(FileBase):
    """下载所需的文件元数据"""
    path: Path
    file_id: int | None = None
    uploaded_at: datetime
    download_count: int

    @classmethod
    def from_record(cls, record: FileInDB) -> "FileDownloadMeta":
        """从文件记录创建下载元数据"""
        return cls(
            file_id=record.file_id,
            filename=record.filename,
            file_type=record.file_type,
            size=record.size,
            storage_type=record.storage_type,
            hash_sha256=record.hash_sha256,
            path=Path(record.path),
            uploaded_at=record.uploaded_at,
            download_count=record.download_count,
        )

