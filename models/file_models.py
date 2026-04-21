from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional, List
import aiofiles
import httpx
from sqlmodel import Field, SQLModel, Relationship
from fastapi.responses import StreamingResponse
from sqlalchemy import Column, Integer, ForeignKey,BigInteger
from tools import utc_time,get_logger
from boot.config import get_config
from models.link_models import UserFileLink
from models.user_models import UserInDB
from models.auth import OssStsToken


# 获取服务器状态模块的日志器
logger = get_logger("models.file_models")

# 模型字段配置来自 config
config = get_config()
storage_config = config.storage
sample_config = config.sample_info
user_config = config.user


class FileBase(SQLModel):
    """公共字段,供数据库与返回模型复用"""

    filename: str = Field(max_length=storage_config.max_filename_length)
    file_type: str = Field(default="", max_length=storage_config.max_file_type_length)
    # 【实验类型字段】用于定位对应的实验表(如 imzML/raw 等),由前端上传时指定
    experiment_type: str = Field(default="",
                                 description="实验类型(imzml/raw等),用于定位实验表",
                                 max_length=sample_config.max_experiment_type_length,)
    size: int = Field(default=0,sa_column=Column(BigInteger, nullable=False, default=0))
    storage_type: str = Field(default="local", max_length=64)
    file_verify_code: str | None = Field(default=None, max_length=32)

    # 样本信息字段
    organism: str = Field(default="",description="样本所属物种",max_length=sample_config.max_organism_length,)
    organism_part: str = Field(default="",
                               description="样本来源组织",
                               max_length=sample_config.max_organism_part_length,)
    condition: str = Field(default="",description="样本状态",max_length=sample_config.max_condition_length,)
    sample_growth_conditions: str = Field(default="",
                                          description="样本生长或处理条件(选填)",
                                          max_length=sample_config.max_sample_growth_conditions_length,)
    sample_stabilization: str = Field(default="",description="样本稳定方式",max_length=sample_config.max_sample_stabilization_length,)
    tissue_modification: str = Field(default="",description="组织修饰方式",max_length=sample_config.max_tissue_modification_length,)
    maldi_matrix: str = Field(default="",description="MALDI 基质",max_length=sample_config.max_maldi_matrix_length,)
    maldi_matrix_application: str = Field(default="",
                                          description="基质施加方式",
                                          max_length=sample_config.max_maldi_matrix_application_length,)
    solvent: str = Field(default="",description="使用溶剂(选填)",max_length=sample_config.max_solvent_length,)
    is_public: bool = Field(default=False, description="是否公开文件")


class FileCreate(FileBase):
    """创建文件时的请求模型(普通上传和分片上传)"""
    total_parts: int | None = Field(default=None, description="分片总数(分片上传时必填)")


class PartBase(SQLModel):
    """分片信息基类"""
    file_hash: str = Field(max_length=256, description="关联的文件哈希")
    part_number: int = Field(..., description="分片编号(从1开始)")
    part_hash: str = Field(max_length=256, description="分片哈希值")
    part_size: int = Field(..., description="分片大小")


class PartCreate(PartBase):
    """上传单个分片时的请求模型,所有字段都从 PartBase 继承"""
    pass # pylint: disable=unnecessary-pass


class PartPublic(PartBase):
    """对外返回的分片信息"""
    part_id: int
    uploaded_at: datetime


class FilePublic(FileBase):
    """对外返回的文件信息"""

    file_id: int
    uploaded_at: datetime
    download_count: int
    last_downloaded_at: datetime
    first_uploaded_by: str | None = Field(default=None, description="首次上传该文件的用户名")
    oss_sts_token: OssStsToken | None = Field(default=None)
    oss_bucket: str | None = Field(default=None, description="OSS存储桶(仅用于OSS存储类型)")
    oss_path: str | None = Field(default=None, description="OSS上传路径(仅用于OSS存储类型)")


class FileListResponse(SQLModel):
    """文件列表响应模型"""
    total: int = Field(description="文件总数")
    files: list[FilePublic] = Field(description="文件列表")
    user_id: int = Field(description="用户ID")


class FileInDB(FileBase, table=True):
    """数据库模型"""

    file_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})  # 文件编号(自增主键)
    is_public: bool = Field(default=False, description="是否公开文件")
    path: str = Field(max_length=getattr(storage_config, "max_path_length", 128))  # 存储路径(长度限制取ext4文件系统限制路径最大长度255字符)

    first_uploaded_by: str | None = Field(default=None, max_length=user_config.max_username_length, description="首次上传该文件的用户名")
    # 与 UserInDB 的多对多关系
    owners: list[UserInDB] = Relationship(back_populates="files", link_model=UserFileLink)
    uploaded_at: datetime = Field(default_factory=utc_time, index=True)  # 上传时间
    last_downloaded_at: datetime = Field(default_factory=utc_time, index=True)  # 最近下载时间
    download_count: int = Field(default=0)  # 下载次数
    total_parts: int | None = Field(default=1)  # 分片上传的总分片数(仅用于分片上传)

    # 与 PartInDB 的一对多关系,配置级联删除
    parts: List["PartInDB"] = Relationship(
        back_populates="file",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",  # ORM层：删除父记录时自动删除子记录 这个可以不需要
            "passive_deletes": True  # ORM层：让数据库处理级联删除,避免ORM额外查询
        }
    )


class FileStreamingResponse(StreamingResponse):  # 是响应类模型 不能使用依赖注入
    """文件流式响应,继承自 StreamingResponse,使用 FileBase 的字段"""

    def __init__(
        self,
        file_path: Path,
        file_base: FileBase,
        chunk_size: int,
        media_type: str = "application/octet-stream",
    ):

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
            },
        )


class URLStreamingResponse(StreamingResponse):
    """从上游 URL 流式代理响应到前端，保持低内存占用"""

    def __init__(
        self,
        url: str,
        filename: str = "download",
        chunk_size: int = 64 * 1024,  # 64KB
        media_type: str = "application/octet-stream",
        headers: dict | None = None,
        **http_kwargs,  # 透传 httpx 参数（timeout, headers 等）
    ):
        self.url = url
        self.chunk_size = chunk_size
        self.http_kwargs = http_kwargs

        # 合并自定义 headers
        response_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        if headers:
            response_headers.update(headers)

        super().__init__(
            self._stream_generator(),
            media_type=media_type,
            headers=response_headers,
        )

    def _stream_generator(self) -> AsyncGenerator[bytes, None]:
        """返回异步生成器函数（非协程）"""

        async def generator():
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", self.url, **self.http_kwargs) as response:
                    # 如果上游返回 Content-Length，可以透传
                    if "Content-Length" in response.headers:
                        self.raw_headers.append(
                            (b"content-length", str(response.headers["Content-Length"]).encode())
                        )

                    # 逐块流式读取并立即 yield
                    async for chunk in response.aiter_bytes(self.chunk_size):
                        yield chunk

        return generator()


class PartInDB(PartBase, table=True):
    """分片记录数据库模型"""

    part_id: int | None = Field(
        default=None,
        primary_key=True,
        sa_column_kwargs={"autoincrement": True}
    )
    temp_path: str = Field(max_length=getattr(storage_config, "max_path_length", 128), description="临时存储路径")
    owner_id: int = Field(index=True, description="分片所属用户ID")
    uploaded_at: datetime = Field(default_factory=utc_time, index=True)
    # 外键关联 FileInDB,数据库层面配置级联删除
    file_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("fileindb.file_id", ondelete="CASCADE"),  # 数据库层：级联删除
            nullable=False,
            index=True
        ),
        description="关联的文件ID(外键)"
    )
    total_file_size: int = Field(..., description="总文件大小")
    total_parts: int = Field(..., description="分片总数")

    # 与 FileInDB 的多对一关系
    file: Optional["FileInDB"] = Relationship(back_populates="parts")


class PartUploadStatus(SQLModel):
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
    oss_download_url: str | None = None
    file_id: int | None = None
    uploaded_at: datetime
    download_count: int
    last_downloaded_at: datetime

    @classmethod
    def from_record(cls, record: FileInDB) -> "FileDownloadMeta":
        """从文件记录创建下载元数据"""
        return cls(
            file_id=record.file_id,
            filename=record.filename,
            file_type=record.file_type,
            size=record.size,
            storage_type=record.storage_type,
            file_verify_code=record.file_verify_code,
            path=Path(record.path),
            uploaded_at=record.uploaded_at,
            download_count=record.download_count,
            last_downloaded_at=record.last_downloaded_at,
        )
