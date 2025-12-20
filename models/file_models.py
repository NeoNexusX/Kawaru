'''
Author: xiongqiurong 1513401658@qq.com
Date: 2025-11-25 17:21:17
LastEditors: xiongqiurong 1513401658@qq.com
LastEditTime: 2025-11-28 00:02:11
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

# 获取服务器状态模块的日志器
logger = get_logger("main.file_control")


LOCAL_TIME_OFFSET = timedelta(hours=8)


def now_local_timezone() -> datetime:
    """返回东八区本地时间（无时区信息，便于 SQLite 存储）"""
    return datetime.now(timezone.utc) + LOCAL_TIME_OFFSET


class FileBase(SQLModel):
    """公共字段，供数据库与返回模型复用"""
    filename: str = Field()
    file_type: str = Field(default="")
    size: int = Field(default=0)
    storage_type: str = Field(default="local")
    hash_sha256: str | None = Field(default=None)


class FilePublic(FileBase):
    """对外返回的文件信息"""
    file_id: int
    uploaded_at: datetime
    download_count: int


class FileInDB(FileBase, table=True):
    """数据库模型"""
    file_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})#文件编号（自增主键）
    path: str = Field()#存储路径
    owner_id: int = Field(foreign_key="userindb.id", index=True)#文件所有者id
    uploaded_at: datetime = Field(default_factory=now_local_timezone, index=True)#上传时间
    download_count: int = Field(default=0)#下载次数


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


class FileDownloadMeta(FileBase):
    """下载所需的文件元数据"""
    path: Path
    file_id: int | None = None
    uploaded_at: datetime
    download_count: int

    @classmethod
    def from_record(cls, record: FileInDB) -> "FileDownloadMeta":#利用该函数将文件记录创建文件元数据
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

