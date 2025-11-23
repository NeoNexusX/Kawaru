from datetime import datetime, timedelta, timezone

from sqlmodel import Field, SQLModel
from logger import get_logger

# 获取服务器状态模块的日志器
logger = get_logger("main.file_control")


LOCAL_TIME_OFFSET = timedelta(hours=8)


def now_local_timezone() -> datetime:
    """返回东八区本地时间（无时区信息，便于 SQLite 存储）"""
    return datetime.now(timezone.utc) + LOCAL_TIME_OFFSET

# TODO：三个应该有继承关系的模型，不应该是独立的

class FileInDB(SQLModel, table=True):
    file_id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})#文件编号（自增主键）
    filename: str = Field()#文件名
    path: str = Field()#存储路径
    owner_id: int = Field(foreign_key="userindb.id", index=True)#文件所有者id
    file_type: str = Field(default="")#文件类型
    size: int = Field(default=0)#文件大小
    storage_type: str = Field(default="local")#存储类型
    hash_sha256: str | None = Field(default=None)#文件hash
    uploaded_at: datetime = Field(default_factory=now_local_timezone, index=True)#上传时间
    download_count: int = Field(default=0)#下载次数


class FilePublic(SQLModel):
    """对外返回的文件信息"""
    file_id: int
    filename: str
    file_type: str
    size: int
    storage_type: str
    hash_sha256: str | None = None
    uploaded_at: datetime
    download_count: int


class FileDownload(SQLModel):
    """下载依赖返回的数据载体"""
    path: str = Field(description="文件在服务器的路径")
    filename: str = Field(description="下载时返回的文件名称")
