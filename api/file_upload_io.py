#########################
# File Upload IO - API 层文件上传 IO
# 职责：处理 FastAPI UploadFile 的落盘与分片持久化。
# 这些函数绑定 FastAPI 的 UploadFile，仅供 API 上传端点使用，
# 不属于业务层（services），background 不会调用。
#########################
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import aiofiles
from fastapi import UploadFile
from sqlmodel.ext.asyncio.session import AsyncSession

from constants.file_constants import CHUNK_BUFFER_SIZE
from models.file_models import FileInDB, PartInDB
from models.user_models import UserInDB
from tools.logger import get_logger

if TYPE_CHECKING:
    from models.file_models import PartCreate

logger = get_logger("api.file_upload_io")


async def save_upload_file(upload_file: UploadFile, destination: Path) -> None:
    """
    将上传文件保存到指定路径
    使用 aiofiles 异步写入,分片读取避免内存占用过大 写入完成后关闭上传文件句柄
    """
    await upload_file.seek(0)
    async with aiofiles.open(destination, "wb") as buffer:
        while True:
            chunk = await upload_file.read(CHUNK_BUFFER_SIZE)
            if not chunk:
                break
            await buffer.write(chunk)
    await upload_file.close()


async def save_and_persist_part(
    async_session: AsyncSession,
    file_record: FileInDB,
    current_user: UserInDB,
    part_data: PartCreate,
    part: UploadFile,
    temp_path: Path,
) -> PartInDB:
    """
    保存分片临时文件并创建数据库记录

    Returns:
        持久化后的 PartInDB 记录

    Raises:
        file_persist_failed_exception: 文件保存失败
    """
    await save_upload_file(part, temp_path)

    part_record = PartInDB(
        **part_data.model_dump(),
        temp_path=str(temp_path),
        owner_id=current_user.id, #type:ignore 使用的时候需要提前判断
        file_id=file_record.file_id, #type:ignore 使用的时候需要提前判断
        total_file_size=file_record.size,
        total_parts=file_record.total_parts, #type:ignore 使用的时候需要提前判断
    )
    async_session.add(part_record)
    await async_session.flush()

    return part_record
