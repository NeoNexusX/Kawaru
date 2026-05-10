##########################
# Origin API
#########################
from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlmodel import select
from api.experiment_api_business import  get_origin_imzml_by_file_id
from api.user_api import token_authen
from api.file_api_business import (
    assert_file_owner
)
from database.db import AsyncSessionDep
from models.user_models import UserInDB
from models.file_models import FileInDB
from models.experiment_models import OriginImzMLPublic

from exceptions.experiment_exceptions import (
    origin_not_found_exception,
    origin_permission_denied_exception,
    source_file_not_found_exception,
)
from tools import get_logger
logger = get_logger("api.experiment_api")

#########################
# 异步查询函数
#########################


# origin 类型到查询函数的映射
# 【扩展接口】后续添加新 origin 类型时，在此添加映射即可
ORIGIN_QUERY_MAP = {
    "imzML": get_origin_imzml_by_file_id,
    # "raw": get_origin_raw,  # 预留:RAW origin 查询函数
}


async def get_origin_by_file_id_api(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> OriginImzMLPublic:
    """根据源文件ID查询 origin 记录,根据文件的 experiment_type 字段定位对应 origin 表"""

    # 1. 查询文件记录
    file_record = (
        await async_session.exec(select(FileInDB).where(FileInDB.file_id == file_id))).first()
    if not file_record:
        raise source_file_not_found_exception

    # 2. 检查权限
    if not await assert_file_owner(async_session, file_record, current_user):
        raise origin_permission_denied_exception

    # 3. 验证 origin 类型并获取查询函数
    query_func = (
        ORIGIN_QUERY_MAP.get(file_record.experiment_type)
        if file_record.experiment_type else None
    )
    if not query_func:
        logger.error("unsupported origin type file_id=%s exp_type=%s", file_id, file_record.experiment_type)
        raise origin_not_found_exception

    # 4. 调用查询函数
    origin = await query_func(async_session, file_id)
    if not origin:
        raise origin_not_found_exception

    logger.debug(f"get origin by file_id user={current_user.id} file_id={file_id} exp_type={file_record.experiment_type}")

    return origin #type:ignore

GetOriginByFileIdDep = Annotated[OriginImzMLPublic, Depends(get_origin_by_file_id_api)]#用于根据文件id查询 origin 表记录
