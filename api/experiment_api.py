##########################
# Origin API
#########################
from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlmodel import select
from api_services.experiment_service import get_origin_imzml_by_file_id
from api.user_api import token_authen
from api_helpers.deletion_helper import file_is_deleting
from api_services.file_service import assert_file_owner
from database.db import AsyncSessionDep
from models.user_models import UserInDB
from models.file_models import FileInDB
from models.experiment_models import OriginImzMLPublic

from exceptions.experiment_exceptions import (
    origin_not_found_exception,
    origin_permission_denied_exception,
    source_file_not_found_exception,
)
from tools import PublicIdPath, get_logger
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


async def get_origin_by_public_id_api(
    public_id: PublicIdPath,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> OriginImzMLPublic:
    """根据源文件 public_id 查询 origin 记录,根据文件的 experiment_type 字段定位对应 origin 表

    文件查询保持内联：不复用 get_file_record_by_public_id，以保留本接口原有的
    source_file_not_found_exception 口径。删除流程中的文件按不存在处理（与文件
    详情 404 一致，见 docs/zh/删除架构现状说明.md §2）。
    """

    # 1. 查询文件记录（删除流程中的按不存在处理）
    file_record = (
        await async_session.exec(select(FileInDB).where(FileInDB.public_id == public_id))).first()
    if not file_record or file_is_deleting(file_record.status):
        logger.warning("origin query: file not found or being deleted public_id=%s", public_id)
        raise source_file_not_found_exception
    file_id = file_record.file_id

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

    logger.debug(f"get origin by public_id user={current_user.id} public_id={public_id} file_id={file_id} exp_type={file_record.experiment_type}")

    return origin #type:ignore

GetOriginByPublicIdDep = Annotated[OriginImzMLPublic, Depends(get_origin_by_public_id_api)]#用于根据文件 public_id 查询 origin 表记录
