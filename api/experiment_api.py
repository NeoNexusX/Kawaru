##########################
# Experiment API
#########################
from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlmodel import select
from api.experiment_api_business import  get_imzml_experiment_by_file_id
from api.user_api import token_authen
from api.file_api_business import (
    assert_file_owner
)
from database.db import AsyncSessionDep
from models.user_models import UserInDB
from models.file_models import FileInDB
from models.experiment_models import ExperimentImzMLPublic

from exceptions.experiment_exceptions import (
    experiment_not_found_exception,
    experiment_permission_denied_exception,
    source_file_not_found_exception,
)
from tools import get_logger
logger = get_logger("api.experiment_api")

#########################
# 异步查询函数
#########################


# 实验类型到查询函数的映射
# 【扩展接口】后续添加新实验类型时，在此添加映射即可
EXPERIMENT_QUERY_MAP = {
    "imzML": get_imzml_experiment_by_file_id,
    # "raw": get_raw_experiment,  # 预留:RAW 实验查询函数
}


async def get_experiment_by_file_id_api(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> ExperimentImzMLPublic:
    """根据源文件ID查询实验记录,根据文件的 experiment_type 字段定位对应实验表"""

    # 1. 查询文件记录
    file_record = (
        await async_session.exec(select(FileInDB).where(FileInDB.file_id == file_id))).first()
    if not file_record:
        raise source_file_not_found_exception

    # 2. 检查权限
    if not await assert_file_owner(async_session, file_record, current_user):
        raise experiment_permission_denied_exception

    # 3. 验证实验类型并获取查询函数
    query_func = (
        EXPERIMENT_QUERY_MAP.get(file_record.experiment_type)
        if file_record.experiment_type else None
    )
    if not query_func:
        logger.error("unsupported experiment type file_id=%s exp_type=%s", file_id, file_record.experiment_type)
        raise experiment_not_found_exception

    # 4. 调用查询函数
    experiment = await query_func(async_session, file_id)
    if not experiment:
        raise experiment_not_found_exception

    logger.debug(f"get experiment by file_id user={current_user.id} file_id={file_id} exp_type={file_record.experiment_type}")

    return experiment #type:ignore

GetExperimentByFileIdDep = Annotated[ExperimentImzMLPublic, Depends(get_experiment_by_file_id_api)]#用于根据文件id查询实验表记录
