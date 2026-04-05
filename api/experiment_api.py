##########################
# Experiment API
#########################
from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import Depends
from sqlmodel import select
from api.experiment_api_business import delete_experiment_files, get_imzml_experiment_by_file_id
from api.user_api import token_authen
from api.file_api_business import (
    assert_file_owner
)
from database.db import AsyncSessionDep
from models.user_models import UserInDB
from models.file_models import FileInDB
from models.link_models import UserFileLink
from models.experiment_models import (
    ExperimentImzMLInDB,
    ExperimentImzMLPublic,
    ExperimentStatus,
)
from constants.experiment_constants import (
    FAILED_EXPERIMENT_TTL_MINUTES,
)
from exceptions.experiment_exceptions import (
    experiment_not_found_exception,
    experiment_permission_denied_exception,
    source_file_not_found_exception,
)
from tools import get_logger,utc_time
logger = get_logger("api.experiment_api")

#########################
# 异步清理函数
#########################


async def cleanup_failed_experiments(
    session: AsyncSessionDep, ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES
) -> int:
    """
    清理过期的失败实验记录
    
    完整的清理流程:
    1. 查询过期的失败实验记录
    2. 手动更新所有相关用户的 file_count 和 total_file_size
    3. 删除实验文件(imzML、ibd)物理文件
    4. 删除文件表记录
    5. 数据库自动级联删除关联的 UserFileLink 和 ExperimentImzMLInDB 记录
    
    注意:
    - 源 ZIP 文件不删除
    - delete_experiment_files 仅处理物理文件删除，不涉及数据库操作
    - 删除 file 记录时，数据库级联删除会处理 UserFileLink 和 ExperimentImzMLInDB
    """
    now = utc_time()
    threshold = now - timedelta(minutes=ttl_minutes)
    cleaned = 0

    # 第一步:查询过期的失败实验
    try:
        failed_experiments = (
            await session.exec(
                select(ExperimentImzMLInDB).where(
                    ExperimentImzMLInDB.status == ExperimentStatus.FAILED,
                    ExperimentImzMLInDB.created_at < threshold,
                )
            )
        ).all()
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error("failed to query failed experiments error=%s", e)
        return cleaned

    # 第二步:处理每个失败的实验
    for exp in failed_experiments:
        try:
            # 查询关联的文件记录
            file_record = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == exp.source_file_id)
            )).first()

            if not file_record:
                logger.warning(
                    "file record not found for exp_id=%s source_file_id=%s",
                    exp.id,
                    exp.source_file_id,
                )
                # 即使文件记录不存在，也要删除实验记录
                await session.delete(exp)
                cleaned += 1
                continue

            # 1. 查询所有关联的用户，手动更新 file_count 和 total_file_size
            user_ids = (await session.exec(
                select(UserFileLink.user_id).where(
                    UserFileLink.file_id == exp.source_file_id
                )
            )).all()

            for user_id in user_ids:
                user_record = (await session.exec(
                    select(UserInDB).where(UserInDB.id == user_id)
                )).first()

                if user_record:
                    user_record.file_count = max(0, (user_record.file_count or 0) - 1)
                    user_record.total_file_size = max(
                        0, (user_record.total_file_size or 0) - (file_record.size or 0)
                    )
                    session.add(user_record)
                    logger.debug(
                        "updated user stats: user_id=%s file_count=%s total_size=%s",
                        user_id,
                        user_record.file_count,
                        user_record.total_file_size,
                    )

            # 2. 删除实验文件(imzML、ibd)物理文件
            # delete_experiment_files 只处理物理文件删除，不操作数据库
            await delete_experiment_files(session, exp.source_file_id)

            # 3. 删除文件表记录
            # 数据库级联删除会自动处理:
            # - 删除关联的 UserFileLink 记录
            # - 删除关联的 ExperimentImzMLInDB 记录
            await session.delete(file_record)

            cleaned += 1
            logger.debug("cleanup failed experiment: exp_id=%s", exp.id)

        except Exception as e: # pylint: disable=broad-exception-caught
            logger.error(
                "failed to cleanup experiment exp_id=%s error=%s", exp.id, e
            )

    # 第三步:提交事务
    try:
        await session.commit()
        logger.info("cleanup failed experiments completed count=%s", cleaned)
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error("failed to commit cleanup changes error=%s", e)

    return cleaned

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
