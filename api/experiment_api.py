##########################
# Experiment API
#########################
from __future__ import annotations

from pathlib import Path
from datetime import timedelta
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session, select
from database.db import AsyncSessionDep, get_sync_engine

from api.user_api import token_authen
from api.file_api_business import (
    get_experiment_by_file_id,
    assert_file_owner,
    delete_experiment_files,
)
from api.experiment_api_helper import (
    extract_and_validate_imzml_zip,
    create_experiment_directories,
    move_experiment_files,
    delete_source_file_and_clear_path,
    cleanup_temp_directory,
)

from tools.logger import get_logger
from models.user_models import UserInDB
from models.file_models import FileInDB, now_local_timezone
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

logger = get_logger("api.experiment_api")

#########################
# 异步清理函数
#########################


async def cleanup_failed_experiments(
    session: AsyncSessionDep, ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES
) -> int:
    """
    清理过期的失败实验记录
    
    完整的清理流程：
    1. 查询过期的失败实验记录
    2. 手动更新所有相关用户的 file_count 和 total_file_size
    3. 删除实验文件（imzML、ibd）物理文件
    4. 删除文件表记录
    5. 数据库自动级联删除关联的 UserFileLink 和 ExperimentImzMLInDB 记录
    
    注意：
    - 源 ZIP 文件不删除
    - delete_experiment_files 仅处理物理文件删除，不涉及数据库操作
    - 删除 file 记录时，数据库级联删除会处理 UserFileLink 和 ExperimentImzMLInDB
    """
    now = now_local_timezone()
    threshold = now - timedelta(minutes=ttl_minutes)
    cleaned = 0

    # 第一步：查询过期的失败实验
    try:
        failed_experiments = (
            await session.exec(
                select(ExperimentImzMLInDB).where(
                    ExperimentImzMLInDB.status == ExperimentStatus.FAILED,
                    ExperimentImzMLInDB.created_at < threshold,
                )
            )
        ).all()
    except Exception as e:
        logger.error("failed to query failed experiments error=%s", e)
        return cleaned

    # 第二步：处理每个失败的实验
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

            # 2. 删除实验文件（imzML、ibd）物理文件
            # delete_experiment_files 只处理物理文件删除，不操作数据库
            await delete_experiment_files(session, exp.source_file_id)

            # 3. 删除文件表记录
            # 数据库级联删除会自动处理：
            # - 删除关联的 UserFileLink 记录
            # - 删除关联的 ExperimentImzMLInDB 记录
            await session.delete(file_record)

            cleaned += 1
            logger.debug("cleanup failed experiment: exp_id=%s", exp.id)

        except Exception as e:
            logger.error(
                "failed to cleanup experiment exp_id=%s error=%s", exp.id, e
            )

    # 第三步：提交事务
    try:
        await session.commit()
        logger.info("cleanup failed experiments completed count=%s", cleaned)
    except Exception as e:
        logger.error("failed to commit cleanup changes error=%s", e)

    return cleaned


#########################
# 后台处理函数（供 BackgroundTasks 调用）
#########################


def process_imzml_experiment_background(
    user_id: int, source_file_id: int, experiment_name: str
):
    """
    后台处理 ImzML 实验的同步任务

    存储路径：
    - 临时目录：experiments/temp/exp_{id}/
    - 正式目录：experiments/processed/exp_{id}/
    """
    temp_dir = None
    experiment_record = None

    try:
        logger.info(
            "starting imzml experiment processing user=%s file=%s",
            user_id,
            source_file_id,
        )

        with Session(get_sync_engine()) as session:
            # 查询源文件
            statement = select(FileInDB).where(FileInDB.file_id == source_file_id)
            source_file = session.exec(statement).first()

            if not source_file:
                logger.error("source file not found file_id=%s", source_file_id)
                return

            # 创建实验记录（不包含 user_id，通过 source_file_id 关联）
            experiment_record = ExperimentImzMLInDB(
                experiment_name=experiment_name,
                source_file_id=source_file_id,
                status=ExperimentStatus.PROCESSING,
            )
            session.add(experiment_record)
            session.commit()
            session.refresh(experiment_record)

            # 创建临时与正式目录（helper 层）
            temp_dir, experiment_dir = create_experiment_directories(experiment_record.id)

            # 解压和验证
            source_path = Path(source_file.path)
            result = extract_and_validate_imzml_zip(source_path, temp_dir)

            if result is None:
                raise ValueError("Failed to extract and validate imzML zip file")

            imzml_temp_path, ibd_temp_path = result

            # 将临时文件移动到正式位置（helper 层）
            final_imzml_path, final_ibd_path = move_experiment_files(
                imzml_temp_path, ibd_temp_path, experiment_dir, experiment_name
            )

            # 更新实验记录
            experiment_record.imzml_path = str(final_imzml_path)
            experiment_record.imzml_size = final_imzml_path.stat().st_size
            experiment_record.ibd_path = str(final_ibd_path)
            experiment_record.ibd_size = final_ibd_path.stat().st_size
            experiment_record.status = ExperimentStatus.COMPLETED
            session.add(experiment_record)

            # 删除原始 ZIP 文件并修改文件表中的 path 字段（helper 层）
            delete_source_file_and_clear_path(source_file)
            session.add(source_file)
            logger.info(
                "deleted source zip file and cleared path field file_id=%s",
                source_file_id,
            )

            session.commit()
            logger.info(
                "imzml experiment processing completed exp_id=%s", experiment_record.id
            )

    except Exception as e:
        logger.error("imzml experiment processing failed error=%s", e)

        # 更新实验状态为失败（business 层）
        if experiment_record and experiment_record.id:
            try:
                with Session(get_sync_engine()) as session:
                    statement = select(ExperimentImzMLInDB).where(
                        ExperimentImzMLInDB.id == experiment_record.id)
                    exp_record = session.exec(statement).first()

                    if exp_record:
                        exp_record.status = ExperimentStatus.FAILED
                        exp_record.error_message = str(e)
                        session.add(exp_record)

                        # 删除原始 ZIP 文件并清空 path 字段（helper 层）
                        file_statement = select(FileInDB).where(
                            FileInDB.file_id == source_file_id
                        )
                        source_file = session.exec(file_statement).first()

                        if source_file and source_file.path:
                            delete_source_file_and_clear_path(source_file)
                            session.add(source_file)
                            logger.info(
                                "deleted source zip file after failure file_id=%s",
                                source_file_id,
                            )

                        session.commit()

            except Exception as update_error:
                logger.error(
                    "failed to update experiment status error=%s", update_error
                )

    finally:
        # 清理临时目录（helper 层）
        cleanup_temp_directory(temp_dir)


#########################
# 异步查询函数
#########################


# 实验类型到查询函数的映射
# 【扩展接口】后续添加新实验类型时，在此添加映射即可
EXPERIMENT_QUERY_MAP = {
    "imzML": get_experiment_by_file_id,
    # "raw": get_raw_experiment,  # 预留：RAW 实验查询函数
}


async def get_experiment_by_file_id_api(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: AsyncSessionDep,
) -> ExperimentImzMLPublic:
    """根据源文件ID查询实验记录，根据文件的 experiment_type 字段定位对应实验表"""
    # 清理过期的失败实验记录
    try:
        await cleanup_failed_experiments(session)
        await session.refresh(current_user)
    except Exception:
        logger.exception(
            "Failed to cleanup failed experiments for file_id=%s user_id=%s",
            file_id,
            getattr(current_user, "id", None),
        )

    # 1. 查询文件记录
    file_record = (
        await session.exec(select(FileInDB).where(FileInDB.file_id == file_id))).first()
    if not file_record:
        raise source_file_not_found_exception

    # 2. 检查权限
    if not await assert_file_owner(session, file_record, current_user):
        raise experiment_permission_denied_exception

    # 3. 验证实验类型并获取查询函数
    query_func = (
        EXPERIMENT_QUERY_MAP.get(file_record.experiment_type)
        if file_record.experiment_type
        else None
    )

    # 4. 调用查询函数
    experiment = await query_func(session, file_id)
    if not experiment:
        raise experiment_not_found_exception

    logger.debug(
        "get experiment by file_id user=%s file_id=%s exp_type=%s",
        current_user.id,
        file_id,
        file_record.experiment_type,
    )
    return experiment

GetExperimentByFileIdDep = Annotated[ExperimentImzMLPublic, Depends(get_experiment_by_file_id_api)]#用于根据文件id查询实验表记录
