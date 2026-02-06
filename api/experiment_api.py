##########################
# Experiment API
#########################
from __future__ import annotations
import asyncio
import shutil
from pathlib import Path
from datetime import timedelta
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session, select
from database.db import SessionDep, get_sync_engine

from api.user_api import token_authen
from api.file_api_helper import delete_files_async
from api.file_api_business import (
    get_experiment_by_file_id,
    assert_file_owner,
)
from api.experiment_api_helper import (
    ensure_experiment_dirs,
    extract_and_validate_imzml_zip,
)

from logger import get_logger
from models.user_models import UserInDB
from models.file_models import FileInDB, now_local_timezone
from models.link_models import UserFileLink
from models.experiment_models import (
    ExperimentImzMLInDB,
    ExperimentImzMLPublic,
    ExperimentStatus,
)
from constants.experiment_constants import (
    EXPERIMENT_TEMP_ROOT,
    EXPERIMENT_PROCESSED_ROOT,
    FAILED_EXPERIMENT_TTL_MINUTES,
)
from exceptions.experiment_exceptions import (
    experiment_not_found_exception,
    experiment_permission_denied_exception,
    source_file_not_found_exception,
    unsupported_experiment_type_exception,
)

logger = get_logger("main.experiment_api")

#########################
# 异步清理函数
#########################


async def cleanup_failed_experiments(
    session: SessionDep, ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES
) -> int:
    """清理过期的失败实验记录，同时删除对应的文件表记录和更新用户表"""
    now = now_local_timezone()
    threshold = now - timedelta(minutes=ttl_minutes)
    cleaned = 0

    try:
        failed_experiments = (
            await session.exec(
                select(ExperimentImzMLInDB).where(
                    ExperimentImzMLInDB.status == ExperimentStatus.FAILED,
                    ExperimentImzMLInDB.created_at < threshold,
                )
            )
        ).all()

        for exp in failed_experiments:
            try:
                # 删除物理文件
                files_to_delete = []
                if exp.imzml_path:
                    files_to_delete.append(exp.imzml_path)
                if exp.ibd_path:
                    files_to_delete.append(exp.ibd_path)

                if files_to_delete:
                    await delete_files_async(files_to_delete)

                # 删除实验目录（如果为空）
                if exp.imzml_path:
                    experiment_dir = Path(exp.imzml_path).parent

                    def _rmdir_if_empty(d: Path):
                        if d.exists() and not any(d.iterdir()):
                            d.rmdir()

                    await asyncio.to_thread(_rmdir_if_empty, experiment_dir)

                # 查找文件表中对应的记录
                file_record = (await session.exec(
                    select(FileInDB).where(FileInDB.file_id == exp.source_file_id)
                )).first()

                if file_record:
                    file_size = file_record.size or 0

                    # 通过 UserFileLink 查询所有拥有该文件的用户ID（避免加载完整的用户对象）
                    user_ids = (await session.exec(
                        select(UserFileLink.user_id).where(
                            UserFileLink.file_id == exp.source_file_id
                        )
                    )).all()

                    # 更新所有拥有该文件的用户：减少 file_count 和 total_file_size
                    for user_id in user_ids:
                        user_record = (await session.exec(
                            select(UserInDB).where(UserInDB.id == user_id)
                        )).first()

                        if user_record:
                            user_record.file_count = max(
                                0, (user_record.file_count or 0) - 1
                            )
                            user_record.total_file_size = max(
                                0, (user_record.total_file_size or 0) - file_size
                            )
                            session.add(user_record)
                            logger.debug(
                                "cleanup user record: user_id=%s file_id=%s",
                                user_id,
                                exp.source_file_id,
                            )

                    # 删除所有 UserFileLink 关联
                    user_file_links = (await session.exec(
                        select(UserFileLink).where(
                            UserFileLink.file_id == exp.source_file_id
                        )
                    )).all()

                    for link in user_file_links:
                        await session.delete(link)

                    # 删除文件记录
                    await session.delete(file_record)
                    logger.debug("cleanup file record: file_id=%s", exp.source_file_id)

                # 删除实验记录
                await session.delete(exp)
                cleaned += 1
                logger.debug("cleanup failed experiment: exp_id=%s", exp.id)
            except Exception as e:
                logger.error(
                    "failed to cleanup experiment exp_id=%s error=%s", exp.id, e
                )

        await session.commit()
        logger.info("cleanup failed experiments completed count=%s", cleaned)

    except Exception as e:
        logger.error("cleanup failed experiments error=%s", e)

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

            # 创建临时目录：experiments/temp/exp_{id}/
            ensure_experiment_dirs()
            temp_dir = EXPERIMENT_TEMP_ROOT / f"exp_{experiment_record.id}"
            temp_dir.mkdir(parents=True, exist_ok=True)

            # 解压和验证
            source_path = Path(source_file.path)
            imzml_temp_path, ibd_temp_path = extract_and_validate_imzml_zip(
                source_path, temp_dir
            )

            # 准备正式存储路径：experiments/processed/exp_{id}/
            experiment_dir = EXPERIMENT_PROCESSED_ROOT / f"exp_{experiment_record.id}"
            experiment_dir.mkdir(parents=True, exist_ok=True)

            # 移动文件到正式位置
            final_imzml_path = experiment_dir / f"{experiment_name}.imzML"
            final_ibd_path = experiment_dir / f"{experiment_name}.ibd"

            shutil.move(str(imzml_temp_path), str(final_imzml_path))
            shutil.move(str(ibd_temp_path), str(final_ibd_path))

            # 更新实验记录
            experiment_record.imzml_path = str(final_imzml_path)
            experiment_record.imzml_size = final_imzml_path.stat().st_size
            experiment_record.ibd_path = str(final_ibd_path)
            experiment_record.ibd_size = final_ibd_path.stat().st_size
            experiment_record.status = ExperimentStatus.COMPLETED
            session.add(experiment_record)

            # 删除原始ZIP文件并清空文件表中的path字段
            try:
                source_path.unlink()
                source_file.path = ""
                session.add(source_file)
                logger.info(
                    "deleted source zip file and cleared path field file_id=%s",
                    source_file_id,
                )
            except (OSError, PermissionError) as e:
                logger.warning("failed to delete source zip file error=%s", e)

            session.commit()
            logger.info(
                "imzml experiment processing completed exp_id=%s", experiment_record.id
            )

    except Exception as e:
        logger.error("imzml experiment processing failed error=%s", e)

        # 更新实验状态为失败
        if experiment_record and experiment_record.id:
            try:
                with Session(get_sync_engine()) as session:
                    statement = select(ExperimentImzMLInDB).where(
                        ExperimentImzMLInDB.id == experiment_record.id
                    )
                    exp_record = session.exec(statement).first()

                    if exp_record:
                        exp_record.status = ExperimentStatus.FAILED
                        exp_record.error_message = str(e)
                        session.add(exp_record)

                        # 删除原始ZIP文件并清空path字段
                        file_statement = select(FileInDB).where(
                            FileInDB.file_id == source_file_id
                        )
                        source_file = session.exec(file_statement).first()

                        if source_file and source_file.path:
                            try:
                                Path(source_file.path).unlink(missing_ok=True)
                                source_file.path = ""
                                session.add(source_file)
                                logger.info(
                                    "deleted source zip file after failure file_id=%s",
                                    source_file_id,
                                )
                            except (OSError, PermissionError) as cleanup_error:
                                logger.warning(
                                    "failed to delete source zip file after failure error=%s",
                                    cleanup_error,
                                )

                        session.commit()

            except Exception as update_error:
                logger.error(
                    "failed to update experiment status error=%s", update_error
                )

    finally:
        # 清理临时目录
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except (OSError, PermissionError, shutil.Error) as e:
                logger.error("failed to cleanup temp directory error=%s", e)


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
    session: SessionDep,
) -> ExperimentImzMLPublic:
    """根据源文件ID查询实验记录，根据文件的 experiment_type 字段定位对应实验表"""
    # 清理过期的失败实验记录
    try:
        await cleanup_failed_experiments(session)
    except Exception:
        logger.exception(
            "Failed to cleanup failed experiments for file_id=%s user_id=%s",
            file_id,
            getattr(current_user, "id", None),
        )

    # 1. 查询文件记录
    file_record = (
        await session.exec(select(FileInDB).where(FileInDB.file_id == file_id))
    ).first()
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
    if not query_func:
        raise unsupported_experiment_type_exception

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
