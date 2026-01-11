#########################
# Experiment API
#########################
from __future__ import annotations

import asyncio
import zipfile
import shutil
from pathlib import Path
from datetime import timedelta
from typing import Annotated
from starlette import status
from fastapi import Depends, HTTPException
from sqlmodel import Session, select
from api.user_api import token_authen
from database.db import SessionDep, get_sync_engine
from envset.config import get_config
from logger import get_logger
from models.user_models import UserInDB
from models.file_models import FileInDB, now_local_timezone
from models.experiment_models import (
    ExperimentImzMLInDB,
    ExperimentImzMLPublic,
    ExperimentStatus,
)

logger = get_logger("main.experiment_api")

# 获取配置
config = get_config()
storage_config = config.storage
STORAGE_ROOT = Path(storage_config.root)
EXPERIMENT_STORAGE_ROOT = STORAGE_ROOT / "experiments"
EXPERIMENT_TEMP_ROOT = (
    EXPERIMENT_STORAGE_ROOT / "temp"
)  # 【临时目录】experiments/temp/exp_{id}
EXPERIMENT_PROCESSED_ROOT = (
    EXPERIMENT_STORAGE_ROOT / "processed"
)  # 【正式目录】experiments/processed/exp_{id}
FAILED_EXPERIMENT_TTL_MINUTES = int(
    getattr(storage_config, "failed_experiment_ttl_minutes", 30)
)

# 实验不存在异常
experiment_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="experiment not found",
)

# 无权限访问实验异常
experiment_permission_denied_exception = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="permission denied to access this experiment",
)

# 源文件不存在异常
source_file_not_found_exception = HTTPException(
    status_code=status.HTTP_404_NOT_FOUND,
    detail="source file not found",
)

# 不支持的实验类型异常
unsupported_experiment_type_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="unsupported experiment type",
)


#########################
# 同步辅助函数
#########################


def _ensure_experiment_dirs():
    """确保实验存储目录存在"""
    EXPERIMENT_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    EXPERIMENT_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    EXPERIMENT_PROCESSED_ROOT.mkdir(parents=True, exist_ok=True)


def _validate_imzml_files(imzml_path: Path, ibd_path: Path) -> bool:
    """
    验证 imzML 和 ibd 文件是否为有效可读的 MSI 实验数据
    TODO: 后续扩展验证逻辑（如使用 pyimzml 进行深度验证）
    """
    return True


def _extract_and_validate_zip(zip_file_path: Path, temp_dir: Path) -> tuple[Path, Path]:
    """
    解压 ZIP 文件并验证内容
    ZIP 文件只包含两个文件：.imzML 和 .ibd
    包含 Zip Slip 路径遍历攻击防护
    """
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            # 验证 ZIP 条目以防止路径遍历（Zip Slip）
            temp_dir_resolved = temp_dir.resolve()
            for member in zip_ref.namelist():
                member_path = (temp_dir / member).resolve()
                try:
                    member_path.relative_to(temp_dir_resolved)
                except ValueError:
                    raise Exception("unsafe file path in zip file")
            zip_ref.extractall(temp_dir)
    except zipfile.BadZipFile:
        raise Exception("invalid zip file format")
    except Exception as e:
        logger.error("zip extraction failed: %s", e)
        raise Exception(f"zip extraction failed: {e}")

    # 查找 imzML 和 ibd 文件
    imzml_path = None
    ibd_path = None

    for file_path in temp_dir.iterdir():
        if file_path.suffix == ".imzML":
            imzml_path = file_path
        elif file_path.suffix == ".ibd":
            ibd_path = file_path

    # 验证文件是否存在
    if not imzml_path or not ibd_path:
        raise Exception("zip file must contain both .imzML and .ibd files")

    # 验证文件（目前直接通过）
    if not _validate_imzml_files(imzml_path, ibd_path):
        raise Exception("imzML file validation failed")

    return imzml_path, ibd_path


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
                if exp.imzml_path:
                    await asyncio.to_thread(
                        lambda p: Path(p).unlink(missing_ok=True), exp.imzml_path
                    )
                if exp.ibd_path:
                    await asyncio.to_thread(
                        lambda p: Path(p).unlink(missing_ok=True), exp.ibd_path
                    )

                # 删除实验目录（如果为空）
                if exp.imzml_path:
                    experiment_dir = Path(exp.imzml_path).parent

                    def _rmdir_if_empty(d: Path):
                        if d.exists() and not any(d.iterdir()):
                            d.rmdir()

                    await asyncio.to_thread(_rmdir_if_empty, experiment_dir)

                # 查找文件表中对应的记录
                file_statement = select(FileInDB).where(
                    FileInDB.file_id == exp.source_file_id
                )
                file_record = session.exec(file_statement).first()

                if file_record:
                    file_size = file_record.size or 0
                    owners = list(file_record.owners or [])

                    # 更新所有拥有该文件的用户：移除 file_id，减少 file_count 和 total_file_size
                    for owner_id in owners:
                        user_statement = select(UserInDB).where(UserInDB.id == owner_id)
                        user_record = session.exec(user_statement).first()

                        if user_record:
                            file_ids = list(user_record.file_ids or [])
                            if exp.source_file_id in file_ids:
                                file_ids.remove(exp.source_file_id)
                                user_record.file_ids = file_ids
                                user_record.file_count = max(
                                    0, (user_record.file_count or 0) - 1
                                )
                                user_record.total_file_size = max(
                                    0, (user_record.total_file_size or 0) - file_size
                                )
                                session.add(user_record)
                                logger.debug(
                                    "cleanup user record: user_id=%s file_id=%s",
                                    owner_id,
                                    exp.source_file_id,
                                )

                    # 删除文件记录
                    session.delete(file_record)
                    logger.debug("cleanup file record: file_id=%s", exp.source_file_id)

                # 删除实验记录
                session.delete(exp)
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
            _ensure_experiment_dirs()
            temp_dir = EXPERIMENT_TEMP_ROOT / f"exp_{experiment_record.id}"
            temp_dir.mkdir(parents=True, exist_ok=True)

            # 解压和验证
            source_path = Path(source_file.path)
            imzml_temp_path, ibd_temp_path = _extract_and_validate_zip(
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
            except Exception as e:
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
                            except Exception as cleanup_error:
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
            except Exception as e:
                logger.error("failed to cleanup temp directory error=%s", e)


#########################
# 异步查询函数
#########################


async def get_imzml_experiment(
    file_id: int, session: SessionDep
) -> ExperimentImzMLPublic | None:
    """查询 imzML 类型实验（异步）"""
    statement = select(ExperimentImzMLInDB).where(
        ExperimentImzMLInDB.source_file_id == file_id,
    )
    experiment = await session.exec(statement).first()
    return experiment


# 实验类型到查询函数的映射
# 【扩展接口】后续添加新实验类型时，在此添加映射即可
EXPERIMENT_QUERY_MAP = {
    "imzML": get_imzml_experiment,
    # "raw": get_raw_experiment,  # 预留：RAW 实验查询函数
}


async def get_experiment_by_file_id(
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
    file_statement = select(FileInDB).where(FileInDB.file_id == file_id)
    file_record = await session.exec(file_statement).first()

    if not file_record:
        raise source_file_not_found_exception

    # 2. 检查文件是否属于当前用户
    user_file_ids = set(current_user.file_ids or [])
    if file_id not in user_file_ids:
        raise experiment_permission_denied_exception

    # 3. 根据 experiment_type 字段确定查询函数
    experiment_type = file_record.experiment_type
    if not experiment_type:
        raise unsupported_experiment_type_exception

    query_func = EXPERIMENT_QUERY_MAP.get(experiment_type)
    if not query_func:
        raise unsupported_experiment_type_exception

    # 4. 调用查询函数（只需要 file_id，不需要 user_id）
    experiment = await query_func(file_id, session)

    if not experiment:
        raise experiment_not_found_exception

    logger.debug(
        "get experiment by file_id user=%s file_id=%s exp_type=%s",
        current_user.id,
        file_id,
        experiment_type,
    )
    return experiment


#########################
# 依赖注入定义
#########################

GetExperimentByFileIdDep = Annotated[
    ExperimentImzMLPublic, Depends(get_experiment_by_file_id)
]
