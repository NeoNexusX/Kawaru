#########################
# Experiment API
#########################
from __future__ import annotations

import asyncio
import zipfile
import tempfile
import shutil
from pathlib import Path
from datetime import timedelta
from typing import Annotated

from fastapi import Depends, HTTPException
from sqlmodel import Session, select
from database.db import sync_engine
from starlette import status

from api.user_api import token_authen
from database.db import SessionDep
from envset.config import get_config
from logger import get_logger
from models.user_models import UserInDB
from models.file_models import FileInDB, now_local_timezone
from models.experiment_models import ExperimentImzMLInDB, ExperimentImzMLPublic

logger = get_logger("main.experiment_api")

# 获取配置
config = get_config()
storage_config = config.storage
STORAGE_ROOT = Path(storage_config.root)
EXPERIMENT_STORAGE_ROOT = STORAGE_ROOT / "experiments"
EXPERIMENT_TEMP_ROOT = EXPERIMENT_STORAGE_ROOT / "temp"  # 【临时目录】解压时的临时文件存放在 experiments/temp 下
FAILED_EXPERIMENT_TTL_MINUTES = int(getattr(storage_config, "failed_experiment_ttl_minutes", 30))  # 【配置项】失败实验记录的过期时间（分钟）

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
# 同步辅助函数（供 asyncio.to_thread 调用）
#########################

def _ensure_experiment_dirs():
    """确保实验存储目录存在（同步）"""
    EXPERIMENT_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    EXPERIMENT_TEMP_ROOT.mkdir(parents=True, exist_ok=True)


def _validate_imzml_files(imzml_path: Path, ibd_path: Path) -> bool:
    """验证 imzML 和 ibd 文件（同步）"""
    try:
        if not imzml_path.exists() or not ibd_path.exists():
            return False
        if imzml_path.stat().st_size == 0 or ibd_path.stat().st_size == 0:
            return False
        with open(imzml_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(1000)
            if 'imzML' not in content and 'mzML' not in content:
                return False
        return True
    except Exception as e:
        logger.error(f"imzML validation error: {e}")
        return False


def _extract_and_validate_zip(zip_file_path: Path, temp_dir: Path) -> tuple[Path, Path]:
    """解压 ZIP 文件并验证内容（同步）"""
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except zipfile.BadZipFile:
        raise Exception("invalid zip file format")
    except Exception as e:
        logger.error(f"zip extraction failed: {e}")
        raise Exception(f"experiment processing failed: {e}")
    
    imzml_files = list(temp_dir.rglob("*.imzML"))
    ibd_files = list(temp_dir.rglob("*.ibd"))
    
    if not imzml_files or not ibd_files:
        raise Exception("zip file must contain both .imzML and .ibd files")
    
    imzml_path = imzml_files[0]
    ibd_path = ibd_files[0]
    
    if not _validate_imzml_files(imzml_path, ibd_path):
        raise Exception("imzML file validation failed")
    
    return imzml_path, ibd_path


#########################
# 异步辅助函数
#########################

async def ensure_experiment_dirs():
    """确保实验存储目录存在（异步）"""
    await asyncio.to_thread(_ensure_experiment_dirs)


async def extract_and_validate_zip(zip_file_path: Path, temp_dir: Path) -> tuple[Path, Path]:
    """解压 ZIP 文件并验证内容（异步）"""
    return await asyncio.to_thread(_extract_and_validate_zip, zip_file_path, temp_dir)


#########################
# 异步清理函数
#########################

async def cleanup_failed_experiments(session: SessionDep, ttl_minutes: int = FAILED_EXPERIMENT_TTL_MINUTES) -> int:
    """
    清理过期的失败实验记录（异步）
    
    Args:
        session: 数据库会话
        ttl_minutes: 过期时间阈值（分钟）
    
    Returns:
        int: 清理的记录数
    """
    now = now_local_timezone()
    threshold = now - timedelta(minutes=ttl_minutes)
    cleaned = 0
    
    try:
        failed_experiments = (await session.exec(
            select(ExperimentImzMLInDB).where(
                ExperimentImzMLInDB.status == "failed",
                ExperimentImzMLInDB.created_at < threshold
            )
        )).all()
        
        for exp in failed_experiments:
            try:
                # 异步删除物理文件
                if exp.imzml_path:
                    await asyncio.to_thread(lambda p: Path(p).unlink(missing_ok=True), exp.imzml_path)
                if exp.ibd_path:
                    await asyncio.to_thread(lambda p: Path(p).unlink(missing_ok=True), exp.ibd_path)
                
                # 删除实验目录（如果为空）
                if exp.imzml_path:
                    experiment_dir = Path(exp.imzml_path).parent
                    def _rmdir_if_empty(d: Path):
                        if d.exists() and not any(d.iterdir()):
                            d.rmdir()
                    await asyncio.to_thread(_rmdir_if_empty, experiment_dir)
                
                # 删除数据库记录
                session.delete(exp)
                cleaned += 1
                logger.debug("cleanup failed experiment: exp_id=%s", exp.id)
            except Exception as e:
                logger.error("failed to cleanup experiment exp_id=%s error=%s", exp.id, e)
        
        await session.commit()
        logger.info("cleanup failed experiments completed count=%s", cleaned)
        
    except Exception as e:
        logger.error("cleanup failed experiments error=%s", e)
    
    return cleaned


#########################
# 后台处理函数（同步，供 BackgroundTasks 调用）
# 注意：BackgroundTasks.add_task 要求同步函数
#########################

def process_imzml_experiment_background(user_id: int, source_file_id: int, experiment_name: str):
    """
    后台处理 ImzML 实验的同步任务
    注意：此函数必须为同步，因为 FastAPI BackgroundTasks 在线程池中执行
    """
    temp_dir = None
    experiment_record = None
    
    try:
        logger.info("starting imzml experiment processing user=%s file=%s", user_id, source_file_id)
        
        with Session(sync_engine) as session:
            source_file = session.exec(
                select(FileInDB).where(FileInDB.file_id == source_file_id)
            ).first()
            
            if not source_file:
                logger.error("source file not found file_id=%s", source_file_id)
                return
            
            # 创建实验记录
            experiment_record = ExperimentImzMLInDB(
                experiment_name=experiment_name,
                user_id=user_id,
                source_file_id=source_file_id,
                status="processing"
            )
            session.add(experiment_record)
            session.commit()
            session.refresh(experiment_record)
            
            # 创建临时目录（在 experiments/temp 下）
            _ensure_experiment_dirs()
            temp_dir = Path(tempfile.mkdtemp(prefix="imzml_extract_", dir=EXPERIMENT_TEMP_ROOT))
            
            # 解压和验证
            source_path = Path(source_file.path)
            imzml_temp_path, ibd_temp_path = _extract_and_validate_zip(source_path, temp_dir)
            
            # 准备最终存储路径
            experiment_dir = EXPERIMENT_STORAGE_ROOT / f"user_{user_id}" / f"exp_{experiment_record.id}"
            experiment_dir.mkdir(parents=True, exist_ok=True)
            
            # 移动文件到最终位置
            final_imzml_path = experiment_dir / f"{experiment_name}.imzML"
            final_ibd_path = experiment_dir / f"{experiment_name}.ibd"
            
            shutil.move(str(imzml_temp_path), str(final_imzml_path))
            shutil.move(str(ibd_temp_path), str(final_ibd_path))
            
            # 更新实验记录
            experiment_record.imzml_path = str(final_imzml_path)
            experiment_record.imzml_size = final_imzml_path.stat().st_size
            experiment_record.ibd_path = str(final_ibd_path)
            experiment_record.ibd_size = final_ibd_path.stat().st_size
            experiment_record.status = "completed"
            
            session.add(experiment_record)
            session.commit()
            
            # 删除原始ZIP文件
            try:
                source_path.unlink()
                logger.info("deleted source zip file path=%s", source_path)
            except Exception as e:
                logger.warning("failed to delete source zip file error=%s", e)
            
            logger.info("imzml experiment processing completed exp_id=%s", experiment_record.id)
        
    except Exception as e:
        logger.error("imzml experiment processing failed error=%s", e)
        
        if experiment_record:
            try:    
                with Session(sync_engine) as session:
                    exp_record = session.exec(
                        select(ExperimentImzMLInDB).where(ExperimentImzMLInDB.id == experiment_record.id)
                    ).first()
                    if exp_record:
                        exp_record.status = "failed"
                        exp_record.error_message = str(e)
                        session.add(exp_record)
                        session.commit()
                        
                        # 删除原始ZIP文件（失败时也删除）
                        try:
                            source_file = session.exec(
                                select(FileInDB).where(FileInDB.file_id == source_file_id)
                            ).first()
                            if source_file:
                                Path(source_file.path).unlink(missing_ok=True)
                                logger.info("deleted source zip file after failure path=%s", source_file.path)
                        except Exception as cleanup_error:
                            logger.warning("failed to delete source zip file after failure error=%s", cleanup_error)
                            
            except Exception as update_error:
                logger.error("failed to update experiment status error=%s", update_error)
        
    finally:
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.error("failed to cleanup temp directory error=%s", e)


#########################
# 异步查询函数
#########################

async def get_imzml_experiment(file_id: int, user_id: int, session: SessionDep) -> ExperimentImzMLPublic | None:
    """查询 imzML 类型实验（异步）"""
    result = await session.exec(
        select(ExperimentImzMLInDB).where(
            ExperimentImzMLInDB.source_file_id == file_id,
            ExperimentImzMLInDB.user_id == user_id
        )
    )
    return result.first()


# 实验类型到查询函数的映射
# 【扩展接口】后续添加新实验类型时，在此添加映射即可
# 注意：查询函数应为异步函数
EXPERIMENT_QUERY_MAP = {
    "imzML": get_imzml_experiment,
    # "raw": get_raw_experiment,  # 预留：RAW 实验查询函数
}


async def get_experiment_by_file_id(
    file_id: int,
    current_user: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep
) -> ExperimentImzMLPublic:
    """
    根据源文件ID查询实验记录，根据文件的 experiment_type 字段定位对应实验表（异步）
    
    Args:
        file_id: 源文件ID
        current_user: 当前用户
        session: 数据库会话
    
    Returns:
        实验记录（目前仅支持 ExperimentImzMLPublic）
    """
    # 先清理过期的失败实验记录
    try:
        await cleanup_failed_experiments(session)
    except Exception:
        pass
    
    # 1. 先查询文件记录是否存在（异步）
    async def _query_file():
        result = await session.exec(
            select(FileInDB).where(FileInDB.file_id == file_id)
        )
        return result.first()
    
    file_record = await _query_file()
    
    if not file_record:
        raise source_file_not_found_exception
    
    # 2. 再检查文件是否属于当前用户
    user_file_ids = set(current_user.file_ids or [])
    if file_id not in user_file_ids:
        raise experiment_permission_denied_exception
    
    # 3. 根据 experiment_type 字段确定查询函数（通过文件记录的实验类型定位对应实验表）
    experiment_type = file_record.experiment_type
    if not experiment_type:
        raise unsupported_experiment_type_exception
    
    query_func = EXPERIMENT_QUERY_MAP.get(experiment_type)
    if not query_func:
        raise unsupported_experiment_type_exception
    
    # 调用异步查询函数
    experiment = await query_func(file_id, current_user.id, session)
    
    if not experiment:
        raise experiment_not_found_exception
    
    logger.debug("get experiment by file_id user=%s file_id=%s exp_type=%s", 
                current_user.id, file_id, experiment_type)
    return experiment


#########################
# 依赖注入定义
#########################

GetExperimentByFileIdDep = Annotated[ExperimentImzMLPublic, Depends(get_experiment_by_file_id)]
