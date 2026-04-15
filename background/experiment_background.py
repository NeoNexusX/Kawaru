import shutil
import zipfile
from pathlib import Path

from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import HTTPException

from storage_backend.storage import LocalStorage, OssStorage
from database.db import get_async_engine
from background.celery_app import celery_app, wait_for_result
from api.experiment_api_business import (
    create_experiment_record, mark_experiment, delete_source_file_and_clear_path
)
from api.experiment_api_helper import validate_imzml_files
from api.file_api_helper import get_file_record_by_id

from models.file_models import FileInDB
from models.user_models import UserInDB
from models.experiment_models import ExperimentStatus

from exceptions.experiment_exceptions import invalid_zip_file_exception

from tools.logger import get_logger
logger = get_logger("background.experiment_background")

async def trigger_experiment_extraction(
    file_record: FileInDB,
    current_user: UserInDB,
    experiment_type: str,
) -> None:
    """
    根据实验类型触发后台实验文件提取任务
    """
    if experiment_type != "imzML":
        return

    if not current_user.id or not file_record.file_id:
        logger.warning(f"skip experiment extraction: missing user_id :{current_user.id} or file_id {file_record.file_id}")
        return

    experiment_name = Path(file_record.filename).stem
    await process_imzml_experiment_background(current_user.id, file_record.file_id, experiment_name) # type: ignore
    logger.info(f"auto extraction triggered user={current_user.id} file_id={file_record.file_id}")


async def process_imzml_experiment_background(
    user_id: int,
    source_file_id: int,
    experiment_name: str
):
    """
    后台处理 ImzML 实验的任务

    存储路径：
    - 临时目录:experiments/temp/exp_{id}/
    - 正式目录:experiments/processed/exp_{id}/
    """
    source_file = None
    async with AsyncSession(get_async_engine()) as async_session:
        try:
            logger.info(f"starting imzml experiment processing user={user_id} file={source_file_id}")

            # 阶段1: 查询文件记录并创建实验记录
            # 查询源文件
            source_file = await get_file_record_by_id(source_file_id, async_session)
            storage_type = source_file.storage_type

            # 创建实验记录
            experiment_record = await create_experiment_record(async_session, experiment_name, source_file_id)
            assert experiment_record.id is not None

            #阶段2: 处理实验文件
            # 创建正式目录（helper 层）
            experiment_dir = None
            # TODO: 封装存储分支逻辑
            if storage_type == 'local':
                experiment_dir = LocalStorage.create_experiment_directories(experiment_record.id) # type: ignore
            elif storage_type == 'oss':
                experiment_dir = OssStorage.create_experiment_directories(experiment_record.id) # type: ignore

            # 解压和验证
            final_imzml_path, final_ibd_path = await extract_and_validate_local_imzml_zip(Path(source_file.path), experiment_dir) # type:ignore

            # 更新实验记录
            await mark_experiment(async_session,experiment_record,final_imzml_path,final_ibd_path)

            # 保留原始 ZIP 文件与数据库 path 字段，由定时任务统一清理
            await async_session.commit()
            await async_session.refresh(experiment_record)

            logger.info(f"completed imzml experiment processing exp_id={experiment_record.id} source_file_id={source_file_id}")

        except HTTPException as e:
            logger.error(e)
            raise e

        except (zipfile.BadZipFile, FileNotFoundError) as e:
            #此处理论上experiment_record一定存在，直接使用即可
            await mark_experiment(async_session = async_session,
                                    experiment=experiment_record.id, #type:ignore
                                    error_message=str(e),
                                    status=ExperimentStatus.FAILED)
            await async_session.commit()
            await async_session.refresh(experiment_record) # type: ignore
            logger.error(f"invalid file ,exp id & file id is {experiment_record.id} & {source_file_id}\nerror={e}") #type:ignore
            raise invalid_zip_file_exception from e

        except Exception as e:
            # 针对其他的异常，回滚实验记录的修改，保持数据库一致性
            source_file_id = source_file.file_id # type: ignore
            await async_session.rollback()

            async with AsyncSession(get_async_engine()) as async_session:
                # 删除原始zip，清理记录的file的path信息
                if source_file:
                    await delete_source_file_and_clear_path(async_session, source_file_id)
                    await async_session.commit()
            logger.error("imzml experiment processing failed error=%s", e)
            raise


async def extract_and_validate_local_imzml_zip(
    zip_file_path: Path,
    target_dir: Path,
) -> tuple[Path, Path]:
    """
    解压并验证 imzML ZIP 文件,ZIP 文件必须包含两个文件：.imzML 和 .ibd,包含 Zip Slip 路径遍历攻击防护

    Args:
        zip_file_path: ZIP 文件路径
        target_dir: 解压目标目录

    Returns:
        成功时返回 (imzml_path, ibd_path) 元组

    Raise:
        - zipfile.BadZipFile: ZIP 文件无效
        - FileNotFoundError: 缺少 .imzML 或 .ibd 文件
        - ValueError: ZIP 文件包含不安全的路径
    """
    try:
        res = extract_and_validate_local_imzml_zip_task.delay(str(zip_file_path), str(target_dir)) # type: ignore
        result = await wait_for_result(res, poll_interval=0.1, timeout=1000) # type: ignore # 针对大文件此处timeout需要调大
        if result is None:
            logger.error(f"failed to extract and validate imzml zip due to server timeout: {zip_file_path}")
            raise TimeoutError("extract and validate imzml zip timeout")
    except Exception as e:
        logger.error(f"extract and validate imzml zip failed error={e}")
        raise e
    imzml_path_str, ibd_path_str = result
    return Path(imzml_path_str), Path(ibd_path_str)


@celery_app.task() # type: ignore
def extract_and_validate_local_imzml_zip_task(zip_file_path_str: str, target_dir_str: str) -> tuple[str, str]:
    """
    解压并验证 imzML ZIP 文件,ZIP 文件必须包含两个文件：.imzML 和 .ibd,包含 Zip Slip 路径遍历攻击防护

    Args:
        zip_file_path: ZIP 文件路径
        target_dir: 解压目标目录

    Returns:
        成功时返回 (imzml_path, ibd_path) 元组

    Raise:
        - zipfile.BadZipFile: ZIP 文件无效
        - FileNotFoundError: 缺少 .imzML 或 .ibd 文件
        - ValueError: ZIP 文件包含不安全的路径
    """
    # 任务队列传过来是str，这里转换回来方便操作
    target_dir = Path(target_dir_str)
    zip_file_path = Path(zip_file_path_str)

    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            # 验证 ZIP 条目以防止路径遍历（Zip Slip）
            temp_dir_resolved = target_dir.resolve()
            for member in zip_ref.namelist():
                member_path = (target_dir / member).resolve()
                # 检查路径是否在临时目录内（避免路径遍历攻击）
                if not member_path.is_relative_to(temp_dir_resolved):
                    logger.error("unsafe file path in zip file: %s", member)
                    # TODO: 应该返回封禁标志，封禁该用户所有操作
                    raise ValueError("unsafe file path in zip file")
            zip_ref.extractall(target_dir)

        # 查找 imzML 和 ibd 文件
        imzml_path = None
        ibd_path = None

        for file_path in target_dir.iterdir():
            if file_path.suffix == ".imzML":
                imzml_path = file_path
            elif file_path.suffix.lower() == ".ibd":
                ibd_path = file_path

        # 验证文件是否存在
        if not imzml_path or not ibd_path:
            logger.error("zip file must contain both .imzML and .ibd files")
            raise FileNotFoundError("ZIP file must contain both .imzML and .ibd files")

        # 验证文件（目前直接通过）
        if not validate_imzml_files(imzml_path, ibd_path):
            logger.error("imzML file validation failed")
            raise AssertionError("Invalid imzML or ibd file")

    except Exception as e:
        logger.error("invalid zip file: %s", e)
        shutil.rmtree(target_dir, ignore_errors=True)
        raise

    return str(imzml_path), str(ibd_path)
