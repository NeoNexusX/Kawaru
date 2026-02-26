import zipfile
from pathlib import Path

from fastapi import HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession

from api.experiment_api import logger
from api.experiment_api_business import create_experiment_record, delete_source_file_and_clear_path, mark_experiment
from api.experiment_api_helper import create_experiment_directories, extract_and_validate_imzml_zip
from api.file_api_business import get_file_record_by_id
from database.db import get_async_engine
from exceptions.experiment_exceptions import invalid_zip_file_exception
from models.experiment_models import ExperimentStatus


async def process_imzml_experiment_background(
    user_id: int,
    source_file_id: int,
    experiment_name: str
):
    """
    后台处理 ImzML 实验的同步任务

    存储路径：
    - 临时目录:experiments/temp/exp_{id}/
    - 正式目录:experiments/processed/exp_{id}/
    """
    source_file = None
    try:
        logger.info(f"starting imzml experiment processing user={user_id} file={source_file_id}")

        async with AsyncSession(get_async_engine()) as async_session:
            # 阶段1: 查询文件记录并创建实验记录
            # 查询源文件
            source_file = await get_file_record_by_id(source_file_id, async_session)

            # 创建实验记录
            experiment_record = await create_experiment_record(async_session, experiment_name, source_file_id)

            #阶段2: 处理实验文件
            # 创建正式目录（helper 层）
            experiment_dir = create_experiment_directories(experiment_record.id)#type:ignore

            # 解压和验证
            final_imzml_path, final_ibd_path = extract_and_validate_imzml_zip(Path(source_file.path), experiment_dir)

            # 更新实验记录
            await mark_experiment(async_session,experiment_record,final_imzml_path,final_ibd_path)

            # 删除原始 ZIP 文件并修改文件表中的 path 字段（helper 层）
            await async_session.commit()

            logger.info(f"deleted source zip file {source_file_id} and completed imzml experiment processing exp_id={experiment_record.id}")

    except HTTPException as e:
        logger.error(e)
        raise e

    except (zipfile.BadZipFile, FileNotFoundError) as e:
        async with AsyncSession(get_async_engine()) as async_session:
            #此处理论上experiment_record一定存在，直接使用即可
            await mark_experiment(async_session = async_session,
                                    experiment=experiment_record.id, #type:ignore
                                    error_message=str(e),
                                    status=ExperimentStatus.FAILED)
            await async_session.commit()
            logger.error(f"invalid file ,exp id & file id is {experiment_record.id} & {source_file_id}\nerror={e}") #type:ignore
            raise invalid_zip_file_exception from e

    except Exception as e:
        logger.error("imzml experiment processing failed error=%s", e)
        raise

    finally:
        async with AsyncSession(get_async_engine()) as async_session:
            # 删除原始zip，清理记录的file的path信息
            if source_file:
                await delete_source_file_and_clear_path(async_session, source_file)
                await async_session.commit()
