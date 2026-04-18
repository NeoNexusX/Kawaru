import shutil
import zipfile
import re
from pathlib import Path

from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import HTTPException

from storage_backend.storage import LocalStorage
from storage_backend.oss_log_helper import(
    get_fc_invoke_status_task,
    get_fc_invoke_instance_id_by_request_id_task,
    get_fc_invoke_logs_by_instance_id_task
)
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

from constants.file_constants import FC_TIMEOUT_SECONDS

from exceptions.experiment_exceptions import invalid_zip_file_exception, oss_fc_request_id_empty_exception
from exceptions.file_exceptions import storage_type_not_allowed_exception
from exceptions.common_exceptions import internal_server_error_exception

from tools.logger import get_logger
logger = get_logger("background.experiment_background")

async def trigger_experiment_extraction(
    file_record: FileInDB,
    current_user: UserInDB,
    experiment_type: str,
    fc_request_id: str | None = None,
) -> None:
    """
    根据实验类型触发后台实验文件提取任务（仅触发celery任务，不会阻塞）
    - 对于本地存储，执行本地解压任务
    - 对于OSS存储，读取并解析oss端自动提取任务的结果

    :param file_record: 文件记录
    :param current_user: 当前用户
    :param experiment_type: 实验类型
    :type fc_request_id: str | None = None
    :param fc_request_id: oss上传后自动触发的函数调用ID（可在oss上传完成后返回的响应head获取到）
    """
    if experiment_type != "imzML":
        return

    if not current_user.id or not file_record.file_id:
        logger.warning(f"skip experiment extraction: missing user_id :{current_user.id} or file_id {file_record.file_id}")
        return

    experiment_name = Path(file_record.filename).stem
    await process_imzml_experiment_background(current_user.id, file_record.file_id, experiment_name, fc_request_id) # type: ignore
    logger.info(f"auto extraction triggered user={current_user.id} file_id={file_record.file_id}")


async def process_imzml_experiment_background(
    user_id: int,
    source_file_id: int,
    experiment_name: str,
    fc_request_id: str | None = None,
) -> None:
    """
    后台处理 ImzML 实验的任务

    存储路径：
    - 临时目录:experiments/temp/exp_{id}/
    - 正式目录:experiments/processed/exp_{id}/
    """
    process_imzml_experiment_background_task.delay(user_id, source_file_id, experiment_name, fc_request_id) # type: ignore


@celery_app.task()
async def process_imzml_experiment_background_task(
    user_id: int,
    source_file_id: int,
    experiment_name: str,
    fc_request_id: str | None = None,
) -> None:
    """
    后台处理 ImzML 实验的任务

    存储路径：
    - 临时目录:experiments/temp/exp_{id}/
    - 正式目录:experiments/exp_{id}/
    - OSS存储目录:oss://{bucket}/experiments/filename/
    """
    source_file = None
    async with AsyncSession(get_async_engine(standalone=True)) as async_session:
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
            if storage_type == 'local':
                # 创建正式目录（helper 层）
                experiment_dir = LocalStorage.create_experiment_directories(experiment_record.id)

                # 解压和验证
                final_imzml_path, final_ibd_path = extract_and_validate_local_imzml_zip(Path(source_file.path), experiment_dir)

            elif storage_type == 'oss':
                # 直接轮询oss端解压结果
                if fc_request_id is None:
                    raise oss_fc_request_id_empty_exception

                # 先获取函数计算的instance_id(说明函数计算开始调用)
                instance_id = await get_fc_invoke_instance_id_by_request_id(fc_request_id)
                if instance_id is None:
                    raise internal_server_error_exception

                # 等待函数计算完成
                is_success = await get_fc_invoke_status(fc_request_id, timeout=FC_TIMEOUT_SECONDS) # 文件越大，计算时间越长，超时时间需要根据实际情况调整
                if is_success is None:
                    raise internal_server_error_exception

                # 再获取并输出函数计算日志
                invoke_logs = await get_fc_invoke_logs_by_request_id(fc_request_id)
                if not invoke_logs:
                    logger.error(f"get fc invoke logs failed, request_id {fc_request_id}")
                    raise internal_server_error_exception

                # 输出函数计算日志至后端日志系统
                for log in invoke_logs:
                    logger.info(f"fc invoke log: {log}")

                # 如果函数计算失败，说明解压过程报错，抛出异常
                if not is_success:
                    raise zipfile.BadZipFile

                # 解析函数计算日志，提取出实验文件最终存储路径
                final_imzml_path, final_ibd_path = parse_fc_invoke_logs(invoke_logs)

                # 检查是否成功提取路径，失败说明压缩包解压失败
                if final_imzml_path is None or final_ibd_path is None:
                    raise zipfile.BadZipFile

            else:
                raise storage_type_not_allowed_exception

            # 更新实验记录
            await mark_experiment(async_session,experiment_record,storage_type,final_imzml_path,final_ibd_path)

            # 保留原始 ZIP 文件与数据库 path 字段，由定时任务统一清理
            await async_session.commit()
            await async_session.refresh(experiment_record)

            logger.info(f"completed imzml experiment processing exp_id={experiment_record.id} source_file_id={source_file_id}")

        except HTTPException as e:
            logger.error(e)
            raise e

        except (zipfile.BadZipFile, FileNotFoundError) as e:
            #此处理论上experiment_record和storage_type一定存在，直接使用即可
            await mark_experiment(async_session = async_session,
                                    experiment=experiment_record.id, # type:ignore
                                    storage_type=storage_type, # type:ignore
                                    error_message=str(e),
                                    status=ExperimentStatus.FAILED)
            await async_session.commit()
            await async_session.refresh(experiment_record) # type: ignore
            logger.error(f"invalid file ,exp id & file id is {experiment_record.id} & {source_file_id}\nerror={e}") #type:ignore
            raise invalid_zip_file_exception from e

        except Exception as e:
            # 针对其他的异常，回滚实验记录的修改，保持数据库一致性
            await async_session.rollback()

            async with AsyncSession(get_async_engine()) as async_session:
                # 删除原始zip，清理记录的file的path信息
                if source_file:
                    await delete_source_file_and_clear_path(async_session, source_file_id)
                    await async_session.commit()
            logger.error("imzml experiment processing failed error=%s", e)
            raise


def extract_and_validate_local_imzml_zip(zip_file_path: Path, target_dir: Path) -> tuple[Path, Path]:
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

    return imzml_path, ibd_path


async def  get_fc_invoke_status(request_id: str, timeout: int = 20) -> bool | None:
    """
    根据request_id获取函数计算成功状态

    :param request_id: 函数调用ID
    :param timeout: 超时时间，默认20秒，大压缩文件可能需要更长的时间

    :return: 返回True表示成功，False表示失败
    """
    res = get_fc_invoke_status_task.delay(request_id) # type: ignore
    is_success: bool | None = await wait_for_result(res, poll_interval=0.1, timeout=timeout) # type: ignore
    if is_success is None:
        logger.error(f"get fc invoke status failed due to server timeout, request_id={request_id}")
        return None
    return is_success


async def get_fc_invoke_instance_id_by_request_id(request_id: str) -> str | None:
    """
    根据request_id获取函数计算的instance_id
    这里超时可以不用设太高，因为函数计算只要一触发就会有instance_id
    """
    res = get_fc_invoke_instance_id_by_request_id_task.delay(request_id) # type: ignore
    instance_id: str | None = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
    if instance_id is None:
        logger.error(f"get fc invoke instance id failed due to server timeout, request_id={request_id}")
        return None
    return instance_id


async def get_fc_invoke_logs_by_request_id(request_id: str) -> list[str] | None:
    """
    根据request_id获取函数计算日志
    这里超时可以不用设太高，等待调用get_fc_invoke_status之后再获取日志，确保函数计算完整日志写入日志服务
    """
    instance_id = await get_fc_invoke_instance_id_by_request_id(request_id)
    res = get_fc_invoke_logs_by_instance_id_task.delay(instance_id) # type: ignore
    logs: list[str] | None = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
    if logs is None:
        logger.error(f"get fc invoke logs failed due to server timeout, instance_id={instance_id}")
        return None

    # 过滤出包含request_id的日志
    logs = [log for log in logs if request_id in log]

    return logs


def parse_fc_invoke_logs(logs: list[str]) -> tuple[str | None, str | None]:
    """
    解析函数计算日志，提取出实验文件最终存储路径
    """

    # 解析日志，提取出实验文件最终存储路径
    final_imzml_path = None
    final_ibd_path = None

    # 正则
    imzml_pattern = r"\[imzML_path\](\S+)"
    ibd_pattern = r"\[ibd_path\](\S+)"

    # 匹配并提取路径
    for log in logs:
        if not final_imzml_path and "[imzML_path]" in log:
            final_imzml_path = re.search(imzml_pattern, log).group(1).strip()

        if not final_ibd_path and "[ibd_path]" in log:
            final_ibd_path = re.search(ibd_pattern, log).group(1).strip()

    return final_imzml_path, final_ibd_path
