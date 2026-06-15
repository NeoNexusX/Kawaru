import shutil
import zipfile
import re
import asyncio
from pathlib import Path

from sqlmodel.ext.asyncio.session import AsyncSession

from database.db import get_async_engine
from celery_runtime.state import get_celery_app
from celery_runtime.result import wait_for_result
from storage_backend.storage import LocalStorage, OssStorage
from storage_backend.oss_log_helper import (
    get_fc_invoke_instance_id_by_request_id,
    get_fc_invoke_logs_by_request_id,
    get_fc_invoke_status,
)
from api_services.experiment_service import (
    create_origin_record, mark_origin, delete_source_file_and_clear_path
)
from api_helpers.experiment_helper import validate_imzml_files
from api_services.file_service import get_file_record_by_id

from models.file_models import FileInDB, FileStatus
from models.user_models import UserInDB
from models.experiment_models import ExperimentStatus

from constants.file_constants import FC_UNZIP_TIMEOUT_SECONDS, OSS_ORIGIN_STORAGE_ROOT

from exceptions.experiment_exceptions import invalid_zip_file_exception, oss_fc_request_id_empty_exception
from exceptions.file_exceptions import storage_type_not_allowed_exception

from boot.config import get_config
from tools.logger import get_logger
logger = get_logger("background.experiment_background")
celery_app = get_celery_app()

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time

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

            # 阶段1: 查询文件记录并创建 origin 记录
            # 查询源文件
            source_file = await get_file_record_by_id(source_file_id, async_session)
            storage_type = source_file.storage_type

            # 创建 origin 记录
            origin_record = await create_origin_record(async_session, experiment_name, source_file_id)
            assert origin_record.id is not None

            #阶段2: 处理实验文件
            if storage_type == 'local':
                # 创建正式目录（helper 层）
                origin_dir = LocalStorage.create_origin_directories(origin_record.id)

                # 解压和验证
                final_imzml_path, final_ibd_path = await extract_and_validate_local_imzml_zip(Path(source_file.path), origin_dir)

            elif storage_type == 'oss':
                # 直接轮询oss端解压结果
                if fc_request_id is None:
                    raise oss_fc_request_id_empty_exception

                # 文件越大，计算时间越长，超时时间需要根据实际情况调整
                # 根据实验结果，2GB文件至少有32秒的延迟，这里根据文件大小动态调整超时时间
                zip_size_gb = source_file.size / (1024 * 1024 * 1024) # 转换为GB
                unzip_timeout = max(int(zip_size_gb * 40), 7) # 40秒/GB
                instance_id = await get_fc_invoke_instance_id_by_request_id(fc_request_id, timeout=unzip_timeout)
                if instance_id is None:
                    raise RuntimeError("get fc invoke instance id failed")

                # 等待函数计算完成
                assert FC_UNZIP_TIMEOUT_SECONDS is not None
                is_success = await get_fc_invoke_status(fc_request_id, timeout=10) # 根据实验结果，2GB文件至少有3秒的延迟，这里设置10秒超时
                if is_success is None:
                    raise RuntimeError("get fc invoke status failed")

                # 再获取并输出函数计算日志
                invoke_logs = await get_fc_invoke_logs_by_request_id(fc_request_id)
                if not invoke_logs:
                    logger.error(f"get fc invoke logs failed, request_id {fc_request_id}")
                    raise RuntimeError("get fc invoke logs failed")

                # 输出函数计算日志至后端日志系统
                for log in invoke_logs:
                    logger.info(f"fc invoke log:\n{log}")

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

            # 更新 origin 记录
            await mark_origin(async_session,origin_record,storage_type,final_imzml_path,final_ibd_path)

            # 标记源文件为已完成
            source_file.status = FileStatus.COMPLETED  # type: ignore
            async_session.add(source_file)

            # 保留原始 ZIP 文件与数据库 path 字段，由定时任务统一清理
            await async_session.commit()
            await async_session.refresh(origin_record)

            logger.info(f"completed imzml experiment processing exp_id={origin_record.id} source_file_id={source_file_id}")


        except (zipfile.BadZipFile, FileNotFoundError) as e:
            #此处理论上 origin_record 和 storage_type 一定存在，直接使用即可
            await mark_origin(async_session = async_session,
                                    origin=origin_record.id, # type:ignore
                                    storage_type=storage_type, # type:ignore
                                    error_message=str(e),
                                    status=ExperimentStatus.FAILED)
            if source_file:
                source_file.status = FileStatus.FAILED  # type: ignore
                async_session.add(source_file)
            await async_session.commit()
            await async_session.refresh(origin_record) # type: ignore
            logger.error(f"invalid file ,exp id & file id is {origin_record.id} & {source_file_id}\nerror={e}") #type:ignore
            raise invalid_zip_file_exception from e

        except RuntimeError as e:
            # 针对获取不到日志的情况，应当检查并删除oss端实验文件（防止出现获取不到日志但是实际解压成功的情况）
            if source_file:
                if source_file.storage_type == 'oss' and source_file is not None:
                    # TODO: 如何设计一个更可靠的机制
                    await asyncio.sleep(20) # 确保文件解压完成，再删除
                    await OssStorage.delete_file(f"{OSS_ORIGIN_STORAGE_ROOT.strip('/')}/{source_file.filename}/") # type: ignore

            # 回滚 origin 记录的修改，保持数据库一致性
            await async_session.rollback()

            # 删除原始zip，清理记录的file的path信息
            if source_file:
                async with AsyncSession(get_async_engine()) as async_session:
                    await delete_source_file_and_clear_path(async_session, source_file_id)
                    await async_session.commit()

            logger.error("imzml experiment processing failed error=%s", e)

            raise e


        except Exception as e:
            # 针对其他的异常，回滚 origin 记录的修改，保持数据库一致性
            await async_session.rollback()

            # 删除原始zip，清理记录的file的path信息
            if source_file:
                async with AsyncSession(get_async_engine()) as async_session:
                    await delete_source_file_and_clear_path(async_session, source_file_id)
                    await async_session.commit()

            logger.error("imzml experiment processing failed error=%s", e)
            raise


async def extract_and_validate_local_imzml_zip(zip_file_path: Path, target_dir: Path) -> tuple[Path, Path]:
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
        result = await wait_for_result(res, poll_interval=0.1, timeout=large_file_poll_time) # type: ignore
        if result is None:
            logger.error(f"failed to extract and validate imzml zip due to server timeout: {zip_file_path}")
            raise TimeoutError("extract and validate imzml zip timeout")
    except Exception as e:
        logger.error(f"extract and validate imzml zip failed error={e}")
        raise e
    imzml_path_str, ibd_path_str = result
    return Path(imzml_path_str), Path(ibd_path_str)


@celery_app.task()
async def extract_and_validate_local_imzml_zip_task(zip_file_path_str: str, target_dir_str: str) -> tuple[str, str]:
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
        with zipfile.ZipFile(zip_file_path_str, "r") as zip_ref:
            # 验证 ZIP 条目以防止路径遍历（Zip Slip）
            temp_dir_resolved = Path(target_dir_str).resolve()
            for member in zip_ref.namelist():
                member_path = (Path(target_dir_str) / member).resolve()
                # 检查路径是否在临时目录内（避免路径遍历攻击）
                if not member_path.is_relative_to(temp_dir_resolved):
                    logger.error("unsafe file path in zip file: %s", member)
                    # TODO: 应该返回封禁标志，封禁该用户所有操作
                    raise ValueError("unsafe file path in zip file")
            zip_ref.extractall(target_dir_str)

        # 查找 imzML 和 ibd 文件
        imzml_path = None
        ibd_path = None

        for file_path in Path(target_dir_str).iterdir():
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
        shutil.rmtree(target_dir_str, ignore_errors=True)
        raise

    return str(imzml_path), str(ibd_path)


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
            final_imzml_path = re.search(imzml_pattern, log).group(1).strip() # type: ignore

        if not final_ibd_path and "[ibd_path]" in log:
            final_ibd_path = re.search(ibd_pattern, log).group(1).strip() # type: ignore

    return final_imzml_path, final_ibd_path
