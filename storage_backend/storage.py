from pathlib import Path
from typing import Sequence
from celery import group
from celery.result import GroupResult, AsyncResult

from celery_runtime.result import wait_for_result
from background.file_background_helper import (
    get_oss_upload_sts_token_task,
    compute_upload_local_file_hash_task,
    check_oss_file_exist_task,
    validate_oss_file_task,
    get_oss_file_size_task,
    delete_oss_file_task,
    get_oss_packup_sts_token_task,
    tag_oss_file_task,
)

from models.auth import OssStsToken
from constants.file_constants import (LOCAL_STORAGE_ROOT, STORAGE_ROOT, TEMP_PARTS_ROOT,
                                      OSS_ZIP_STORAGE_ROOT, OSS_DEFAULT_TIMEOUT_SECONDS,
                                      OSS_PACKUP_ZIP_STORAGE_ROOT
                                     )
from constants.experiment_constants import ORIGIN_STORAGE_ROOT, OSS_ORIGIN_STORAGE_ROOT

from tools.logger import get_logger

from boot.config import get_config
logger = get_logger("storage")

celery_worker_config = get_config().celery
large_file_poll_time = celery_worker_config.large_file_poll_time

class LocalStorage:
    """
    本地存储相关操作
    """
    @staticmethod
    def ensure_storage_dirs() -> None:
        """
        确保本地存储目录存在
        在上传操作前调用,创建必要的存储目录结构
        """
        for path in (STORAGE_ROOT, LOCAL_STORAGE_ROOT, TEMP_PARTS_ROOT):
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def prepare_storage_path(filename: str,file_type: str) -> Path:
        """
        为简单上传准备最终保存路径

        文件直接保存在 LOCAL_STORAGE_ROOT 下,不按用户ID分文件夹
        """
        LOCAL_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        file_path=f"{filename}.{file_type}"
        return LOCAL_STORAGE_ROOT / file_path

    @staticmethod
    def create_origin_directories(origin_id: int) -> Path:
        """
        创建并返回 origin目录
        Return:
            processed_dir Path对象
        Raise:
            None
        """
        processed_dir = ORIGIN_STORAGE_ROOT / f"org_{origin_id}"
        processed_dir.mkdir(parents=True, exist_ok=True)

        return processed_dir

    @staticmethod
    def delete_file(path: str | Path) -> None:
        """
        删除本地文件

        同步操作,因为文件删除只是删除元数据
        使用 missing_ok=True,文件不存在时不会抛出异常

        异常处理：不捕获,让异常传播到调用方

        Args:
            path: 文件路径(字符串或 Path 对象)
        """
        # 验证路径不为空
        if not path:
            logger.warning("empty path for deletion, skipping")
            return


        # 转换为字符串进行验证
        path_str = str(path).strip()

        # 验证路径不是特殊值
        if not path_str or any(x in path_str for x in ('..', '\\')):
            logger.warning("invalid path for deletion: %r, skipping", path_str)
            return

        # 转换为 Path 对象
        path_obj = Path(path_str)

        # 验证路径不是目录
        if path_obj.exists() and path_obj.is_dir():
            logger.warning("cannot delete directory: %s, skipping", path_str)
            return

        # 删除文件
        path_obj.unlink(missing_ok=True)

    @staticmethod
    async def compute_upload_file_hash(file_path: str) -> tuple[str, int]:
        """
        计算上传文件元数据的本地版本(md值和大小)
        分片读取指定本地路径的文件内容,避免一次性加载到内存
        """
        res = compute_upload_local_file_hash_task.delay(file_path) # type: ignore
        result: tuple[str, int] = await wait_for_result(res, poll_interval=0.1, timeout=large_file_poll_time) # type: ignore
        if result is None:
            logger.error(f"failed to compute upload file hash due to server timeout: {file_path}")
            return ("", 0)
        return result


class OssStorage:
    """
    OSS存储相关操作
    """
    @staticmethod
    def prepare_storage_path(filename: str, file_type: str) -> str:
        """
        为OSS存储准备最终保存路径

        文件直接保存在 OSS 存储桶的 OSS_ZIP_STORAGE_ROOT 下
        """
        return f"{OSS_ZIP_STORAGE_ROOT.rstrip('/')}/{filename}.{file_type}" # type: ignore

    @staticmethod
    def create_origin_directories(origin_id: int) -> str:
        """
        创建并返回 origin目录
        Return:
            str路径
        Raise:
            None
        """
        processed_dir = f"{OSS_ORIGIN_STORAGE_ROOT.rstrip('/')}/org_{origin_id}" # type: ignore

        return processed_dir

    @staticmethod
    async def get_upload_sts_token(file_path: str) -> OssStsToken | None:
        """
        获取OSS上传STS Token(任务队列异步执行)

        :param file_path: 文件路径（如"folder/file.zip")
        :return: OSS上传STS Token模型
        """
        res: AsyncResult = get_oss_upload_sts_token_task.delay(file_path) # type: ignore
        result: dict = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if result is None:
            logger.error(f"failed to get oss upload sts token due to server timeout: {file_path}")
            return None
        oss_sts_token = OssStsToken(**result)
        return oss_sts_token

    @staticmethod
    async def get_packup_sts_token(packup_folder: str, zip_file_path: str) -> OssStsToken | None:
        """
        获取OSS打包STS Token(任务队列异步执行)

        :param packup_folder: 要打包的oss文件夹路径（如"experiment/aaa/")
        :param zip_file_path: 打包后的zip文件路径（如"data/aaa.zip")
        :return: OSS打包STS Token模型
        """
        res: AsyncResult = get_oss_packup_sts_token_task.delay(packup_folder, zip_file_path) # type: ignore
        result: dict = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if result is None:
            logger.error(f"failed to get oss packup sts token due to server timeout: from {packup_folder} to {zip_file_path}")
            return None
        oss_sts_token = OssStsToken(**result)
        return oss_sts_token

    @staticmethod
    async def validate_file(file_path: str, size: int, file_verify_code: str) -> bool:
        """
        检查OSS文件是否存在且大小匹配

        :param file_path: OSS文件路径（如"folder/file.zip")
        :param size: 文件大小（字节）

        :return: 是否存在且大小匹配，否则返回False
        """
        res: AsyncResult = validate_oss_file_task.delay(file_path, size, file_verify_code) # type:ignore
        is_file_exist: bool = await wait_for_result(res, poll_interval=0.1, timeout=OSS_DEFAULT_TIMEOUT_SECONDS) # type: ignore # 此处需等待OSS端fc函数自动计算完hash
        if is_file_exist is None:
            logger.error(f"failed to validate oss file due to server timeout: {file_path}")
            return False
        return is_file_exist

    @staticmethod
    async def check_file_exist(file_path: str) -> bool:
        """
        检查OSS文件是否存在

        :param file_path: OSS文件路径（如"folder/file.zip")
        :return: 是否存在，否则返回False
        """
        res: AsyncResult = check_oss_file_exist_task.delay(file_path) # type:ignore
        is_file_exist: bool = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if is_file_exist is None:
            logger.warning(f"failed to check oss file existence due to server timeout: {file_path}")
            return False
        return is_file_exist

    @staticmethod
    async def get_file_size(file_path: str) -> int:
        """
        获取OSS文件大小

        :param file_path: OSS文件路径（如"folder/file.zip")
        :return: 文件大小，单位字节
        """
        res: AsyncResult = get_oss_file_size_task.delay(file_path) # type: ignore
        size: int = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if size is None:
            logger.error(f"failed to get oss file size due to server timeout: {file_path}")
            return 0
        return size


    @staticmethod
    async def delete_file(file_path: str) -> bool | None:
        """
        删除OSS文件

        :param file_path: OSS文件路径（如"folder/file.zip")
        """
        if not file_path:
            logger.warning("empty path for deletion, skipping")
            return None

        # 1. 初始化任务列表（固定删除原文件）
        task_list = [delete_oss_file_task.si(file_path)] # type: ignore

        # 2. 判断是否需要添加【临时打包文件】的删除任务
        if not file_path.endswith("/"):
            potential_packup_path = f"{OSS_PACKUP_ZIP_STORAGE_ROOT.rstrip('/')}/{Path(file_path).name}" # type: ignore
            # 检查打包文件是否存在
            is_packup_file_exist = await OssStorage.check_file_exist(potential_packup_path)
            if is_packup_file_exist:
                task_list.append(delete_oss_file_task.si(potential_packup_path)) # type: ignore

        # 3. 并发执行删除任务
        delete_tasks: GroupResult = group(task_list).delay() # type: ignore

        # 4. 异步等待结果（支持GroupResult，无阻塞）
        delete_results: Sequence[bool] | None = await wait_for_result(
            result=delete_tasks,
            poll_interval=0.1,
            timeout=10
        )

        # 5. 结果处理
        if delete_results is None:
            logger.error(f"failed to delete oss file due to server timeout: {file_path}")
            return False

        # 所有删除任务都成功才返回True，任意一个失败返回False
        all_success = all(delete_results)
        return all_success


    @staticmethod
    async def tag_oss_file(file_path: str, tag: dict[str, str]) -> bool:
        """
        给OSS文件打标签

        :param file_path: OSS文件路径（如"folder/file.zip")
        :param tag: 标签字典（如{"key1": "value1", "key2": "value2"})

        :return: 是否成功，否则返回False
        """
        if not file_path:
            logger.error("empty path for tagging")
            return False
        res: AsyncResult = tag_oss_file_task.delay(file_path, tag) # type: ignore
        is_tagged: bool = await wait_for_result(res, poll_interval=0.1, timeout=10) # type: ignore
        if is_tagged is None:
            logger.error(f"failed to tag oss file due to server timeout: {file_path}")
            return False
        return is_tagged
