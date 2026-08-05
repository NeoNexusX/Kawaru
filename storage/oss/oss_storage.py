from pathlib import Path
from typing import Sequence

from background.file_background_helper import (
    check_oss_file_exist_task,
    delete_oss_file_task,
    get_oss_file_size_task,
    get_oss_presigned_url_task,
    get_oss_upload_sts_token_task,
    get_zarr_read_sts_token_task,
    sign_url_task,
    tag_oss_file_task,
)
from celery import group
from celery.result import AsyncResult, GroupResult

from celery_runtime.result import wait_for_result
from constants.file_constants import (
    OSS_DEFAULT_TIMEOUT_SECONDS,
    OSS_PACKUP_ZIP_STORAGE_ROOT,
    OSS_ZIP_STORAGE_ROOT,
)
from exceptions.oss_exceptions import oss_timeout_exception
from models.auth import OssStsToken
from tools.logger import get_logger

logger = get_logger("storage")


class OssStorage:
    """OSS 存储相关操作。"""

    @staticmethod
    def prepare_storage_path(filename: str, file_type: str) -> str:
        """为 OSS 存储准备最终保存路径。"""
        return f"{OSS_ZIP_STORAGE_ROOT.rstrip('/')}/{filename}.{file_type}"  # type: ignore

    @staticmethod
    async def get_upload_sts_token(file_path: str) -> OssStsToken | None:
        """获取 OSS 上传 STS Token。"""
        res: AsyncResult = get_oss_upload_sts_token_task.delay(file_path)  # type: ignore
        result: dict = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if result is None:
            logger.error(
                "failed to get oss upload sts token due to server timeout: %s",
                file_path,
            )
            return None
        return OssStsToken(**result)

    @staticmethod
    async def check_file_exist(file_path: str) -> bool:
        """检查 OSS 文件或文件夹是否存在。"""
        res: AsyncResult = check_oss_file_exist_task.delay(file_path)  # type: ignore
        is_file_exist: bool = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if is_file_exist is None:
            logger.warning(
                "failed to check oss file existence due to server timeout: %s",
                file_path,
            )
            return False
        return is_file_exist

    @staticmethod
    async def get_file_size(file_path: str) -> int:
        """获取 OSS 文件大小。"""
        res: AsyncResult = get_oss_file_size_task.delay(file_path)  # type: ignore
        size: int = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if size is None:
            logger.error(
                "failed to get oss file size due to server timeout: %s",
                file_path,
            )
            return 0
        return size

    @staticmethod
    async def delete_file_and_packup(file_path: str) -> bool | None:
        """删除 OSS 文件及对应的临时打包文件。

        """
        if not file_path:
            logger.warning("empty path for deletion, skipping")
            return None

        task_list = [delete_oss_file_task.si(file_path)]  # type: ignore

        if not file_path.endswith("/"):
            potential_packup_path = (
                f"{OSS_PACKUP_ZIP_STORAGE_ROOT.rstrip('/')}/{Path(file_path).name}"  # type: ignore
            )
            if await OssStorage.check_file_exist(potential_packup_path):
                task_list.append(delete_oss_file_task.si(potential_packup_path))  # type: ignore

        delete_tasks: GroupResult = group(task_list).delay()  # type: ignore
        delete_results: Sequence[bool] | None = await wait_for_result(
            result=delete_tasks,
            poll_interval=1,
            timeout=10,
        )

        if delete_results is None:
            logger.error(f"failed to delete oss file due to server timeout: {file_path}")
            raise oss_timeout_exception

        return all(delete_results)

    @staticmethod
    async def delete_file(file_path: str) -> bool | None:
        """
        删除单个指定的 OSS 文件。
        """
        if not file_path:
            logger.warning("empty path for deletion, skipping")
            return None

        res: AsyncResult = delete_oss_file_task.delay(file_path)  # type: ignore
        delete_result: bool | None = await wait_for_result(
            res,
            poll_interval=1,
            timeout=10,
        )  # type: ignore

        if delete_result is None:
            logger.error(f"failed to delete oss file due to server timeout: {file_path}")
            raise oss_timeout_exception

        return delete_result

    @staticmethod
    async def tag_oss_file(file_path: str, tag: dict[str, str]) -> bool:
        """给 OSS 文件打标签。"""
        res: AsyncResult = tag_oss_file_task.delay(file_path, tag)  # type: ignore
        is_tagged: bool = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if is_tagged is None:
            logger.error(
                "failed to tag oss file due to server timeout: %s",
                file_path,
            )
            return False
        return is_tagged

    @staticmethod
    async def sign_download_url(file_path: str, expires: int) -> str | None:
        """签名 OSS 文件下载 URL。"""
        res: AsyncResult = sign_url_task.delay(file_path, expires)  # type: ignore
        sign_url: str | None = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if sign_url is None:
            logger.error(
                "failed to sign oss file due to server timeout: %s",
                file_path,
            )
            return None
        return sign_url

    @staticmethod
    async def get_presigned_download_url(
        file_path: str,
        expires: int = 300,
    ) -> str | None:
        """获取 OSS 文件预签名下载 URL。"""
        res: AsyncResult = get_oss_presigned_url_task.delay(  # type: ignore
            file_path,
            expires,
        )
        url: str | None = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=OSS_DEFAULT_TIMEOUT_SECONDS,
        )  # type: ignore
        if url is None:
            logger.error(
                "failed to get presigned url due to server timeout: %s",
                file_path,
            )
            return None
        return url

    @staticmethod
    async def get_zarr_read_sts_token(
        folder_path: str,
        expires: int = 3600,
    ) -> OssStsToken | None:
        """获取 zarr 文件夹只读 STS Token。"""
        res: AsyncResult = get_zarr_read_sts_token_task.delay(  # type: ignore
            folder_path,
            expires,
        )
        result: dict = await wait_for_result(
            res,
            poll_interval=0.1,
            timeout=10,
        )  # type: ignore
        if result is None:
            logger.error(
                "failed to get zarr read sts token due to server timeout: %s",
                folder_path,
            )
            return None
        return OssStsToken(**result)
