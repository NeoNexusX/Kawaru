import os

from celery_runtime.state import get_celery_app
from constants.file_constants import OSS_BUCKET_NAME, FC_PACKUP_ENDPOINT, FC_PACKUP_TIMEOUT_SECONDS
from tools.fc_client import invoke_fc
from tools.logger import get_logger


logger = get_logger("background.experiment_background_helper")
celery_app = get_celery_app()


@celery_app.task()
async def packup_oss_file_task(filename: str, experiment_folder: str, dest_zip_path: str) -> str | None:
    """
    异步触发打包 OSS 中的实验文件夹
    """
    logger.info(f"start to packup_oss_file, experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")

    event = {
        "bucket": OSS_BUCKET_NAME,
        "filename": filename,
        "source-dir": experiment_folder,
        "dest-file": dest_zip_path,
    }

    fc_token = os.getenv("OSS_FC_PACKUP_TOKEN")
    fc_url = FC_PACKUP_ENDPOINT
    fc_timeout = FC_PACKUP_TIMEOUT_SECONDS

    request_id = await invoke_fc(fc_url, fc_token, event, fc_timeout)
    if request_id is None:
        logger.error(f"packup_oss_file trigger failed, experiment_folder={experiment_folder}")
        return None

    logger.info(f"packup_oss_file task triggered, request_id={request_id},"
                f"experiment_folder={experiment_folder}, dest_zip_path={dest_zip_path}")
    return request_id
