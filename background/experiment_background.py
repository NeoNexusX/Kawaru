
from pathlib import Path

from fastapi import BackgroundTasks

from background.file_background import process_imzml_experiment_background
from models.file_models import FileInDB
from models.user_models import UserInDB

from tools.logger import get_logger
logger = get_logger("background.experiment_background")

def trigger_experiment_extraction(
    background_tasks: BackgroundTasks,
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
    background_tasks.add_task(
        process_imzml_experiment_background,
        current_user.id,
        file_record.file_id,
        experiment_name,
    )
    logger.info(f"auto extraction triggered user={current_user.id} file_id={file_record.file_id}")
