#########################
# File Background Helper - 后台任务辅助函数
# 职责：低级后台任务函数，避免循环导入
#########################
from __future__ import annotations

import asyncio
import zipfile
from pathlib import Path

from background.celery_app import celery_app
from tools.logger import get_logger

logger = get_logger("background.file_background_helper")


@celery_app.task()  # type: ignore
async def build_zip_file_background(
    files: list[tuple[Path, str]],
    output_path: Path,
    compression: int | None = None,
) -> str:
    """
    后台创建ZIP文件任务
    """
    if compression is None:
        compression = zipfile.ZIP_DEFLATED

    output = output_path
    output.parent.mkdir(parents=True, exist_ok=True)

    # 转换参数为字符串
    files_str = [(str(fp), arc) for fp, arc in files]
    output_str = str(output_path)

    def _create_zip() -> None:
        with zipfile.ZipFile(output_str, "w", compression) as zf:
            for file_path_str, arc_name in files_str:
                file_path = Path(file_path_str)
                if not file_path.exists():
                    raise FileNotFoundError(f"Source file not found: {file_path}")
                zf.write(file_path, arc_name)

    await asyncio.to_thread(_create_zip)
    logger.info(f"ZIP file created at {output_str}")
    return output_str
