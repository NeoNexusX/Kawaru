"""
删除流程状态位的唯一出口：状态判定 + 读侧可见性谓词

删除状态位（DELETING / DELETE_FAILED）的判定原先散在十余处各写各的元组字面量，
出现了一处有意 opt-out、一处漏检——因为规则写在注释里而不是代码里。本模块把它收敛成
唯一出口：对象判定用 file_is_deleting / run_is_deleting，SQL 过滤用 visible_*_clause。

读侧语义：删除流程中的行一律按不存在处理（详情 404、列表隐藏），
不泄露「删除中」状态。
"""
from __future__ import annotations

from typing import Any

from sqlmodel import col

from models.file_models import FileInDB, FileStatus
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus

# 删除流程中的状态位：OSS 清理进行中（DELETING）/ 清理失败保留行等重投（DELETE_FAILED）
DELETING_FILE_STATUSES: tuple[FileStatus, ...] = (
    FileStatus.DELETING,
    FileStatus.DELETE_FAILED,
)
DELETING_RUN_STATUSES: tuple[ProcessingStatus, ...] = (
    ProcessingStatus.DELETING,
    ProcessingStatus.DELETE_FAILED,
)


def file_is_deleting(status: str | None) -> bool:
    """文件是否处于删除流程（读侧按不存在处理，登记/重投路径显式放行）"""
    return status in DELETING_FILE_STATUSES


def run_is_deleting(status: str | None) -> bool:
    """run 是否处于删除流程（读侧按不存在处理，回调据此丢弃迟到写入）"""
    return status in DELETING_RUN_STATUSES


def visible_files_clause() -> Any:
    """文件读侧可见性谓词：排除删除流程中的文件"""
    return col(FileInDB.status).notin_(DELETING_FILE_STATUSES)


def visible_runs_clause() -> Any:
    """run 读侧可见性谓词：排除删除流程中的 run"""
    return col(ExperimentProcessRunInDB.status).notin_(DELETING_RUN_STATUSES)
