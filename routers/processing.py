"""
算法处理运行管理路由
"""
from typing import Annotated

from fastapi import APIRouter, Depends

from api.processing_api import (
    create_algorithm_task,
    list_public_process_runs,
    list_my_process_runs,
    delete_process_run_endpoint,
    get_zarr_folder_access,
)
from models.processing_models import ExperimentProcessRunPublic
from models.file_models import ZarrFolderAccess

processing_router = APIRouter(prefix="/processes", tags=["process manager"])

# 依赖注入类型别名
CreateProcessDep = Annotated[ExperimentProcessRunPublic, Depends(create_algorithm_task)]
ListPublicDep = Annotated[list[ExperimentProcessRunPublic], Depends(list_public_process_runs)]
ListMyDep = Annotated[list[ExperimentProcessRunPublic], Depends(list_my_process_runs)]
DeleteProcessDep = Annotated[dict, Depends(delete_process_run_endpoint)]
ZarrFolderDep = Annotated[ZarrFolderAccess, Depends(get_zarr_folder_access)]


@processing_router.post("", response_model=ExperimentProcessRunPublic)
async def create_process(record: CreateProcessDep):
    """创建算法处理任务"""
    return record


@processing_router.get("/public/{file_id}", response_model=list[ExperimentProcessRunPublic])
async def list_public(result: ListPublicDep):
    """根据文件ID查询公开的运行记录"""
    return result


@processing_router.get("/mine", response_model=list[ExperimentProcessRunPublic])
async def list_mine(result: ListMyDep):
    """查询当前用户的运行记录"""
    return result


@processing_router.get("/{run_id}/zarr", response_model=ZarrFolderAccess)
async def get_zarr_folder(result: ZarrFolderDep):
    """获取 zarr 文件夹访问凭证（OSS STS Token）"""
    return result


@processing_router.delete("/{run_id}")
async def delete_process(result: DeleteProcessDep):
    """根据 run_id 删除算法处理运行记录"""
    return result
