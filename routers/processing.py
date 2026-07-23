"""
算法处理运行管理路由
"""
import asyncio

from fastapi import APIRouter

from api.processing_api import (
    CreateAlgorithmTaskDep,
    CreateRawConvertTaskDep,
    CreateClusteringTaskDep,
    DeleteProcessRunDep,
    ListMyRunsDep,
    ZarrFolderDep,
)
from models.processing_models import ExperimentProcessRunPublic
from models.file_models import ZarrFolderAccess
from models.filter_models import ListDataResponse
from database.visit_stats import incr_visit

processing_router = APIRouter(prefix="/processes", tags=["process manager"])


@processing_router.post("", response_model=ExperimentProcessRunPublic)
async def create_process(record: CreateAlgorithmTaskDep):
    """创建算法处理任务"""
    asyncio.create_task(incr_visit())
    return record


@processing_router.post("/raw-convert", response_model=ExperimentProcessRunPublic)
async def create_raw_convert(record: CreateRawConvertTaskDep):
    """创建 imzML 直转 Zarr 任务（无算法处理）"""
    asyncio.create_task(incr_visit())
    return record


@processing_router.post("/{run_id}/clustering", response_model=ExperimentProcessRunPublic)
async def create_clustering(record: CreateClusteringTaskDep):
    """创建聚类分析任务（对已完成算法处理的 run 执行 UMAP/KMeans）"""
    asyncio.create_task(incr_visit())
    return record


@processing_router.get("/mine", response_model=ListDataResponse)
async def list_mine(result: ListMyRunsDep):
    """分页查询当前用户的运行记录"""
    return result


@processing_router.get("/{run_id}/zarr", response_model=ZarrFolderAccess)
async def get_zarr_folder(result: ZarrFolderDep):
    """获取 zarr 文件夹访问凭证（OSS STS Token）"""
    return result


@processing_router.delete("/{run_id}")
async def delete_process(result: DeleteProcessRunDep):
    """根据 run_id 删除算法处理运行记录"""
    return result
