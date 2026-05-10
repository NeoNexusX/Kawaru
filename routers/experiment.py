"""
Originexperiment 管理路由
"""
from fastapi import APIRouter

from models.experiment_models import OriginImzMLPublic
from api.experiment_api import GetOriginByFileIdDep

originexperiment_router = APIRouter(prefix="/origins", tags=["origin manager"])

#########################
# ImzML origin 查询 API
#########################

@originexperiment_router.get("/imzml/{file_id}", response_model=OriginImzMLPublic)
async def get_origin_by_file_id_endpoint(origin: GetOriginByFileIdDep):
    """根据源文件ID查询 origin 记录"""
    return origin
