"""
Originexperiment 管理路由
"""
from fastapi import APIRouter

from models.experiment_models import OriginImzMLPublic
from api.experiment_api import GetOriginByPublicIdDep

originexperiment_router = APIRouter(prefix="/origins", tags=["origin manager"])

#########################
# ImzML origin 查询 API
#########################

@originexperiment_router.get("/imzml/{public_id}", response_model=OriginImzMLPublic)
async def get_origin_by_public_id_endpoint(origin: GetOriginByPublicIdDep):
    """根据源文件 public_id 查询 origin 记录"""
    return origin
