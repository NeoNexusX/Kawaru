"""
实验管理路由
"""
from fastapi import APIRouter

from models.experiment_models import ExperimentImzMLPublic
from api.experiment_api import GetExperimentByFileIdDep

experiment_router = APIRouter(prefix="/experiments", tags=["experiment manager"])

#########################
# ImzML 实验查询 API
#########################

@experiment_router.get("/imzml/{file_id}", response_model=ExperimentImzMLPublic)
async def get_experiment_by_file_id_endpoint(experiment: GetExperimentByFileIdDep):
    """根据源文件ID查询实验记录"""
    return experiment
