'''
Author: xiongqiurong 1513401658@qq.com
Date: 2025-12-26 23:03:35
LastEditors: xiongqiurong 1513401658@qq.com
LastEditTime: 2025-12-27 14:24:55
FilePath: \Kawaru\routers\experiment.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
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
