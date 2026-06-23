from fastapi import APIRouter, Depends
from fastapi import status

from api.callback_api import (GetExpectHashDep, GetConfigDep,
                              OssUploadCallbackDep, AlgorithmCallbackDep,
                              TicCallbackDep, FeatureCallbackDep,
                              OssPackupCallbackDep, verify_fc_callback_token)

callback_router = APIRouter(
    prefix="/callback",
    tags=["callback"],
    dependencies=[Depends(verify_fc_callback_token)],  # 所有回调路由都需要 Token 验证
)

#############
# 回调 API
# 所有api都需要签名验证
#############

@callback_router.get("/get_expected_hash")
async def get_expected_hash(expected_hash: GetExpectHashDep):
    """获取文件的理论哈希"""
    return {"expected_hash": expected_hash}

@callback_router.get("/config", response_model=GetConfigDep)
async def get_config(config: GetConfigDep):
    """函数计算FC统一获取系统配置接口"""
    return config

@callback_router.post("/oss_upload_callback", response_model=OssUploadCallbackDep)
async def oss_upload_callback(file_record: OssUploadCallbackDep):
    """OSS上传完成回调"""
    return file_record


@callback_router.post("/algorithm_callback", response_model=AlgorithmCallbackDep)
async def algorithm_callback(run_record: AlgorithmCallbackDep):
    """FC算法处理完成回调"""
    return run_record


@callback_router.post("/tic_callback", response_model=TicCallbackDep)
async def tic_callback(file_record: TicCallbackDep):
    """FC TIC图像生成回调"""
    return file_record


@callback_router.post("/feature_callback", response_model=FeatureCallbackDep)
async def feature_callback(file_record: FeatureCallbackDep):
    """FC UMAP/KMeans 特征分析回调"""
    return file_record


@callback_router.post("/oss_packup_callback", status_code=status.HTTP_204_NO_CONTENT)
async def oss_packup_callback(packup_status: OssPackupCallbackDep): # pylint: disable=unused-argument
    """OSS打包完成回调"""
    return
