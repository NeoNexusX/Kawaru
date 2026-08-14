from fastapi import APIRouter, Depends
from fastapi import status

from api.callback_api import (GetExpectHashDep, GetConfigDep,
                              OssUploadCallbackDep, AlgorithmCallbackDep,
                              TicCallbackDep, UmapCallbackDep,
                              ClusteringCallbackDep,
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
    """
    OSS 上传完成回调。

    回调到达但处理失败时会尽力删除本次 OSS 对象；FC 未回调产生的未标记
    对象由 OSS 生命周期规则自动清理。
    """
    return file_record


@callback_router.post("/algorithm_callback", response_model=AlgorithmCallbackDep)
async def algorithm_callback(run_record: AlgorithmCallbackDep):
    """FC算法处理完成回调"""
    return run_record


@callback_router.post("/tic_callback", response_model=TicCallbackDep)
async def tic_callback(file_record: TicCallbackDep):
    """FC TIC图像生成回调"""
    return file_record


@callback_router.post("/umap_callback", response_model=UmapCallbackDep)
async def umap_callback(file_record: UmapCallbackDep):
    """FC UMAP图生成回调"""
    return file_record


@callback_router.post("/clustering_callback", response_model=ClusteringCallbackDep)
async def clustering_callback(run_record: ClusteringCallbackDep):
    """FC 聚类分析完成回调"""
    return run_record


@callback_router.post("/oss_packup_callback", status_code=status.HTTP_204_NO_CONTENT)
async def oss_packup_callback(packup_status: OssPackupCallbackDep): # pylint: disable=unused-argument
    """OSS打包完成回调"""
    return
