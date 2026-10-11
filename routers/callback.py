from fastapi import APIRouter, Depends

from api.callback_api import (GetExpectHashDep, GetConfigDep,
                              OssUploadCallbackDep, AlgorithmCallbackDep,
                              TicCallbackDep, UmapCallbackDep,
                              ClusteringCallbackDep,
                              verify_fc_callback_token)

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

    回调失败时记录会标记为 FAILED；路径已落库的对象由失败实验定时任务
    尝试删除，未落库且未持久化的对象由 OSS 生命周期规则回收。
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
