from pydantic import BaseModel

class OssUploadCallback(BaseModel):
    """OSS上传完成回调模型

    FC 解压函数从 OSS 对象键（data/{file_id}/{file_verify_code}.zip）解析出
    file_id 后回调，后端按 file_id 定位文件记录。
    """
    file_id: int
    # FC 超大文件跳过等失败场景下无文件路径，允许 None，否则 422 导致失败通知丢失
    file_path: str | None = None
    is_success: bool
    act_hash: str | None = None
    imzml_path: str | None = None
    ibd_path: str | None = None
    imzml_size: int | None = None
    ibd_size: int | None = None
    error_type: str | None = None


class AlgorithmCallback(BaseModel):
    """FC算法处理完成回调模型"""
    run_id: int
    is_success: bool
    out_path: str | None = None
    output_size: int | None = None
    error_type: str | None = None
    error_message: str | None = None


class TicCallback(BaseModel):
    """FC TIC 图像生成完成回调模型"""
    file_id: int
    is_success: bool
    image_path: str | None = None
    error_type: str | None = None
    error_message: str | None = None


class UmapCallback(BaseModel):
    """FC UMAP 图生成完成回调模型"""
    file_id: int
    is_success: bool
    image_path: str | None = None
    error_type: str | None = None
    error_message: str | None = None


class ClusteringCallback(BaseModel):
    """FC 聚类分析完成回调模型（聚类结果写回源 zarr，回调只携带状态）"""
    run_id: int
    is_success: bool
    error_type: str | None = None
    error_message: str | None = None
