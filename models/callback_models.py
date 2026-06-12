from pydantic import BaseModel

class OssUploadCallback(BaseModel):
    """OSS上传完成回调模型"""
    filename: str
    file_path: str
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


class FeatureCallback(BaseModel):
    """FC UMAP/KMeans 特征分析完成回调模型"""
    file_id: int
    is_success: bool
    image_path: str | None = None
    error_type: str | None = None
    error_message: str | None = None
