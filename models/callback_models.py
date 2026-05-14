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
