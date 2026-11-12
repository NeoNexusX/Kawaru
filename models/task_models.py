"""计算任务载荷模型：API 端点构造并校验（唯一一次校验，入队即模型 dump）。

每个 EHPC 任务的参数形状在这里定义一次；提交器签名与容器 CLI 参数仍由
tools/ehpc_client 维护，二者字段必须保持一致。
"""
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _TaskParams(BaseModel):
    """任务载荷基类：未知字段一律报错（拼错参数在构造时立刻暴露）。"""
    model_config = ConfigDict(extra="forbid")

    # 任务键与标识字段：调度层据此选后端、取日志/告警标识；不属于载荷，不参与序列化
    task: ClassVar[str]
    id_field: ClassVar[str]

    input_bytes: int | None = Field(
        default=None, ge=0,
        description="输入总字节数；EHPC 按它选资源档位，FC 事件体不透出",
    )


class _MetadataParams(_TaskParams):
    """带 metadata 的任务：None 归一化为 {}，两种后端拿到的形状一致。"""
    metadata: dict = Field(default_factory=dict, description="镜像写入 zarr 的元数据")

    @field_validator("metadata", mode="before")
    @classmethod
    def _none_to_empty(cls, v):
        return v or {}


class AlgorithmParams(_MetadataParams):
    """算法管线处理（任务键 algorithm）。"""
    task: ClassVar[str] = "algorithm"
    id_field: ClassVar[str] = "run_id"

    run_id: int
    imzml_path: str
    config_json: str
    output_path: str


class RawConvertParams(_MetadataParams):
    """imzML 直转 Zarr（任务键 raw_convert）。"""
    task: ClassVar[str] = "raw_convert"
    id_field: ClassVar[str] = "run_id"

    run_id: int
    imzml_path: str
    ibd_path: str
    output_path: str


class ClusteringParams(_TaskParams):
    """UMAP/KMeans 聚类，结果写回源 zarr（任务键 clustering）。"""
    task: ClassVar[str] = "clustering"
    id_field: ClassVar[str] = "run_id"

    run_id: int
    source_zarr_path: str


PreviewMode = Literal["feature", "tic"]

class PreviewParams(_TaskParams):
    """上传后预览图（任务键 preview）：feature=UMAP/TIC/离子图，tic=仅 TIC。"""
    task: ClassVar[str] = "preview"
    id_field: ClassVar[str] = "file_id"

    file_id: int
    imzml_path: str
    image_dest: str
    mode: PreviewMode
    ibd_path: str | None = None


# API 层 dispatch 接受的载荷联合类型
ComputeTaskParams = AlgorithmParams | RawConvertParams | ClusteringParams | PreviewParams
