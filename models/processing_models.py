"""
算法处理运行模型
"""
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlmodel import SQLModel, Column, Integer, ForeignKey, BigInteger, Field, Relationship

from tools import utc_time
from boot.config import get_config

if TYPE_CHECKING:
    from models.experiment_models import OriginImzMLInDB

config = get_config()
storage_config = config.storage


#########################
# 处理状态枚举
#########################

class ProcessingStatus(str, Enum):
    """算法处理运行状态"""
    PROCESSING = "processing"  # worker 正在执行
    COMPLETED = "completed"    # 全部算法执行成功
    FAILED = "failed"          # 执行失败
    # 删除流程状态位（OSS 先删、DB 后删、失败标记）
    DELETING = "deleting"            # 删除已登记：OSS 清理进行中，读侧拒绝、回调丢弃
    DELETE_FAILED = "delete_failed"  # OSS 清理失败：行保留作为任务记录，等兜底重投


#########################
# 算法参数子模型（Pydantic，API 层校验用）
#########################

class PeakPickParams(BaseModel):
    """峰检测参数"""
    method: str = Field(default="diff", description="检测方法: sd/mad/quantile/diff")
    snr: float = Field(default=2.0, gt=0, description="信噪比阈值，正数")
    return_type: str = Field(default="height", description="返回类型: height/area")
    width: int = Field(default=5, ge=1, description="窗口宽度，≥1")
    backend: str = Field(default="python", description="后端: python/cardinal")


class NormalizationParams(BaseModel):
    """归一化参数"""
    method: str = Field(default="tic_numba", description="归一化方法: tic_numba/rms_numba/ref_numba")
    scale: float | None = Field(default=None, description="缩放因子，一般为1")
    ref: float | None = Field(default=None, description="参考峰 m/z，ref_numba 用，None 时自动取值")
    ref_tolerance: float = Field(default=0.1, ge=0, description="参考峰容差，非负数")


class PeakAlignParams(BaseModel):
    """峰对齐参数"""
    reference: Any | None = Field(default=None, description="参考质谱 np.ndarray，None 时自动计算")
    tolerance: float | None = Field(default=None, description="m/z 容差，None 时按 binratio 自动估计")
    units: str = Field(default="ppm", description="容差单位: ppm/relative 或 Da/absolute")
    binfun: str = Field(default="min", description="聚合函数: min/median/max/mean")
    binratio: float = Field(default=2.0, gt=0, description="容差/分辨率比例，正数")
    min_count: int | None = Field(default=None, description="最小离子计数，python 后端用")
    min_frequency: float | None = Field(default=0.01, description="最小出现频率，python 后端用")


class NoiseReductionParams(BaseModel):
    """平滑降噪参数"""
    method: str = Field(default="gaussian_numba", description="降噪方法: savgol_numba/gaussian_numba/ma_numba")
    window: int = Field(default=5, gt=0, description="平滑窗口大小，正整数")
    sd: float | None = Field(default=None, description="高斯标准差，gaussian_numba 用")
    polyorder: int = Field(default=3, ge=0, description="多项式阶，savgol_numba 用")
    deriv: int = Field(default=0, ge=0, description="导数阶，0=平滑，savgol_numba 用")
    delta: float = Field(default=1.0, gt=0, description="采样间距，savgol_numba 用")


class BaselineCorrectionParams(BaseModel):
    """基线校正参数"""
    method: str = Field(default="locmin_numba", description="基线校正方法: locmin_numba/snip_numba")


#########################
# 算法配置聚合模型
#########################

class ProcessingAlgorithmsConfig(BaseModel):
    """算法配置聚合模型
    - 字段为 None 表示不执行该算法
    """
    peak_pick: PeakPickParams | None = None
    normalization: NormalizationParams | None = None
    peak_align: PeakAlignParams | None = None
    noise_reduction: NoiseReductionParams | None = None
    baseline_correction: BaselineCorrectionParams | None = None


#########################
# 算法处理运行数据库表
#########################

# raw_convert 类型标记：params_json 为此值时表示直转 run（无算法处理）
RAW_CONVERT_MARKER = "__RAW_CONVERT__"


class ExperimentProcessRunBase(SQLModel):
    """算法处理运行 — 公共字段（不含文件路径，API 返回用）"""
    params_json: str | None = Field(default=None, max_length=4096, description="算法参数 JSON 字符串")
    status: str = Field(default=ProcessingStatus.PROCESSING, max_length=32, description="运行状态")
    error_message: str | None = Field(default=None, max_length=1024, description="错误信息")
    finished_at: datetime | None = Field(default=None, description="结束时间")
    clustering_status: str | None = Field(default=None, max_length=32, description="聚类状态: processing/completed/failed，None 表示未执行")
    clustering_error_message: str | None = Field(default=None, max_length=1024, description="聚类错误信息(失败时)")
    clustering_started_at: datetime | None = Field(default=None, description="聚类开始时间，超时巡检判定僵尸聚类 run 的依据")


class ExperimentProcessRunInDB(ExperimentProcessRunBase, table=True):
    """算法处理运行 — 数据库表"""
    __tablename__ = "experiment_process_run"  # type:ignore

    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    # 一对多外键：数据库级联删除关联的处理记录
    experiment_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("origin_imzml.id", ondelete="CASCADE"),  # 数据库层兜底
            nullable=False,
            index=True
        ),
        description="关联 origin_imzml.id"
    )
    # 冗余外键，方便直接定位源文件做权限校验
    source_file_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("fileindb.file_id", ondelete="CASCADE"),  # 数据库层兜底
            nullable=False,
            index=True
        ),
        description="关联 fileindb.file_id"
    )
    # 冗余外键，方便 _complete_run 直接定位用户更新配额
    user_id: int = Field(
        sa_column=Column(
            Integer,
            ForeignKey("userindb.id", ondelete="CASCADE"),
            nullable=False,
            index=True
        ),
        description="关联 userindb.id"
    )

    created_at: datetime = Field(default_factory=utc_time, description="创建时间")
    # 删除登记时间：置 DELETING 时写入，兜底扫描据此判定「停留过久的 DELETING」
    delete_marked_at: datetime | None = Field(
        default=None,
        description="删除登记时间（DELETING/DELETE_FAILED 状态的置位时间）",
    )

    # 输出路径和大小仅 DB 层存储，不对外暴露
    # 聚类结果直接写回该 zarr 内部，不单独记录路径
    out_path: str | None = Field(
        default=None,
        max_length=getattr(storage_config, 'max_path_length', 255),
        description="结果输出路径",
    )
    output_size: int | None = Field(
        default=None,
        sa_column=Column(BigInteger, nullable=True),
        description="输出文件总大小(byte)"
    )

    # ORM 层反向关系（对应 OriginImzMLInDB.process_runs）
    experiment: "OriginImzMLInDB" = Relationship(
        back_populates="process_runs",
    )


#########################
# API 请求/返回模型
#########################

class ExperimentProcessRunCreate(SQLModel):
    """前端提交算法处理请求

    使用文件 public_id 而非 experiment_id 的原因：
    - 前端上传文件后已持有 public_id，无需额外查询 experiment
    - fileindb → origin_imzml.source_file_id 是 1:1（UNIQUE），
      后端按 public_id 查到文件后即可唯一确定 experiment
    """
    file_public_id: str
    algorithms: ProcessingAlgorithmsConfig


class ExperimentProcessRunRawConvert(SQLModel):
    """前端提交 raw_convert 直转请求（无算法处理，仅需文件 public_id）"""
    file_public_id: str


class ExperimentProcessRunPublic(ExperimentProcessRunBase):
    """API 返回模型

    源文件以 public_id 对外标识（source_file_public_id），不返回内部 source_file_id；
    该字段不落库，由查询时关联 fileindb.public_id 得到。
    """
    id: int
    experiment_id: int
    source_file_public_id: str
    user_id: int
    created_at: datetime
    filename: str = ""


class ExperimentProcessRunCallbackResponse(ExperimentProcessRunBase):
    """FC 回调（algorithm_callback / clustering_callback）的响应模型

    与前端返回模型 ExperimentProcessRunPublic 分开：回调是后端与 FC 之间的内部接口，
    FC 只判断 HTTP 状态码、不读取响应内容。回调直接返回数据库记录，没有前端专用的
    source_file_public_id / filename，若共用前端模型会因必填字段缺失导致响应校验失败（500）。
    """
    id: int
    experiment_id: int
    source_file_id: int
    user_id: int
    created_at: datetime


# 运行时导入（不能只放 TYPE_CHECKING）：experiment 反向 Relationship 以字符串引用
# 'OriginImzMLInDB'，mapper 配置要求该类已在本进程注册（与 experiment_models 末尾的
# 导入互为闭环，任意导入顺序都不会循环导入）。
from models.experiment_models import OriginImzMLInDB  # noqa: E402,F401
