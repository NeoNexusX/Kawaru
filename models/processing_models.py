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

class ExperimentProcessRunBase(SQLModel):
    """算法处理运行 — 公共字段（不含文件路径，API 返回用）"""
    params_json: str | None = Field(default=None, max_length=4096, description="算法参数 JSON 字符串")
    status: str = Field(default=ProcessingStatus.PROCESSING, max_length=32, description="运行状态")
    error_message: str | None = Field(default=None, max_length=1024, description="错误信息")
    started_at: datetime | None = Field(default=None, description="开始执行时间")
    finished_at: datetime | None = Field(default=None, description="结束时间")


class ExperimentProcessRunInDB(ExperimentProcessRunBase, table=True):
    """算法处理运行 — 数据库表"""
    __tablename__ = "experiment_process_run"  # type:ignore

    id: int | None = Field(default=None, primary_key=True, sa_column_kwargs={"autoincrement": True})
    # 一对多外键：级联删除（参考 PartInDB → FileInDB 模式）
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

    # 输出路径和大小仅 DB 层存储，不对外暴露
    out_path: str | None = Field(default=None, max_length=getattr(storage_config, 'max_path_length', 255), description="结果输出路径")
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

    使用 file_id 而非 experiment_id 的原因：
    - 前端上传文件后已持有 file_id，无需额外查询 experiment
    - fileindb.file_id → origin_imzml.source_file_id 是 1:1（UNIQUE），
      后端通过 file_id 即可唯一确定 experiment
    """
    file_id: int
    algorithms: ProcessingAlgorithmsConfig


class ExperimentProcessRunPublic(ExperimentProcessRunBase):
    """API 返回模型"""
    id: int
    experiment_id: int
    source_file_id: int
    user_id: int
    created_at: datetime
    filename: str = ""
