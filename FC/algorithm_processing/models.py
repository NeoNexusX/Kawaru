# -*- coding: utf-8 -*-
"""算法参数模型，与主服务 models/processing_models.py 保持一致。"""

from typing import Any

from pydantic import BaseModel, Field


class PeakPickParams(BaseModel):
    method: str = Field(default="diff")
    snr: float = Field(default=2.0, gt=0)
    return_type: str = Field(default="height")
    width: int = Field(default=5, ge=1)
    backend: str = Field(default="python")


class NormalizationParams(BaseModel):
    method: str = Field(default="tic_numba")
    scale: float | None = None
    ref: float | None = None
    ref_tolerance: float = Field(default=0.1, ge=0)


class PeakAlignParams(BaseModel):
    reference: Any = None  # np.ndarray | None, 默认 None 时自动计算
    tolerance: float | None = None
    units: str = Field(default="ppm")
    binfun: str = Field(default="min")
    binratio: float = Field(default=2.0, gt=0)
    min_count: int | None = None
    min_frequency: float | None = Field(default=0.01)


class NoiseReductionParams(BaseModel):
    method: str = Field(default="gaussian_numba")
    window: int = Field(default=5, gt=0)
    sd: float | None = None
    polyorder: int = Field(default=3, ge=0)
    deriv: int = Field(default=0, ge=0)
    delta: float = Field(default=1.0, gt=0)


class BaselineCorrectionParams(BaseModel):
    method: str = Field(default="locmin_numba")


class ProcessingAlgorithmsConfig(BaseModel):
    peak_pick: PeakPickParams | None = None
    normalization: NormalizationParams | None = None
    peak_align: PeakAlignParams | None = None
    noise_reduction: NoiseReductionParams | None = None
    baseline_correction: BaselineCorrectionParams | None = None
