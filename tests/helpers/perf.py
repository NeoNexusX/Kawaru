"""
性能测试共用工具

- compute_stats: 延迟数据的统计聚合（min/avg/p50/p95/p99/max）
- dump_json: 保存原始数据为 JSON
- results_path: 在 tests/results/ 下生成时间戳路径
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any


# helpers/ 在 tests/ 下一级，结果统一落到 tests/results/
_RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"


def compute_stats(values: list[float]) -> dict[str, float]:
    """对延迟列表计算 min/avg/p50/p95/p99/max（单位与输入一致）"""
    if not values:
        return {}
    sorted_v = sorted(values)
    n = len(sorted_v)

    def _percentile(p: float) -> float:
        # 线性插值版分位数（与 numpy 默认 linear 一致）
        if n == 1:
            return sorted_v[0]
        idx = (n - 1) * p
        lo = math.floor(idx)
        hi = math.ceil(idx)
        if lo == hi:
            return sorted_v[lo]
        return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (idx - lo)

    return {
        "count": n,
        "min_ms": sorted_v[0],
        "avg_ms": sum(sorted_v) / n,
        "p50_ms": _percentile(0.50),
        "p95_ms": _percentile(0.95),
        "p99_ms": _percentile(0.99),
        "max_ms": sorted_v[-1],
    }


def results_path(prefix: str) -> Path:
    """在 tests/results/ 下生成 <prefix>_<timestamp>.json 路径，自动建目录"""
    _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return _RESULTS_DIR / f"{prefix}_{ts}.json"


def dump_json(payload: Any, path: Path) -> None:
    """以 UTF-8 写出 JSON，indent=2，方便论文里复用"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
