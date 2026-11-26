"""处理次数配额的纯函数单测：边界判断与 run 计费谓词。"""
from types import SimpleNamespace

from api_helpers.processing_helper import check_processing_quota, run_counts_toward_quota
from models.processing_models import RAW_CONVERT_MARKER


def _user(used: int, limit: int) -> SimpleNamespace:
    return SimpleNamespace(id=1, processing_count_used=used, max_processing_count=limit)


def test_check_processing_quota_boundaries() -> None:
    assert check_processing_quota(_user(0, 50)) is True
    assert check_processing_quota(_user(49, 50)) is True
    assert check_processing_quota(_user(50, 50)) is False
    assert check_processing_quota(_user(51, 50)) is False
    # 0 表示禁止处理（与旧口径一致）
    assert check_processing_quota(_user(0, 0)) is False


def test_run_counts_toward_quota() -> None:
    # 非直转且曾成功完成（output_size > 0）
    assert run_counts_toward_quota(None, 4096) is True
    assert run_counts_toward_quota('{"peak_pick":{}}', 1) is True
    # 直转不占处理次数，即使有 output_size
    assert run_counts_toward_quota(RAW_CONVERT_MARKER, 4096) is False
    # 失败 / 处理中（无 output_size）
    assert run_counts_toward_quota(None, None) is False
    assert run_counts_toward_quota(None, 0) is False
