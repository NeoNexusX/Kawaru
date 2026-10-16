"""
ORCID 校验时间预算不变式单测（调用方预算 vs 任务内 HTTP 超时）

背景：调用方 user_service.validate_orcid 用 wait_for_result 等任务结果；任务内 httpx 的
HTTP 超时若 >= 调用方预算，20-30s 的慢响应会被调用方先放弃，合法 ORCID 被误判成
「ORCID 不存在」（曾为 调用方 20s / 任务 30s）。两个数字都在 constants/user_constants.py。

钉住三件事：
  ① 预算关系：任务 HTTP 超时 < 调用方预算（= HTTP 超时 + 入队/取任务余量）
  ② validate_orcid 确实把该预算传给 wait_for_result
  ③ 任务确实用 ORCID_HTTP_TIMEOUT_SECONDS 建 httpx 客户端

运行方式: uv run pytest tests/test_orcid_timeout_unit.py -v
"""
from __future__ import annotations

from types import SimpleNamespace

import api_services.user_service as user_service
import background.api_background as api_background
from constants.user_constants import (
    ORCID_HTTP_TIMEOUT_SECONDS,
    ORCID_QUEUE_SLACK_SECONDS,
    ORCID_VALIDATE_TIMEOUT_SECONDS,
)

ORCID_SAMPLE = "0000-0002-1825-0097"


class TestBudgetInvariant:
    """① 预算关系"""

    def test_caller_budget_exceeds_task_http_timeout(self):
        assert ORCID_HTTP_TIMEOUT_SECONDS < ORCID_VALIDATE_TIMEOUT_SECONDS
        assert ORCID_VALIDATE_TIMEOUT_SECONDS == ORCID_HTTP_TIMEOUT_SECONDS + ORCID_QUEUE_SLACK_SECONDS


class TestCallerSide:
    """② 调用方把预算显式传给 wait_for_result"""

    async def test_validate_orcid_passes_budget(self, monkeypatch):
        captured: dict = {}

        async def _fake_wait(result, poll_interval, timeout=None):
            captured["poll_interval"] = poll_interval
            captured["timeout"] = timeout
            return True

        class _TaskStub:
            def delay(self, orcid: str):
                captured["orcid"] = orcid
                return SimpleNamespace(id="stub-task-id")

        monkeypatch.setattr(user_service, "wait_for_result", _fake_wait)
        monkeypatch.setattr(user_service, "validate_orcid_task", _TaskStub())
        assert await user_service.validate_orcid(ORCID_SAMPLE) is True
        assert captured["orcid"] == ORCID_SAMPLE
        assert captured["timeout"] == ORCID_VALIDATE_TIMEOUT_SECONDS


class TestTaskSide:
    """③ 任务用常量建 httpx 客户端（不是魔法数字）"""

    async def test_task_uses_http_timeout_constant(self, monkeypatch):
        captured: dict = {}

        class _Response:
            status_code = 200

        class _ClientStub:
            def __init__(self, timeout=None, **kwargs):
                captured["timeout"] = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def head(self, url, headers=None):
                captured["url"] = url
                return _Response()

        monkeypatch.setattr(api_background, "httpx", SimpleNamespace(AsyncClient=_ClientStub))
        assert await api_background.validate_orcid_task.run(ORCID_SAMPLE) is True
        assert captured["url"] == f"https://pub.orcid.org/v3.0/{ORCID_SAMPLE}/person"
        assert captured["timeout"] == ORCID_HTTP_TIMEOUT_SECONDS
