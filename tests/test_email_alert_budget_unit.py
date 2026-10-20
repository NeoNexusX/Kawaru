"""
管理员告警邮件额度闸单测（api_helpers/alert_budget.py + 共享发件任务的分类闸）

背景：告警与验证码共用同一 SMTP 账号（同一个 send_smtp_email_task），服务商按账号限流，
告警洪峰（FC 端点挂掉、OSS 批量删除失败）会把验证码邮件挤掉，所以告警侧加全局额度闸。

覆盖：
  ① 额度闸本身：窗口内超限返回 False；只在首次计数时设置 TTL；每次调用都关短连接
  ② fail-open：Redis 异常时放行（告警是旁路能力，不得因 Redis 抖动被误杀）
  ③ 任务侧：category=alert 超限时不经 SMTP 直接丢弃（返回 suppressed）
  ④ 验证码邮件（无 category 键）不过闸，照常发送
  ⑤ 投递侧：admin_notify 的 payload 必须带 category=alert

测试策略：不发真实邮件、不连 Redis——monkeypatch connect_new_redis / get_config / smtplib。
运行方式: uv run pytest tests/test_email_alert_budget_unit.py -v
"""
from __future__ import annotations

from types import SimpleNamespace

import api_helpers.admin_notify as admin_notify
import api_helpers.alert_budget as alert_budget
import background.api_background as api_background
from api_helpers.admin_notify import notify_fc_trigger_failed
from constants.user_constants import EMAIL_CATEGORY_ALERT


class _FakeRedis:
    """connect_new_redis 的桩：内存计数 + 记录 expire / aclose 调用。"""

    def __init__(self, raise_on_incr: Exception | None = None):
        self.counters: dict[str, int] = {}
        self.expire_calls: list[tuple[str, int]] = []
        self.closed = False
        self.raise_on_incr = raise_on_incr

    async def incr(self, key: str) -> int:
        if self.raise_on_incr is not None:
            raise self.raise_on_incr
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def expire(self, key: str, seconds: int) -> bool:
        self.expire_calls.append((key, seconds))
        return True

    async def aclose(self) -> None:
        self.closed = True


def _install_budget_fakes(
    monkeypatch,
    *,
    max_per_window: int = 2,
    window_seconds: int = 3600,
    fake: _FakeRedis | None = None,
) -> _FakeRedis:
    """把额度闸依赖的 config / Redis 客户端换成桩。"""
    fake = fake or _FakeRedis()
    config = SimpleNamespace(
        email=SimpleNamespace(
            alert_max_per_window=max_per_window,
            alert_budget_window_seconds=window_seconds,
        ),
        redis=SimpleNamespace(default_db_index=0),
    )
    monkeypatch.setattr(alert_budget, "get_config", lambda: config)
    monkeypatch.setattr(alert_budget, "connect_new_redis", lambda db_index: fake)
    return fake


class TestAlertBudgetGate:
    """① ② 额度闸本身"""

    async def test_allows_up_to_limit_then_suppresses(self, monkeypatch):
        fake = _install_budget_fakes(monkeypatch, max_per_window=2)
        assert await alert_budget.alert_budget_available() is True
        assert await alert_budget.alert_budget_available() is True
        assert await alert_budget.alert_budget_available() is False
        # 只在首次计数时设 TTL；key 带窗口序号，expire 漏掉也不会永久封死
        assert len(fake.expire_calls) == 1
        assert fake.expire_calls[0][1] == 3600
        assert set(fake.counters) == {fake.expire_calls[0][0]}
        # 短连接用完即关：worker 每个任务一个事件循环，连接不能跨 loop 复用
        assert fake.closed is True

    async def test_fail_open_on_redis_error(self, monkeypatch):
        _install_budget_fakes(monkeypatch, fake=_FakeRedis(raise_on_incr=RuntimeError("redis down")))
        assert await alert_budget.alert_budget_available() is True


class _SmtpStub:
    """smtplib.SMTP / SMTP_SSL 的桩：记录发出的邮件，不碰真实网络。"""

    sent: list = []

    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, username, password):
        return None

    def send_message(self, msg):
        type(self).sent.append(msg)


def _install_smtp_stub(monkeypatch) -> type[_SmtpStub]:
    _SmtpStub.sent = []
    monkeypatch.setattr(api_background, "smtplib", SimpleNamespace(
        SMTP=_SmtpStub,
        SMTP_SSL=_SmtpStub,
        SMTPResponseException=Exception,
    ))
    return _SmtpStub


def _payload(category: str | None = None) -> dict:
    payload = {
        "email": "admin@example.com",
        "subject": "test subject",
        "text_content": "test body",
        "smtp_username": "noreply@example.com",
        "smtp_from_email": "noreply@example.com",
        "smtp_server": "smtp.example.com",
        "smtp_port": 465,
    }
    if category is not None:
        payload["category"] = category
    return payload


class TestSendTaskGate:
    """③ ④ 任务内按类别生效"""

    async def test_alert_over_budget_skips_smtp(self, monkeypatch):
        _install_smtp_stub(monkeypatch)

        async def _blocked() -> bool:
            return False

        monkeypatch.setattr(api_background, "alert_budget_available", _blocked)
        result = await api_background.send_smtp_email_task.run(_payload(EMAIL_CATEGORY_ALERT))
        assert result == {"success": True, "suppressed": True}
        assert _SmtpStub.sent == []  # 超限时根本不该碰 SMTP

    async def test_code_email_bypasses_gate(self, monkeypatch):
        _install_smtp_stub(monkeypatch)
        calls = {"n": 0}

        async def _gate() -> bool:
            calls["n"] += 1
            return False  # 若验证码误入额度闸，会走 suppressed 分支而不发信

        monkeypatch.setattr(api_background, "alert_budget_available", _gate)
        result = await api_background.send_smtp_email_task.run(_payload())
        assert result == {"success": True}
        assert calls["n"] == 0
        assert len(_SmtpStub.sent) == 1


class TestAdminNotifyCategory:
    """⑤ 投递侧标记"""

    def test_payload_carries_alert_category(self, monkeypatch):
        captured: dict = {}

        class _DelayStub:
            def delay(self, payload: dict):
                captured.update(payload)
                return SimpleNamespace(id="stub-task-id")

        monkeypatch.setattr(admin_notify, "send_smtp_email_task", _DelayStub())
        assert notify_fc_trigger_failed("fc-algorithm", run_id=1) is True
        assert captured["category"] == EMAIL_CATEGORY_ALERT
