"""中转下载单元测试（无需 OSS / DB / Redis，纯本地逻辑）

覆盖：token 签发/校验/过期/篡改、Content-Disposition 编码与头注入防护。

运行：
    uv run pytest tests/test_relay_unit.py -v
"""
from __future__ import annotations

import time

import jwt
import pytest

from api_services.relay_service import (
    RELAY_SEMAPHORE,
    _content_disposition,
    issue_relay_token,
    verify_relay_token,
)
from constants.file_constants import RELAY_MAX_CONCURRENCY, RELAY_TOKEN_EXPIRE_SECONDS
from models.auth import SECRET_KEY


class TestRelayToken:
    """中转 token：绑定 file_id + part，短时有效，防篡改"""

    def test_roundtrip(self):
        assert verify_relay_token(issue_relay_token(42, "ibd"), 42, "ibd")
        assert verify_relay_token(issue_relay_token(42, "imzml"), 42, "imzml")

    def test_wrong_file_id_rejected(self):
        """token 不能被挪用到其它 file_id（防越权取任意对象）"""
        assert not verify_relay_token(issue_relay_token(42, "ibd"), 43, "ibd")

    def test_wrong_part_rejected(self):
        assert not verify_relay_token(issue_relay_token(42, "ibd"), 42, "imzml")

    def test_tampered_rejected(self):
        tok = issue_relay_token(42, "ibd")
        assert not verify_relay_token(tok + "x", 42, "ibd")
        assert not verify_relay_token("garbage", 42, "ibd")
        assert not verify_relay_token("", 42, "ibd")

    def test_expired_rejected(self):
        expired = jwt.encode(
            {"file_id": 42, "part": "ibd", "exp": int(time.time()) - 1},
            SECRET_KEY,
            algorithm="HS256",
        )
        assert not verify_relay_token(expired, 42, "ibd")

    def test_forged_with_other_key_rejected(self):
        forged = jwt.encode(
            {"file_id": 42, "part": "ibd", "exp": int(time.time()) + 60},
            "not-the-real-secret",
            algorithm="HS256",
        )
        assert not verify_relay_token(forged, 42, "ibd")

    def test_ttl_is_positive(self):
        assert RELAY_TOKEN_EXPIRE_SECONDS > 0


class TestContentDisposition:
    """下载文件名头：ASCII 回退 + UTF-8 编码，且过滤控制字符防头注入"""

    def test_ascii_filename(self):
        cd = _content_disposition("Brain01.ibd")
        assert 'filename="Brain01.ibd"' in cd
        assert "filename*=UTF-8''Brain01.ibd" in cd

    def test_chinese_filename(self):
        cd = _content_disposition("脑切片01.ibd")
        assert "filename*=UTF-8''" in cd
        assert "%E8%84%91" in cd  # "脑" 的 UTF-8 百分号编码前缀

    def test_header_injection_filtered(self):
        cd = _content_disposition('a"\r\nX-Evil: 1.ibd')
        assert "\r" not in cd and "\n" not in cd


class TestRelaySemaphore:
    """并发信号量按配置初始化"""

    def test_initial_value(self):
        # asyncio.Semaphore 的初始值保存在 _value
        assert RELAY_SEMAPHORE._value == RELAY_MAX_CONCURRENCY  # pylint: disable=protected-access
