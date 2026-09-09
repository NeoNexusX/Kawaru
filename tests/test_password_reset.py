"""
密码重置（忘记密码）端到端测试 —— 对运行中的服务器发 HTTP 请求（集成测试）。

覆盖三点：
  1) 发送侧限流：同一邮箱冷却窗口内重复申请验证码 → 429；等待冷却后再申请 → 200（恢复）
  2) 校验侧防爆破：连续提交错误验证码达上限 → 锁定 429，且本轮验证码作废
  3) 正常重置：用有效验证码重置 → 200，新密码可登录、旧密码失效

验证码来源：worker 以 LOG_LEVEL=debug 把 SMTP 正文（含验证码）写入 background.log，
测试从日志按正则截取最新有效码（与注册测试同机制）。

前置（CI 已满足）：服务器 + worker(debug 日志) + SMTP + Redis + DB 在 BASE_URL 运行。
"""
from __future__ import annotations

import asyncio
import secrets
import time

import pytest
from httpx import AsyncClient

from boot.config import get_config
from .conftest import logger, BASE_URL
from .helpers.totp import (
    RESET_CODE_PATTERN,
    fetch_verify_code_from_logs,
    request_email_verify_code,
)

REQUEST_TIMEOUT = 60  # 秒，单个请求超时
# 等待时间按当前测试栈配置动态计算，避免和默认冷却值耦合。
COOLDOWN_WAIT_BUFFER_SECONDS = 10


def _wrong_code(real_code: str) -> str:
    """返回一个保证不等于 real_code 的 6 位错误验证码，避免百万分之一的巧合命中。"""
    return "000000" if real_code != "000000" else "111111"


async def _login(client: AsyncClient, username: str, password: str):
    """登录，返回 httpx 响应。"""
    return await client.post(
        "/login",
        data={"grant_type": "password", "username": username, "password": password},
    )


async def _signup_dedicated_account(
    client: AsyncClient, username: str, email: str, password: str
) -> None:
    """
    创建专用测试账号。

    注册验证码是全局 TOTP（与邮箱无关），因此用一个独立 probe 邮箱取码，
    从而不在目标邮箱上占用发送冷却（与 test_perf_concurrent_login 同套路），
    保证后续对目标邮箱的首次重置发码能立即成功。
    """
    probe_email = f"pwdreset_probe_{secrets.token_hex(4)}@example.com"
    code, meta = await fetch_verify_code_from_logs(
        base_url=BASE_URL,
        email=probe_email,
        timeout=REQUEST_TIMEOUT,
        purpose="register",
    )
    logger.info("register code fetched via %s -> %s", meta["source"], code)

    rs = await client.post(
        "/user_signup",
        json={
            "username": username,
            "email": email,
            "password": password,
            "verify_code": code,
            "active": True,
            "institution": "Reset Test",
            "position": "Researcher",
            "research_field": "Massflow",
            "region": "CN",
            "orcid": None,
            "homepage": None,
        },
    )
    assert rs.status_code == 200, f"signup failed: {rs.status_code} {rs.text[:300]}"


@pytest.mark.password_reset
async def test_password_reset_flow():
    """忘记密码重置：限流拦截 → 爆破锁定 → 冷却恢复后正常重置。"""
    run_id = f"{int(time.time())}_{secrets.token_hex(3)}"
    username = f"pwdreset_{run_id}"
    email = f"pwdreset_{run_id}@example.com"
    old_password = "OldPass123456!"
    new_password = "NewPass654321!"

    email_config = get_config().email
    max_attempts = email_config.reset_password_max_attempts
    cooldown_seconds = email_config.email_send_cooldown_seconds
    cooldown_wait_seconds = cooldown_seconds + COOLDOWN_WAIT_BUFFER_SECONDS

    async with AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as client:
        # ---------- Setup：专用账号 ----------
        await _signup_dedicated_account(client, username, email, old_password)
        rs = await _login(client, username, old_password)
        assert rs.status_code == 200, f"login(old) failed: {rs.status_code} {rs.text[:300]}"

        # ---------- 1) 发送侧限流：首次发码成功并取码，立即重复申请被拦截 ----------
        code1, meta = await fetch_verify_code_from_logs(
            base_url=BASE_URL,
            email=email,
            timeout=REQUEST_TIMEOUT,
            purpose="reset_password",
            pattern=RESET_CODE_PATTERN,
        )
        logger.info("reset code #1 via %s -> %s", meta["source"], code1)

        dup = await request_email_verify_code(
            BASE_URL, email, REQUEST_TIMEOUT, purpose="reset_password"
        )
        assert dup["status_code"] == 429, (
            f"cooldown should block duplicate request, got {dup['status_code']}: {dup['text']}"
        )

        # ---------- 2) 校验侧防爆破：错误码达上限后锁定，验证码作废 ----------
        wrong = _wrong_code(code1)
        for i in range(max_attempts):
            rs = await client.post(
                "/password_reset",
                json={"email": email, "verify_code": wrong, "new_password": new_password},
            )
            assert rs.status_code == 400, (
                f"wrong attempt {i + 1}/{max_attempts}: expected 400, "
                f"got {rs.status_code}: {rs.text[:200]}"
            )
        # 超过上限 → 锁定 429（本轮 code1 被作废）
        rs = await client.post(
            "/password_reset",
            json={"email": email, "verify_code": wrong, "new_password": new_password},
        )
        assert rs.status_code == 429, (
            f"exceeding max attempts should lock with 429, got {rs.status_code}: {rs.text[:200]}"
        )
        # 作废后即便提交真码也无效
        rs = await client.post(
            "/password_reset",
            json={"email": email, "verify_code": code1, "new_password": new_password},
        )
        assert rs.status_code in (400, 429), (
            f"invalidated code must be rejected, got {rs.status_code}: {rs.text[:200]}"
        )

        # ---------- 3) 冷却恢复 + 正常重置 ----------
        logger.info(
            "waiting %ss for send cooldown (%ss) to expire ...",
            cooldown_wait_seconds,
            cooldown_seconds,
        )
        await asyncio.sleep(cooldown_wait_seconds)

        code2, meta = await fetch_verify_code_from_logs(
            base_url=BASE_URL,
            email=email,
            timeout=REQUEST_TIMEOUT,
            purpose="reset_password",
            pattern=RESET_CODE_PATTERN,
        )
        logger.info("reset code #2 after cooldown via %s -> %s", meta["source"], code2)

        rs = await client.post(
            "/password_reset",
            json={"email": email, "verify_code": code2, "new_password": new_password},
        )
        assert rs.status_code == 200, f"reset failed: {rs.status_code} {rs.text[:300]}"

        # 新密码可登录，旧密码失效，证明确实更新
        rs = await _login(client, username, new_password)
        assert rs.status_code == 200, f"login(new) failed: {rs.status_code} {rs.text[:300]}"
        rs = await _login(client, username, old_password)
        assert rs.status_code == 401, (
            f"old password must be rejected after reset, got {rs.status_code}"
        )
