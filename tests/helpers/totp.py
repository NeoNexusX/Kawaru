"""Helpers for obtaining email verification codes in tests (server logs or direct issuance)."""
from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from httpx import AsyncClient

# 注册/改邮箱文案（见 api_helpers/email_template.py）："Your verification code is: 123456"
VERIFY_CODE_PATTERN = re.compile(r"verification code is:\s+(\d{6})")
# 密码重置文案（见 api_helpers/email_template.py）："Password reset code: 123456"
RESET_CODE_PATTERN = re.compile(r"Password reset code:\s+(\d{6})")

# SMTP debug 行由 background.api_background 写入（logger 组名 background），固定落此文件。
VERIFY_CODE_LOG_FILE = "background.log"

# 项目根：tests/helpers/totp.py -> parents[2]
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _log_dir() -> Path:
    # 与 conftest 一致：LOG_DIR 相对项目根解析，避免依赖 pytest 启动时的 CWD。
    return _PROJECT_ROOT / os.getenv("LOG_DIR", "kawaru_logs")


def _verify_code_log_path() -> Path:
    return _log_dir() / VERIFY_CODE_LOG_FILE


def _read_since(path: Path, offset: int) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as file:
            file.seek(offset)
            return file.read()
    except FileNotFoundError:
        return ""


def _latest_match(text: str, pattern: re.Pattern[str]) -> str | None:
    matches = pattern.findall(text)
    return matches[-1] if matches else None


async def issue_email_code(email: str, purpose: str = "register") -> str:
    """
    直接调用服务端签发逻辑为指定邮箱生成验证码并返回（不发邮件、不占用该邮箱的发送冷却）。

    验证码与用途 + 邮箱绑定且单次有效，供测试前置步骤为账号准备注册码；
    测试进程需与服务端连接同一个 Redis（CI 在 app 容器内运行 pytest，满足该条件）。
    """
    # 延迟导入：服务端模块依赖 conftest 中 bootstrap() 完成的配置与 Celery 初始化
    # pylint: disable=import-outside-toplevel
    from api_services.user_service import generate_email_code
    from models.email_models import EmailPurpose

    return await generate_email_code(EmailPurpose(purpose), email)


async def request_email_verify_code(
    base_url: str,
    email: str,
    timeout: float | int,
    purpose: str = "register",
) -> dict[str, Any]:
    """Request the app to generate an email verification code for the given purpose."""
    async with AsyncClient(base_url=base_url, timeout=timeout) as client:
        response = await client.post(
            "/emailsmtprequest",
            json={
                "email": email,
                "send_time": datetime.now(timezone.utc).isoformat(),
                "purpose": purpose,
            },
        )
        return {
            "status_code": response.status_code,
            "text": response.text[:500],
        }


async def fetch_verify_code_from_logs(
    base_url: str,
    email: str,
    timeout: float | int = 60,
    poll_timeout: float = 15,
    poll_interval: float = 0.2,
    purpose: str = "register",
    pattern: re.Pattern[str] = VERIFY_CODE_PATTERN,
) -> tuple[str, dict[str, Any]]:
    """
    Trigger /emailsmtprequest (for `purpose`) and read the generated code from newly
    appended logs, matching `pattern` (defaults to the register/update code format).

    The SMTP message is logged at debug level by background.api_background, which
    (logger group 'background') writes to <LOG_DIR>/background.log, so we only read
    that single file.
    """
    log_path = _verify_code_log_path()
    try:
        offset = log_path.stat().st_size
    except FileNotFoundError:
        offset = 0

    request_result = await request_email_verify_code(base_url, email, timeout, purpose=purpose)
    if request_result["status_code"] != 200:
        raise RuntimeError(
            f"/emailsmtprequest(purpose={purpose}) failed (status={request_result['status_code']}), "
            f"no verification code will be generated. Response: {request_result['text']}"
        )

    deadline = time.monotonic() + poll_timeout
    while time.monotonic() < deadline:
        code = _latest_match(_read_since(log_path, offset), pattern)
        if code:
            return code, {
                "source": f"log:{log_path}",
                "request_result": request_result,
            }
        await asyncio.sleep(poll_interval)

    raise RuntimeError(
        f"Could not find verification code (purpose={purpose}, pattern={pattern.pattern!r}) "
        "in logs after /emailsmtprequest returned 200. The code is logged by the SMTP debug "
        "message; make sure the worker process runs with LOG_LEVEL=debug so the record is written. "
        f"Checked log: {log_path}"
    )
