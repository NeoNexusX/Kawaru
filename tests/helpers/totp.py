"""Helpers for retrieving email verification codes from server logs."""
from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from httpx import AsyncClient

VERIFY_CODE_PATTERN = re.compile(r"Your code is\s+(\d{6})")

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


def _latest_verify_code(text: str) -> str | None:
    matches = VERIFY_CODE_PATTERN.findall(text)
    return matches[-1] if matches else None


async def request_email_verify_code(
    base_url: str,
    email: str,
    timeout: float | int,
) -> dict[str, Any]:
    """Request the app to generate an email verification code."""
    async with AsyncClient(base_url=base_url, timeout=timeout) as client:
        response = await client.post(
            "/emailsmtprequest",
            json={
                "email": email,
                "send_time": datetime.now(timezone.utc).isoformat(),
                "purpose": "register",
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
    poll_timeout: float = 10,
    poll_interval: float = 0.2,
) -> tuple[str, dict[str, Any]]:
    """
    Trigger /emailsmtprequest and read the generated code from newly appended logs.

    The SMTP message is logged at debug level by background.api_background, which
    (logger group 'background') writes to <LOG_DIR>/background.log, so we only read
    that single file.
    """
    log_path = _verify_code_log_path()
    try:
        offset = log_path.stat().st_size
    except FileNotFoundError:
        offset = 0

    request_result = await request_email_verify_code(base_url, email, timeout)
    if request_result["status_code"] != 200:
        raise RuntimeError(
            f"/emailsmtprequest failed (status={request_result['status_code']}), "
            f"no verification code will be generated. Response: {request_result['text']}"
        )

    deadline = time.monotonic() + poll_timeout
    while time.monotonic() < deadline:
        code = _latest_verify_code(_read_since(log_path, offset))
        if code:
            return code, {
                "source": f"log:{log_path}",
                "request_result": request_result,
            }
        await asyncio.sleep(poll_interval)

    raise RuntimeError(
        "Could not find verification code in logs after /emailsmtprequest returned 200. "
        "The code is logged by the SMTP debug message as 'Your code is <digits>'; "
        "make sure the worker process runs with LOG_LEVEL=debug so the record is written. "
        f"Checked log: {log_path}"
    )
