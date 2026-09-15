"""
100 并发注册后，使用这 100 个账号做登录 + 公共 file_list 接口延迟测试

每个并发任务独立创建 AsyncClient，避免共享连接池干扰。
流程:
  1. 生成 100 个唯一测试账号
  2. 并发调用 /user_signup 注册
  3. 注册全部成功后，用这 100 个账号并发执行 login → list_files

测量:
  - signup 注册阶段延迟
  - login → list_files 整流程的端到端延迟（ms），不含 logout（仅清理）

复用:
  - .env 中的 BASE_URL / PERF_TEST_PASSWORD
  - conftest.py 中的 logger
  - helpers.perf.compute_stats / dump_json / results_path

说明:
  - /user_signup 需要邮箱验证码，且验证码与邮箱绑定、单次有效。测试直接调用服务端签发逻辑
    为每个账号的邮箱各签发一个注册码（不真实发信），需与服务端连接同一个 Redis。
  - 用户名和邮箱会带时间戳，避免重复运行时冲突。

运行方式:
    uv run pytest -m perf_concurrent
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Any

import pytest
from httpx import AsyncClient

from .conftest import logger, BASE_URL
from .helpers.perf import compute_stats, dump_json, results_path
from .helpers.totp import issue_email_code

# ------------------------------------------------------------------ #
# 并发测试参数，按需修改
# ------------------------------------------------------------------ #
CONCURRENCY = 100
REQUEST_TIMEOUT = 60  # 秒，单个请求超时
LIST_PAGE = 1
LIST_SIZE = 10
TEST_PASSWORD = os.getenv("PERF_TEST_PASSWORD", "PerfTest123456!")
VERIFY_CODE_SOURCE = "direct:api_services.user_service.generate_email_code"


@dataclass(frozen=True)
class PerfAccount:
    """并发测试使用的临时账号。"""

    index: int
    username: str
    email: str
    password: str


def _build_accounts(run_id: str) -> list[PerfAccount]:
    """生成本次压测使用的唯一账号，避免重复运行时撞用户名/邮箱。"""
    return [
        PerfAccount(
            index=i,
            username=f"perf_{run_id}_{i:03d}",
            email=f"perf_{run_id}_{i:03d}@example.com",
            password=TEST_PASSWORD,
        )
        for i in range(CONCURRENCY)
    ]


async def _issue_verify_codes(accounts: list[PerfAccount]) -> dict[int, str]:
    """
    为每个账号的邮箱各签发一个注册验证码（验证码与邮箱绑定，不能多个账号共用）。

    直接调用服务端签发逻辑、不真实发信：压测关注登录与列表延迟，不应为 100 个账号发 100 封邮件。
    逐个签发即可（每个只是两次 Redis 操作），避免并发请求超出 Redis 连接池上限。
    """
    return {account.index: await issue_email_code(account.email) for account in accounts}


async def _signup_user(account: PerfAccount, verify_code: str) -> dict[str, Any]:
    """
    单个并发注册任务：独立 AsyncClient → user_signup

    返回字段:
      index         : 任务序号
      username      : 注册用户名
      signup_status : user_signup HTTP 状态码
      duration_ms   : 注册接口耗时
      error         : 任何异常文本
    """
    result: dict[str, Any] = {
        "index": account.index,
        "username": account.username,
        "signup_status": None,
        "duration_ms": None,
        "error": None,
    }
    try:
        async with AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as cli:
            t0 = time.perf_counter()
            rs = await cli.post(
                "/user_signup",
                json={
                    "username": account.username,
                    "email": account.email,
                    "password": account.password,
                    "verify_code": verify_code,
                    "active": True,
                    "institution": "Perf Test",
                    "position": "Researcher",
                    "research_field": "Massflow",
                    "region": "CN",
                    "orcid": None,
                    "homepage": None,
                },
            )
            result["signup_status"] = rs.status_code
            result["duration_ms"] = (time.perf_counter() - t0) * 1000.0

            if rs.status_code != 200:
                result["error"] = f"signup failed: {rs.text[:200]}"

    except Exception as e:  # pylint: disable=broad-except
        result["error"] = repr(e)

    return result


async def _user_flow(account: PerfAccount) -> dict[str, Any]:
    """
    单个并发任务：独立 AsyncClient → login → list_files → logout(清理)

    返回字段:
      index          : 任务序号
      username       : 登录用户名
      login_status   : login HTTP 状态码
      list_status    : list_files HTTP 状态码
      logout_status  : logout HTTP 状态码（仅供观察）
      duration_ms    : login 起到 list_files 响应止 的总耗时
      error          : 任何异常文本
    """
    result: dict[str, Any] = {
        "index": account.index,
        "username": account.username,
        "login_status": None,
        "list_status": None,
        "logout_status": None,
        "duration_ms": None,
        "error": None,
    }
    try:
        async with AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as cli:
            t0 = time.perf_counter()

            # 1. login (form-data)
            rs = await cli.post(
                "/login",
                data={
                    "grant_type": "password",
                    "username": account.username,
                    "password": account.password,
                },
            )
            result["login_status"] = rs.status_code
            if rs.status_code != 200:
                result["error"] = f"login failed: {rs.text[:200]}"
                return result

            body = rs.json()
            token = body["access_token"]
            token_type = body["token_type"]
            auth = {"Authorization": f"{token_type.title()} {token}"}

            # 2. list_files (公共文件列表，page/size 走 query, FileFilter 走 body)
            rs = await cli.post(
                f"/files/list_files?page={LIST_PAGE}&size={LIST_SIZE}",
                headers=auth,
                json={},
            )
            result["list_status"] = rs.status_code
            t1 = time.perf_counter()
            result["duration_ms"] = (t1 - t0) * 1000.0

            if rs.status_code != 200:
                result["error"] = f"list_files failed: {rs.text[:200]}"

            # 3. logout 仅清理 redis token，不计入延迟
            try:
                rs_lo = await cli.post("/logout", headers=auth)
                result["logout_status"] = rs_lo.status_code
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("[perf concurrent] idx=%s logout failed: %r", account.index, e)

    except Exception as e:  # pylint: disable=broad-except
        result["error"] = repr(e)

    return result


class TestPerfConcurrentLogin:
    """100 并发注册后，使用这 100 个账号做登录 + 公共 file_list 延迟测试"""

    @pytest.mark.perf_concurrent
    async def test_concurrent_login_listfile(self):
        """100 个账号并发注册成功后，并行执行 login → list_files，记录端到端延迟"""
        run_id = os.getenv("PERF_TEST_RUN_ID", time.strftime("%Y%m%d%H%M%S"))
        accounts = _build_accounts(run_id)
        verify_codes = await _issue_verify_codes(accounts)

        logger.info("[perf concurrent] starting %s concurrent signup tasks", CONCURRENCY)
        signup_wall_t0 = time.perf_counter()

        signup_tasks = [_signup_user(account, verify_codes[account.index]) for account in accounts]
        signup_results = await asyncio.gather(*signup_tasks, return_exceptions=False)

        signup_wall_elapsed = time.perf_counter() - signup_wall_t0
        signup_successes = [
            r for r in signup_results
            if r["error"] is None and r["duration_ms"] is not None
        ]
        signup_failures = [r for r in signup_results if r["error"] is not None]
        signup_latencies = [r["duration_ms"] for r in signup_successes]
        signup_stats = compute_stats(signup_latencies) if signup_latencies else {}

        logger.info(
            "[perf concurrent] signup total=%s success=%s fail=%s wall_elapsed=%.2fs",
            CONCURRENCY, len(signup_successes), len(signup_failures), signup_wall_elapsed,
        )
        if signup_stats:
            logger.info(
                "[perf concurrent] signup latency ms: min=%.2f avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f",
                signup_stats["min_ms"], signup_stats["avg_ms"], signup_stats["p50_ms"],
                signup_stats["p95_ms"], signup_stats["p99_ms"], signup_stats["max_ms"],
            )
        for f in signup_failures:
            logger.error("[perf concurrent] signup idx=%s error: %s", f["index"], f["error"])

        if signup_failures:
            json_path = results_path("concurrent_signup_login")
            dump_json(
                {
                    "run_id": run_id,
                    "verify_code_source": VERIFY_CODE_SOURCE,
                    "concurrency": CONCURRENCY,
                    "endpoint_signup": "/user_signup",
                    "signup_wall_elapsed_s": signup_wall_elapsed,
                    "signup_success_count": len(signup_successes),
                    "signup_failure_count": len(signup_failures),
                    "signup_stats": signup_stats,
                    "signup_latencies_ms": signup_latencies,
                    "signup_results_full": signup_results,
                },
                json_path,
            )
            pytest.fail(f"{len(signup_failures)}/{CONCURRENCY} signup tasks failed; see {json_path}")

        logger.info("[perf concurrent] starting %s concurrent login/list_files tasks", CONCURRENCY)
        wall_t0 = time.perf_counter()

        tasks = [_user_flow(account) for account in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        wall_elapsed = time.perf_counter() - wall_t0

        # 拆分成功/失败
        successes = [r for r in results if r["error"] is None and r["duration_ms"] is not None]
        failures = [r for r in results if r["error"] is not None]
        latencies = [r["duration_ms"] for r in successes]

        stats = compute_stats(latencies) if latencies else {}

        logger.info(
            "[perf concurrent] total=%s success=%s fail=%s wall_elapsed=%.2fs",
            CONCURRENCY, len(successes), len(failures), wall_elapsed,
        )
        if stats:
            logger.info(
                "[perf concurrent] latency ms: min=%.2f avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f",
                stats["min_ms"], stats["avg_ms"], stats["p50_ms"],
                stats["p95_ms"], stats["p99_ms"], stats["max_ms"],
            )
        for f in failures:
            logger.error("[perf concurrent] idx=%s error: %s", f["index"], f["error"])

        # 落盘
        json_path = results_path("concurrent_signup_login")
        payload = {
            "run_id": run_id,
            "verify_code_source": VERIFY_CODE_SOURCE,
            "concurrency": CONCURRENCY,
            "endpoint_signup": "/user_signup",
            "endpoint_login": "/login",
            "endpoint_list": f"/files/list_files?page={LIST_PAGE}&size={LIST_SIZE}",
            "signup_wall_elapsed_s": signup_wall_elapsed,
            "signup_success_count": len(signup_successes),
            "signup_failure_count": len(signup_failures),
            "signup_stats": signup_stats,
            "signup_latencies_ms": signup_latencies,
            "signup_results_full": signup_results,
            "wall_elapsed_s": wall_elapsed,
            "success_count": len(successes),
            "failure_count": len(failures),
            "stats": stats,
            "latencies_ms": latencies,
            "results_full": results,
        }
        dump_json(payload, json_path)
        logger.info("[perf concurrent] result saved to %s", json_path)

        assert not failures, f"{len(failures)}/{CONCURRENCY} tasks failed; see {json_path}"
