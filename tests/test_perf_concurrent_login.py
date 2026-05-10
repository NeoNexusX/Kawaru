"""
100 并发登录 + 公共 file_list 接口延迟测试

每个并发任务独立创建 AsyncClient，避免共享连接池干扰。
测量"login → list_files"整流程的端到端延迟（ms），不含 logout（仅清理）。

复用:
  - .env.pytest 中的 BASE_URL / TEST_USER / TEST_PWD
  - conftest.py 中的 logger
  - perf_helper.compute_stats / dump_json / results_path

运行方式:
    uv run pytest -m perf_concurrent
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from httpx import AsyncClient

from .conftest import logger, BASE_URL, TEST_USER, TEST_PWD
from .perf_helper import compute_stats, dump_json, results_path

# ------------------------------------------------------------------ #
# 并发测试参数，按需修改
# ------------------------------------------------------------------ #
CONCURRENCY = 100
REQUEST_TIMEOUT = 60  # 秒，单个请求超时
LIST_PAGE = 1
LIST_SIZE = 10


async def _user_flow(idx: int) -> dict[str, Any]:
    """
    单个并发任务：独立 AsyncClient → login → list_files → logout(清理)

    返回字段:
      index          : 任务序号
      login_status   : login HTTP 状态码
      list_status    : list_files HTTP 状态码
      logout_status  : logout HTTP 状态码（仅供观察）
      duration_ms    : login 起到 list_files 响应止 的总耗时
      error          : 任何异常文本
    """
    result: dict[str, Any] = {
        "index": idx,
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
                    "username": TEST_USER,
                    "password": TEST_PWD,
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
                logger.warning("[perf concurrent] idx=%s logout failed: %r", idx, e)

    except Exception as e:  # pylint: disable=broad-except
        result["error"] = repr(e)

    return result


class TestPerfConcurrentLogin:
    """100 并发登录 + 公共 file_list 延迟测试"""

    @pytest.mark.perf_concurrent
    async def test_concurrent_login_listfile(self):
        """100 个并发任务并行执行 login → list_files，记录每个任务的端到端延迟"""
        if not BASE_URL:
            pytest.skip("BASE_URL not set in .env.pytest")
        if not (TEST_USER and TEST_PWD):
            pytest.skip("TEST_USER/TEST_PWD not set in .env.pytest")

        logger.info("[perf concurrent] starting %s concurrent tasks", CONCURRENCY)
        wall_t0 = time.perf_counter()

        tasks = [_user_flow(i) for i in range(CONCURRENCY)]
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
        json_path = results_path("concurrent_login")
        payload = {
            "concurrency": CONCURRENCY,
            "endpoint_login": "/login",
            "endpoint_list": f"/files/list_files?page={LIST_PAGE}&size={LIST_SIZE}",
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
