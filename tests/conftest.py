from typing import Optional
from pathlib import Path
import os
import re
from dotenv import load_dotenv
import pytest
from httpx import AsyncClient, Limits, Timeout

from tools.logger import get_logger
from boot.config import get_config
from boot.bootstrap import bootstrap
bootstrap()  # 初始化 Env + Config + Logger

# ------------------------------------------------------------------ #
# 环境变量加载
# 统一从项目 .env 读取（含测试用的 BASE_URL / TEST_USER / TEST_PWD）。
# CI 不依赖此文件：BASE_URL 由 compose 注入，账号留空时回落 api.log 解析。
# ------------------------------------------------------------------ #
project_env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(project_env_path)

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")


# ------------------------------------------------------------------ #
# 测试账号解析
# 优先使用 .env 中显式配置的 TEST_USER/TEST_PWD；
# 未配置时，尝试从 api.log 中提取首次启动时自动创建的管理员账号密码
# （该密码只在数据库为空、首次创建管理员账号时打印一次）
# ------------------------------------------------------------------ #
_ADMIN_CREATED_LOG_PATTERN = re.compile(r"create admin user (\S+), password (\d+)")


def _extract_admin_credentials_from_log() -> tuple[Optional[str], Optional[str]]:
    """从 api.log 中读取首次创建管理员账号时打印的用户名/密码，找不到则返回 (None, None)"""
    log_dir = os.getenv("LOG_DIR", "kawaru_logs")
    log_path = Path(__file__).resolve().parent.parent / log_dir / "api.log"
    if not log_path.exists():
        return None, None

    username = password = None
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            match = _ADMIN_CREATED_LOG_PATTERN.search(line)
            if match:
                username, password = match.group(1), match.group(2)
    return username, password


def _resolve_test_credentials() -> tuple[Optional[str], Optional[str]]:
    username, password = os.getenv("TEST_USER"), os.getenv("TEST_PWD")
    if username and password:
        return username, password
    log_user, log_pwd = _extract_admin_credentials_from_log()
    return username or log_user, password or log_pwd


TEST_USER, TEST_PWD = _resolve_test_credentials()


# ------------------------------------------------------------------ #
# 并发测试配置
# ------------------------------------------------------------------ #
MAX_CONNECTIONS = 1200
MAX_KEEPALIVE_CONNECTIONS = 200
REQUEST_TIMEOUT = 60.0  # 增加超时时间


# ------------------------------------------------------------------ #
# 日志系统初始化
# ------------------------------------------------------------------ #
def _init_logging(log_level: Optional[str] = None, log_dir: Optional[str] = None) -> None:
    """显式初始化日志系统（必须先于任何 get_logger 调用）"""
    import tools.logger as _logger  # pylint: disable=import-outside-toplevel

    if getattr(_logger.LOGGER_MANAGER, "initialized", False):
        return
    _logger.LOGGER_MANAGER = _logger.LoggerManager()

    _logger.LOGGER_MANAGER.init_app(
        log_level=log_level or "debug",
        log_dir=log_dir or "tests",
    )

_init_logging(log_level="debug", log_dir="tests")
logger = get_logger("test")

# 校验 config 已正确加载（测试本身不直接消费具体配置项）
if not get_config():
    logger.error("pytest configuration is not loaded")
    raise RuntimeError("Pytest configuration is not loaded")


# ------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------ #
@pytest.fixture(scope="session", name="async_client")
async def _async_client():
    async with AsyncClient(base_url=BASE_URL, timeout=60) as client:
        yield client


@pytest.fixture(scope="session", name="async_client_pool")
async def _async_client_pool():
    limits = Limits(
        max_connections=MAX_CONNECTIONS,
        max_keepalive_connections=MAX_KEEPALIVE_CONNECTIONS,
        keepalive_expiry=60.0
    )
    async with AsyncClient(
        base_url=BASE_URL,
        limits=limits,
        timeout=Timeout(REQUEST_TIMEOUT, connect=20.0),
        follow_redirects=True,
        max_redirects=3,
        http2=True,
        trust_env=False
    ) as client:
        yield client


@pytest.fixture(scope="session", name="admin_login")
async def _admin_login(async_client):
    client = async_client
    username = TEST_USER
    password = TEST_PWD
    if not (username and password):
        pytest.fail("TEST_USER/TEST_PWD could not be resolved from env or api.log; cannot run tests that require login.")

    # 登录
    rs = await client.post(
        "/login",
        data={
            "grant_type": "password",
            "username": username,
            "password": password,
        },
    )
    assert rs.status_code == 200, rs.text
    token, token_type = rs.json()["access_token"], rs.json()["token_type"]

    # 验证当前用户与配置用户一致
    rs = await client.get(
        "/user",
        headers={"Authorization": f"{token_type.title()} {token}"},
    )
    assert rs.status_code == 200, rs.text
    current_username = rs.json()["username"]
    if current_username != username:
        pytest.fail(
            f"Current user is not the same as target user, "
            f"username:{username}, current username:{current_username}"
        )

    yield client, token, token_type

    # 测试结束后，退出登录
    rs = await client.post(
        "/logout",
        headers={"Authorization": f"{token_type.title()} {token}"},
    )
    assert rs.status_code == 200, rs.text
