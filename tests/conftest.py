from pathlib import Path
import os
from dotenv import load_dotenv
import pytest
from httpx import AsyncClient

from tools.logger import get_logger
from boot.config import get_config
from boot.bootstrap import bootstrap
bootstrap()  # 初始化 Env + Config + Logger

# 此处同时加载项目环境变量.env和pytest环境变量.env.pytest
# pytest环境变量用于设置pytest测试相关的变量（如接口地址、测试文件地址等）
# 加载项目环境变量如有测试项涉及到相关变量，方便调用
project_env_path = Path(__file__).resolve().parent.parent / ".env" # 项目环境变量
pytest_env_path = Path(__file__).resolve().parent.parent / ".env.pytest" # pytest环境变量
load_dotenv(project_env_path)
load_dotenv(pytest_env_path)

BASE_URL = os.getenv("BASE_URL")
UPLOAD_FILE_DIR = os.getenv("UPLOAD_FILE_DIR")
TEST_USER = os.getenv("TEST_USER")
TEST_PWD = os.getenv("TEST_PWD")

logger = get_logger("test")
# 基础存储配置来自 config
config = get_config()
if not config:
    logger.error("pytest configuration is not loaded")
    raise RuntimeError("Pytest configuration is not loaded")
storage_config = config.storage

BYTES_IN_GB = 1024 ** 3
BYTES_IN_MB = 1024 ** 2
ALLOWED_FILE_TYPES = set(storage_config.allowed_file_types)
CHUNK_BUFFER_SIZE = int(storage_config.chunk_buffer_size_mb) * BYTES_IN_MB


@pytest.fixture(scope="session", name="async_client")
async def _async_client():
    if not BASE_URL:
        pytest.fail("Environment variable BASE_URL is required")
    async with AsyncClient(base_url=BASE_URL, timeout=60) as client:
        yield client


@pytest.fixture(scope="session", name="admin_login")
async def _admin_login(async_client):
    client = async_client
    username = os.getenv("TEST_USER")
    password = os.getenv("TEST_PWD")
    if not (username and password):
        pytest.skip("TEST_USER/TEST_PWD hasn't provided yet.")

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
