"""
用户模块内存消耗测试 - 使用 ASGI 内嵌模式（不启动真实服务器）

通过 httpx.ASGITransport 直接挂载 FastAPI app，memray 可以追踪到服务端内存。

运行方式:
    uv run pytest tests/test_user_memory.py -m memray --memray-bin-path=tests/
"""
from typing import Tuple
import pytest
from httpx import AsyncClient, ASGITransport
from main import app
from .conftest import logger, TEST_USER, TEST_PWD


class TestUserMemory:
    """用户模块关键接口内存消耗测试，使用 ASGI 内嵌模式追踪服务端内存"""

    @pytest.fixture(scope="class")
    async def asgi_client(self):
        """使用 ASGITransport 挂载 FastAPI app，class 级别复用，避免重复初始化 lifespan"""
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            timeout=60,
        ) as client:
            yield client

    @pytest.fixture(scope="class")
    async def asgi_token(self, asgi_client: AsyncClient):
        """登录并返回 (token, token_type)，class 级别复用"""
        if not (TEST_USER and TEST_PWD):
            pytest.skip("TEST_USER/TEST_PWD not configured")

        rs = await asgi_client.post(
            "/login",
            data={"grant_type": "password", "username": TEST_USER, "password": TEST_PWD},
        )
        assert rs.status_code == 200, rs.text
        data = rs.json()
        return data["access_token"], data["token_type"]

    @pytest.mark.memray
    @pytest.mark.limit_memory("10 MB")
    async def test_memory_login(self, asgi_client: AsyncClient):
        """POST /login 内存消耗，超出 10MB 则失败"""
        rs = await asgi_client.post(
            "/login",
            data={"grant_type": "password", "username": TEST_USER, "password": TEST_PWD},
        )
        assert rs.status_code == 200, rs.text
        logger.info(f"test_memory_login: status={rs.status_code}")

    @pytest.mark.memray
    @pytest.mark.limit_memory("10 MB")
    async def test_memory_get_user(self, asgi_client: AsyncClient, asgi_token: Tuple[str, str]):
        """GET /user 内存消耗"""
        token, token_type = asgi_token
        rs = await asgi_client.get(
            "/user",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 200, rs.text
        logger.info(f"test_memory_get_user: status={rs.status_code}")

    @pytest.mark.memray
    @pytest.mark.limit_memory("10 MB")
    async def test_memory_update_user(self, asgi_client: AsyncClient, asgi_token: Tuple[str, str]):
        """POST /user_change 内存消耗"""
        token, token_type = asgi_token
        rs = await asgi_client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "institution": "Memory Test University"},
        )
        assert rs.status_code == 200, rs.text
        logger.info(f"test_memory_update_user: status={rs.status_code}")

    @pytest.mark.memray
    @pytest.mark.limit_memory("10 MB")
    async def test_memory_logout(self, asgi_client: AsyncClient, asgi_token: Tuple[str, str]):
        """POST /logout 内存消耗"""
        token, token_type = asgi_token
        rs = await asgi_client.post(
            "/logout",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 200, rs.text
        logger.info(f"test_memory_logout: status={rs.status_code}")
