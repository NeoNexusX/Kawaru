"""
Tests for user management API.

# To test this module:
# 1. Copy .env.pytest.example as .env.pytest and configure it.
# 2. Run "uv run main.py" to start the FastAPI app.
# 3. Run "uv run pytest -m user" in shell with venv activated.
# 4. Run "uv run pytest -m user --benchmark-only" for benchmark only.
"""
import asyncio
from typing import Tuple
import pytest
from httpx import AsyncClient
from .conftest import logger, TEST_USER, TEST_PWD, BASE_URL


class TestUser:
    """Tests for user management flow: login → get info → update → verify."""

    @pytest.mark.user
    async def test_login(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证登录成功并返回有效 token"""
        client, token, token_type = admin_login
        logger.info("test_login: verifying token is valid...")

        rs = await client.get(
            "/user",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 200, rs.text
        data = rs.json()
        assert data["username"] == TEST_USER
        assert data["active"] is True
        logger.info(f"test_login passed: username={data['username']}")

    @pytest.mark.user
    async def test_get_user_info(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证 GET /user 返回完整的用户详情字段"""
        client, token, token_type = admin_login
        logger.info("test_get_user_info: fetching user detail...")

        rs = await client.get(
            "/user",
            headers={"Authorization": f"{token_type.title()} {token}"},
        )
        assert rs.status_code == 200, rs.text
        data = rs.json()

        for field in ("username", "active", "identity", "institution",
                      "position", "research_field", "region", "orcid", "homepage"):
            assert field in data, f"missing field: {field}"

        logger.info(f"test_get_user_info passed: {data}")

    @pytest.mark.user
    async def test_update_user_institution(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证更新 institution 字段"""
        client, token, token_type = admin_login
        new_institution = "Xiamen University"
        logger.info(f"test_update_user_institution: updating to '{new_institution}'...")

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "institution": new_institution},
        )
        assert rs.status_code == 200, rs.text

        rs = await client.get("/user", headers={"Authorization": f"{token_type.title()} {token}"})
        assert rs.status_code == 200, rs.text
        assert rs.json()["institution"] == new_institution
        logger.info("test_update_user_institution passed")

    @pytest.mark.user
    async def test_update_wrong_user_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证修改其他用户名时被拒绝"""
        client, token, token_type = admin_login

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": "nonexistent_other_user", "institution": "Should Fail"},
        )
        assert rs.status_code in (400, 401), rs.text
        logger.info("test_update_wrong_user_rejected passed")

    @pytest.mark.user
    async def test_wrong_password_rejected(self, async_client: AsyncClient):
        """2. 错误密码登录应返回 401"""
        rs = await async_client.post(
            "/login",
            data={
                "grant_type": "password",
                "username": TEST_USER,
                "password": "definitely_wrong_password_123",
            },
        )
        assert rs.status_code == 401, rs.text
        logger.info("test_wrong_password_rejected passed")

    @pytest.mark.user
    async def test_update_invalid_homepage_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """3. 更新非法 homepage URL 应返回 400"""
        client, token, token_type = admin_login

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "homepage": "not-a-valid-url"},
        )
        assert rs.status_code == 400, rs.text
        logger.info("test_update_invalid_homepage_rejected passed")

    @pytest.mark.user
    async def test_update_valid_homepage(self, admin_login: Tuple[AsyncClient, str, str]):
        """4. 更新合法 homepage URL 应成功并能读回"""
        client, token, token_type = admin_login
        valid_url = "https://github.com/kawaru-test"

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "homepage": valid_url},
        )
        assert rs.status_code == 200, rs.text

        rs = await client.get("/user", headers={"Authorization": f"{token_type.title()} {token}"})
        assert rs.status_code == 200, rs.text
        assert rs.json()["homepage"] == valid_url
        logger.info("test_update_valid_homepage passed")

    @pytest.mark.user
    async def test_update_invalid_position_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """5. 更新非法 position 应返回 400"""
        client, token, token_type = admin_login

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "position": "Galactic Emperor"},
        )
        assert rs.status_code == 400, rs.text
        logger.info("test_update_invalid_position_rejected passed")

    @pytest.mark.user
    async def test_update_invalid_region_rejected(self, admin_login: Tuple[AsyncClient, str, str]):
        """6. 更新非法 region 应返回 400"""
        client, token, token_type = admin_login

        rs = await client.post(
            "/user_change",
            headers={"Authorization": f"{token_type.title()} {token}"},
            json={"username": TEST_USER, "region": "Narnia"},
        )
        assert rs.status_code == 400, rs.text
        logger.info("test_update_invalid_region_rejected passed")

    @pytest.mark.user
    async def test_logout(self, admin_login: Tuple[AsyncClient, str, str]):
        """验证登出后 token 失效"""
        client, token, token_type = admin_login

        rs = await client.post("/logout", headers={"Authorization": f"{token_type.title()} {token}"})
        assert rs.status_code == 200, rs.text

        rs = await client.get("/user", headers={"Authorization": f"{token_type.title()} {token}"})
        assert rs.status_code in (401, 403), f"expected 401/403 after logout, got {rs.status_code}"
        logger.info("test_logout: token invalidated as expected")

    @pytest.mark.user
    async def test_no_token_rejected(self, async_client: AsyncClient):
        """1. 无 token 访问受保护接口应返回 401"""
        rs = await async_client.get("/user")
        assert rs.status_code == 401, rs.text
        logger.info("test_no_token_rejected passed")


class TestUserBenchmark:
    """
    使用 pytest-benchmark 测量用户接口端到端响应时间。

    pytest-benchmark 本身是同步的，异步请求需要通过 asyncio.get_event_loop().run_until_complete
    或 anyio.from_thread.run_sync 包裹。这里使用标准库 asyncio 方案。
    运行: uv run pytest -m benchmark --benchmark-autosave
    """
    if BASE_URL is None:
        raise RuntimeError("Environment variable BASE_URL is required")
    @pytest.mark.benchmark
    def test_benchmark_login(self, benchmark):
        """benchmark: POST /login 响应时间"""

        async def _do_login():
            async with AsyncClient(base_url=BASE_URL, timeout=60) as client: # type:ignore
                rs = await client.post(
                    "/login",
                    data={"grant_type": "password", "username": TEST_USER, "password": TEST_PWD},
                )
                assert rs.status_code == 200, rs.text
                return rs.json()

        result = benchmark(lambda: asyncio.run(_do_login()))
        logger.info(f"benchmark login result: {result}")

    @pytest.mark.benchmark
    def test_benchmark_get_user(self, benchmark, admin_login):
        """benchmark: GET /user 响应时间"""
        client, token, token_type = admin_login

        async def _do_get():
            rs = await client.get(
                "/user",
                headers={"Authorization": f"{token_type.title()} {token}"},
            )
            assert rs.status_code == 200, rs.text
            return rs.json()

        result = benchmark(lambda: asyncio.run(_do_get()))
        logger.info(f"benchmark get_user result: {result}")

    @pytest.mark.benchmark
    def test_benchmark_update_user(self, benchmark, admin_login):
        """benchmark: POST /user_change 响应时间"""
        client, token, token_type = admin_login

        async def _do_update():
            rs = await client.post(
                "/user_change",
                headers={"Authorization": f"{token_type.title()} {token}"},
                json={"username": TEST_USER, "institution": "Benchmark University"},
            )
            assert rs.status_code == 200, rs.text
            return rs.json()

        result = benchmark(lambda: asyncio.run(_do_update()))
        logger.info(f"benchmark update_user result: {result}")
