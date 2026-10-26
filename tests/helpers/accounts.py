"""
测试账号工具：注册（直签验证码，不发邮件）/ 登录 / 清扫残留账号。

注册码签发依赖测试进程与服务端连同一个 Redis（CI 在 app 容器内运行 pytest，
满足该条件），见 tests/helpers/totp.py::issue_email_code。

删除场景 ③④（tests/test_deletion_user_cleanup.py / test_deletion_failed_experiments.py）
共用；注册请求体沿用 test_password_reset._signup_dedicated_account 的字段集。
"""
from __future__ import annotations

from httpx import AsyncClient
from sqlmodel import select

from database.db import get_standalone_async_session
from models.user_models import UserInDB
from tools.logger import get_logger

from .totp import issue_email_code

logger = get_logger("test.helpers.accounts")


async def signup_and_login(
    client: AsyncClient,
    *,
    username: str,
    email: str,
    password: str,
) -> dict[str, str]:
    """注册一个普通用户并登录，返回 Authorization 请求头。"""
    code = await issue_email_code(email)
    signup_body = {
        "username": username,
        "email": email,
        "password": password,
        "active": True,
        "institution": "Deletion Test",
        "position": "Researcher",
        "research_field": "Massflow",
        "region": "CN",
        "orcid": None,
        "homepage": None,
    }
    rs = await client.post("/user_signup", json={**signup_body, "verify_code": code})
    assert rs.status_code == 200, f"signup failed: {rs.status_code} {rs.text[:300]}"

    rs = await client.post(
        "/login",
        data={"grant_type": "password", "username": username, "password": password},
    )
    assert rs.status_code == 200, f"login failed: {rs.status_code} {rs.text[:300]}"
    data = rs.json()
    logger.info("signup_and_login ok: username=%s", username)
    return {"Authorization": f"{data['token_type'].title()} {data['access_token']}"}


async def sweep_users_by_prefix(username_prefix: str) -> int:
    """硬删用户名以 prefix 开头的用户行（测试专用账号前缀，不碰真实数据）。

    上次运行中断残留的账号若不清理，重跑会撞用户名/邮箱唯一约束；
    user 行删除按 FK 级联清掉 UserFileLink 与 run 行。
    """
    async with get_standalone_async_session() as session:
        rows = (await session.exec(
            select(UserInDB).where(UserInDB.username.startswith(username_prefix))
        )).all()
        for row in rows:
            await session.delete(row)
        await session.commit()
    if rows:
        logger.info("sweep: hard-deleted %d leftover user(s) with prefix %s", len(rows), username_prefix)
    return len(rows)


async def get_user_id_by_username(username: str) -> int | None:
    """按用户名查内部 user id（原始查询，不按 active 过滤）。"""
    async with get_standalone_async_session() as session:
        row = (await session.exec(
            select(UserInDB).where(UserInDB.username == username)
        )).first()
    return row.id if row else None


async def get_user_active(username: str) -> bool | None:
    """按用户名取 active：None = 行不存在；False = 已被管理员标记删除（见 api/user_api.del_user）。"""
    async with get_standalone_async_session() as session:
        row = (await session.exec(
            select(UserInDB).where(UserInDB.username == username)
        )).first()
    return row.active if row else None
