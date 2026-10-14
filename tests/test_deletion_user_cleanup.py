"""
删除状态位场景 ③：管理员删用户善后（真实上传 → 直插 run → 删用户 → 断言收敛）

test_admin_delete_user_full         [deletion]  建用户 → 真实上传链造独占文件 →
                                                直插 run + 真实 out_path 对象 →
                                                DELETE /user_delete/{user_id} →
                                                断言用户/文件/run 全链路收敛
test_admin_delete_user_shared_file  [deletion]  同上但文件与 admin 共有 →
                                                断言文件存活（files_unlinked 分支）

验证删除状态位改造（docs/zh/删除架构现状说明.md 场景 3）的核心承诺：
  ★1 用户行**软删除**（同步，API 返回前已 commit）：只置 `active=False`，行保留
  ★2 用户独占文件行由 clean_user_all_task 收敛删除（登记 DELETING → worker 删 OSS
     → finalize 删行）
  ★3 run 输出从 OSS 删净 —— 软删除后 run 行不随 user 级联消失，文件快照按
     source_file_id 反查即可收全（不再需要预收集路径）
  ★4 共有文件分支：文件仍有其他持有者 → 不进入删除流程（files_unlinked），被删用户
     的关联被解除，文件行/对象全部保留。该用户在这份共享文件上的 run 也随之保留
     （源文件不删 → run 不删），测试**显式断言这批对象残留**——它会在该共享文件
     日后被删时随快照一并清理，不是永久泄漏

与真实链路的取舍同场景 ②：文件走真实上传链（helpers/uploads.upload_completed_file），
run 直插（真实算法无法对随机数据产出 run）；被删用户经 /user_signup 真实注册
（helpers/accounts.signup_and_login，验证码直签不发邮件）。

前置（同 test_deletion_flow）：
  - 服务器 + celery worker 在跑（worker 不在则文件行卡 DELETING，★2 超时）
  - OSS_ACCESS_KEY_ID/SECRET 可用
  - 数据库已迁移到 head（含 delete_marked_at 列）

运行方式:
    uv run pytest tests/test_deletion_user_cleanup.py -v
    uv run pytest -m deletion -v
"""
from __future__ import annotations

import secrets
from typing import Tuple

import pytest
from httpx import AsyncClient
from sqlmodel import select

from database.db import get_standalone_async_session
from models.file_models import FileInDB, FileStatus
from models.link_models import UserFileLink
from models.processing_models import ExperimentProcessRunInDB
from storage.oss.oss_storage import OssStorage
from .conftest import TEST_USER, logger
from .helpers.accounts import (
    get_user_active,
    get_user_id_by_username,
    signup_and_login,
    sweep_users_by_prefix,
)
from .helpers.deletion import insert_completed_run, sweep_file_rows_by_prefix, wait_db_row_gone
from .helpers.uploads import upload_completed_file

# 专用前缀（文件名/用户名），各测试开头按前缀清扫上次运行残留
DELETION_USER_FILENAME = "test_del_user_file"
DELETED_USER_PREFIX = "deluser_tgt"
DELETED_USER_PASSWORD = "DelUser123456!"
DELETION_IMZML_SIZE = 256 * 1024
DELETION_IBD_SIZE = 256 * 1024
RUN_OUTPUT_SIZE = 4096
CONVERGE_MAX_WAIT = 30


async def _signup_doomed_user(client: AsyncClient) -> tuple[str, dict[str, str]]:
    """注册一个即将被删除的普通用户，返回 (username, auth 头)。"""
    username = f"{DELETED_USER_PREFIX}_{secrets.token_hex(3)}"
    email = f"{username}@example.com"
    auth = await signup_and_login(
        client, username=username, email=email, password=DELETED_USER_PASSWORD,
    )
    return username, auth


async def _file_row(file_id: int) -> FileInDB | None:
    async with get_standalone_async_session() as session:
        return (await session.exec(
            select(FileInDB).where(FileInDB.file_id == file_id)
        )).first()


@pytest.mark.deletion
async def test_admin_delete_user_full(
    async_client: AsyncClient,
    admin_login: Tuple[AsyncClient, str, str],
):
    """管理员删除持有独占文件 + run 的用户：全链路收敛（见模块 docstring ★1-★3）"""
    client = async_client
    admin_client, admin_token, admin_token_type = admin_login
    admin_auth = {"Authorization": f"{admin_token_type.title()} {admin_token}"}

    # 0. 清扫上次运行残留
    await sweep_file_rows_by_prefix(DELETION_USER_FILENAME)
    await sweep_users_by_prefix(DELETED_USER_PREFIX)

    # 1. 建被删用户 + 真实上传链造其独占文件
    username, user_auth = await _signup_doomed_user(client)
    up = await upload_completed_file(
        client, user_auth,
        filename=DELETION_USER_FILENAME,
        imzml_size=DELETION_IMZML_SIZE,
        ibd_size=DELETION_IBD_SIZE,
    )
    user_id = await get_user_id_by_username(username)
    assert user_id is not None, f"signup succeeded but user row missing: {username}"

    # 2. 直插用户名下的 COMPLETED run + 真实 out_path 对象（★3 的靶子）
    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=user_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=4,
        wire_default_run=False,
    )

    # 3. 管理员删除用户（只置 active=False 即返回；文件清理由后台任务异步完成，
    #    故下方一律用 wait_db_row_gone 轮询收敛）
    rs = await admin_client.delete(f"/user_delete/{user_id}", headers=admin_auth)
    assert rs.status_code == 200, rs.text
    assert rs.json() == {"user_id": user_id, "status": "deleted"}, rs.text
    logger.info("admin deleted user_id=%s", user_id)

    # 4. ★1 软删除：行仍在，但 active 已置 False
    assert await get_user_active(username) is False, "user must be marked deleted (active=False)"
    assert await get_user_id_by_username(username) is not None, "user row must be kept (soft delete)"
    logger.info("★1 user soft-deleted (active=False, row kept)")

    # 5. ★2 文件行由 clean_user_all_task 收敛删除
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"file_id={up['file_id']}",
    )
    logger.info("★2 file row finalized")

    # 6. ★3 OSS 删净：主 zip + origin + run 输出（out_path 由文件快照反查收全）
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"], *run_info["keys"]):
        assert not await OssStorage.check_file_exist(key), f"object still exists: {key}"
    logger.info("★3 all objects gone (main zip + origin + %d run outputs via file snapshot)",
                len(run_info["keys"]))

    # 7. run 行随文件行级联消失（源文件被删 → source_file_id 级联）
    async with get_standalone_async_session() as session:
        run_row = (await session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.id == run_info["run_id"])
        )).first()
    assert run_row is None, "run row must be cascade-deleted with file row"
    logger.info("test_admin_delete_user_full passed: user_id=%s file_id=%s run_id=%s",
                user_id, up["file_id"], run_info["run_id"])


@pytest.mark.deletion
async def test_admin_delete_user_shared_file(
    async_client: AsyncClient,
    admin_login: Tuple[AsyncClient, str, str],
):
    """管理员删除与 admin 共有文件的用户：文件存活（files_unlinked 分支，★4）"""
    client = async_client
    admin_client, admin_token, admin_token_type = admin_login
    admin_auth = {"Authorization": f"{admin_token_type.title()} {admin_token}"}

    await sweep_file_rows_by_prefix(DELETION_USER_FILENAME)
    await sweep_users_by_prefix(DELETED_USER_PREFIX)

    username, user_auth = await _signup_doomed_user(client)
    up = await upload_completed_file(
        client, user_auth,
        filename=DELETION_USER_FILENAME,
        imzml_size=DELETION_IMZML_SIZE,
        ibd_size=DELETION_IBD_SIZE,
    )
    user_id = await get_user_id_by_username(username)
    assert user_id is not None
    admin_id = await get_user_id_by_username(TEST_USER)
    assert admin_id is not None

    # admin 加为共同持有者（直插 link 等价：判定只按 UserFileLink 行数）
    async with get_standalone_async_session() as session:
        session.add(UserFileLink(user_id=admin_id, file_id=up["file_id"]))
        await session.commit()

    # 用户名下的 run：文件是共享的，本次清理不删文件 → run 行与输出都保留（见 ★4b）
    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=user_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=4,
        wire_default_run=False,
    )

    rs = await admin_client.delete(f"/user_delete/{user_id}", headers=admin_auth)
    assert rs.status_code == 200, rs.text
    assert await get_user_active(username) is False, "user must be marked deleted"

    # ★4 文件仍有 admin 持有：不进入删除流程，行保留、状态不变
    file_row = await _file_row(up["file_id"])
    assert file_row is not None, "shared file row must survive user deletion"
    assert file_row.status == FileStatus.COMPLETED, f"file status changed: {file_row.status}"
    logger.info("★4 shared file survived (status=%s)", file_row.status)

    # admin 仍可正常访问该文件
    rs = await admin_client.get(f"/files/{up['public_id']}/metadata", headers=admin_auth)
    assert rs.status_code == 200, f"admin must still access shared file: {rs.text}"

    # 文件存活 → 主 zip / origin 对象必须保留
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"]):
        assert await OssStorage.check_file_exist(key), f"object must survive: {key}"

    # 收尾：admin 作为持有者走正常删除
    rs = await admin_client.delete(f"/files/{up['public_id']}", headers=admin_auth)
    assert rs.status_code == 200, f"shared file cleanup delete failed: {rs.text}"
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"cleanup file_id={up['file_id']}",
    )
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"]):
        assert not await OssStorage.check_file_exist(key), f"cleanup object still exists: {key}"

    # ★4b 残留 run 的输出随共享文件被删而一并清理：软删除后 run 行不随 user 级联消失，
    # 仍挂在源文件上，故文件删除快照会收走它的 out_path。
    # （硬删时 run 行已随 user 消失，输出只能等 bucket 生命周期——旧版测试正是断言那条泄漏）
    for key in run_info["keys"]:
        assert not await OssStorage.check_file_exist(key), (
            f"leftover run output should be cleaned with the shared file: {key}"
        )
    logger.info("★4b leftover run outputs cleaned with the shared file (%d object(s))",
                len(run_info["keys"]))
    logger.info("test_admin_delete_user_shared_file passed: file_id=%s cleaned up", up["file_id"])
