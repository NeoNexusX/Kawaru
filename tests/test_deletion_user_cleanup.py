"""
删除状态位场景 ③：管理员删用户善后（真实上传 → 直插 run → 删用户 → 断言收敛）

test_admin_delete_user_full         [deletion]  建用户 → 真实上传链造独占文件 →
                                                直插 run + 真实 out_path 对象 →
                                                DELETE /user_delete/{user_id} →
                                                断言用户/文件/run 全链路收敛
test_admin_delete_user_shared_file  [deletion]  同上但文件与 admin 共有 →
                                                断言文件存活（files_unlinked 分支）

验证删除状态位改造（docs/zh/删除架构现状说明.md 场景 3）的核心承诺：
  ★1 用户行删除（同步，API 返回前已 commit）
  ★2 用户独占文件行由 clean_user_all_task 收敛删除（三件套：登记 DELETING →
     worker 删 OSS → finalize 删行）
  ★3 run 输出从 OSS 删净 —— 场景 3 快照时机的关键验收：run.user_id 外键
     ondelete=CASCADE，user 行删除时 run 行随之消失，out_path 事后查不到，
     只能来自 del_user 在删行前的预收集（api/user_api.del_user）合入文件删除快照。
     这条断言失败即预收集链路断裂。
  ★4 共有文件分支：文件仍有其他持有者 → 不进入删除流程（files_unlinked），
     文件行/对象全部保留，run 输出依赖 bucket 生命周期回收（方案 §4/§7 已接受的取舍）

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
from .helpers.accounts import get_user_id_by_username, signup_and_login, sweep_users_by_prefix
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

    # 2. 直插用户名下的 COMPLETED run + 真实 out_path 对象
    #    （user 行删除时该行随级联消失，out_path 只有预收集能救——★3 的靶子）
    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=user_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=4,
        wire_default_run=False,
    )

    # 3. 管理员删除用户（del_user 内 wait_for_result 等登记任务完成：
    #    返回时 user 行已删、文件已登记 DELETING、删除任务已投递）
    rs = await admin_client.delete(f"/user_delete/{user_id}", headers=admin_auth)
    assert rs.status_code == 200, rs.text
    assert rs.json() == {"user_id": user_id, "status": "deleted"}, rs.text
    logger.info("admin deleted user_id=%s", user_id)

    # 4. ★1 用户行消失
    assert await get_user_id_by_username(username) is None, "user row must be deleted"

    # 5. ★2 文件行由 clean_user_all_task 收敛删除
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"file_id={up['file_id']}",
    )
    logger.info("★2 file row finalized")

    # 6. ★3 OSS 删净：主 zip + origin + run 输出。
    #    run 行在快照收集前已被级联删除，out_path 只能来自预收集传参合入快照——
    #    这条断言是场景 3 快照时机改造的核心验收
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"], *run_info["keys"]):
        assert not await OssStorage.check_file_exist(key), f"object still exists: {key}"
    logger.info("★3 all objects gone (main zip + origin + %d run outputs via pre-collected snapshot)",
                len(run_info["keys"]))

    # 7. run 行也应已随用户级联消失（防御性确认，非删除架构行为）
    async with get_standalone_async_session() as session:
        run_row = (await session.exec(
            select(ExperimentProcessRunInDB).where(
                ExperimentProcessRunInDB.id == run_info["run_id"])
        )).first()
    assert run_row is None, "run row must be cascade-deleted with user row"
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

    # admin 加为共同持有者（真实共享入口不在删除链路内，直插 link 等价：
    # mark_file_for_deletion(None) 只按 UserFileLink 行数判断是否有其他持有者）
    async with get_standalone_async_session() as session:
        session.add(UserFileLink(user_id=admin_id, file_id=up["file_id"]))
        await session.commit()

    # 用户名下的 run（其输出在用户删除后无处合入快照，按文档取舍留 bucket 生命周期）
    run_info = await insert_completed_run(
        file_id=up["file_id"],
        user_id=user_id,
        output_size=RUN_OUTPUT_SIZE,
        object_count=4,
        wire_default_run=False,
    )

    rs = await admin_client.delete(f"/user_delete/{user_id}", headers=admin_auth)
    assert rs.status_code == 200, rs.text
    assert await get_user_id_by_username(username) is None, "user row must be deleted"

    # ★4 文件仍有 admin 持有：不进入删除流程，行保留、状态不变
    file_row = await _file_row(up["file_id"])
    assert file_row is not None, "shared file row must survive user deletion"
    assert file_row.status == FileStatus.COMPLETED, f"file status changed: {file_row.status}"
    logger.info("★4 shared file survived (status=%s)", file_row.status)

    # admin 仍可正常访问该文件
    rs = await admin_client.get(f"/files/{up['public_id']}/metadata", headers=admin_auth)
    assert rs.status_code == 200, f"admin must still access shared file: {rs.text}"

    # 对象全部保留：主 zip / origin（文件存活），run 输出（快照未投递，
    # 方案 §4/§7 明确接受由 bucket 生命周期回收的取舍）
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"], *run_info["keys"]):
        assert await OssStorage.check_file_exist(key), f"object must survive: {key}"

    # 收尾：admin 作为持有者走正常删除（成功路径自清；中断残留由下次运行开头 sweep 兜底）
    rs = await admin_client.delete(f"/files/{up['public_id']}", headers=admin_auth)
    assert rs.status_code == 200, f"shared file cleanup delete failed: {rs.text}"
    await wait_db_row_gone(
        FileInDB, FileInDB.file_id == up["file_id"],
        timeout=CONVERGE_MAX_WAIT, desc=f"cleanup file_id={up['file_id']}",
    )
    for key in (up["oss_path"], up["imzml_path"], up["ibd_path"]):
        assert not await OssStorage.check_file_exist(key), f"cleanup object still exists: {key}"
    logger.info("test_admin_delete_user_shared_file passed: file_id=%s cleaned up", up["file_id"])
