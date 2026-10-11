"""
管理员告警邮件通知测试（删除架构现状说明 §8 落地的前两例）

覆盖两处典型失败点的通知逻辑：
  例① OSS 删除失败 → DELETE_FAILED（background/file_background._mark_delete_failed）：
     告警只挂「DELETING → DELETE_FAILED」跃迁一次，重投不重发（跃迁即去重）；
     file / run 两种 kind；行已消失 / 未知 kind 静默
  例② FC 触发失败（invoke_fc 返回 None，background/processing_background 的
     5 个 trigger 任务）：失败分支告警一次（fc 名 + run_id/file_id 正确），
     成功分支静默

测试策略（不发真实邮件、不依赖 server / worker / OSS）：
  - 投递侧全程 mock：payload 组测试替换 api_helpers.admin_notify.send_smtp_email_task；
    例① patch background.file_background.notify_delete_failed；
    例② patch background.processing_background.invoke_fc / notify_fc_trigger_failed
  - 例① 的状态跃迁用本地库真实行验证（kawaru_bear）：直插 file / origin / run 行，
    OSS 对象从不创建，收尾按文件名前缀硬删行（FK 级联清 origin / run），
    直接 await _mark_delete_failed（普通 async 函数）
  - 例② 直接 await trigger_fc_*.run(...)：CeleryAsyncTask.run 即原协程函数，
    绕开 __call__ 的 asyncio.run，避免在测试事件循环内嵌套起 loop

运行方式:
    uv run pytest tests/test_admin_notify_unit.py -v
"""
from __future__ import annotations

import secrets
from types import SimpleNamespace

import pytest
from sqlmodel import select

import api_helpers.admin_notify as admin_notify
import background.file_background as file_background
import background.processing_background as processing_background
from api_helpers.admin_notify import notify_delete_failed, notify_fc_trigger_failed
from background.file_background import _mark_delete_failed
from boot.config import get_config
from database.db import get_standalone_async_session
from models.experiment_models import ExperimentStatus, OriginImzMLInDB
from models.file_models import FileInDB, FileStatus
from models.processing_models import ExperimentProcessRunInDB, ProcessingStatus
from models.user_models import UserInDB
from tools import utc_time
from .conftest import logger

# DB 夹具专用文件名前缀（收尾按前缀硬删；与其他测试组隔离）
ADMIN_NOTIFY_FILENAME_PREFIX = "test_admin_notify_del"


class _DelayCapture:
    """send_smtp_email_task 的桩：记录 .delay 收到的 payload，可注入投递异常。"""

    def __init__(self, raise_on_delay: Exception | None = None):
        self.payloads: list[dict] = []
        self.raise_on_delay = raise_on_delay

    def delay(self, payload: dict):
        if self.raise_on_delay is not None:
            raise self.raise_on_delay
        self.payloads.append(payload)
        return SimpleNamespace(id="stub-task-id")


def _record_notify(calls: list[tuple[str, int, str | None]]):
    """notify_delete_failed 的桩：记录 (kind, resource_id, public_id) 后返回 True。"""
    def fake(kind: str, resource_id: int, *, public_id: str | None = None) -> bool:
        calls.append((kind, resource_id, public_id))
        return True
    return fake


async def _hard_delete_rows_by_prefix() -> int:
    """硬删本测试前缀的 file 行；FK ON DELETE CASCADE 级联清 origin / run / link。

    与 helpers/deletion.sweep_file_rows_by_prefix 的区别：本组测试从不创建 OSS
    对象，无需收集删除快照、也无需删 OSS，纯 DB 清理即可。
    """
    async with get_standalone_async_session() as session:
        rows = (await session.exec(
            select(FileInDB).where(FileInDB.filename.startswith(ADMIN_NOTIFY_FILENAME_PREFIX))
        )).all()
        for row in rows:
            await session.delete(row)
        await session.commit()
    if rows:
        logger.info("sweep: hard-deleted %d leftover row(s) with prefix %s",
                    len(rows), ADMIN_NOTIFY_FILENAME_PREFIX)
    return len(rows)


@pytest.fixture(name="_sweep_admin_notify_rows")
async def _sweep_admin_notify_rows_fixture():
    """DB 用例前后各清扫一次残留（中断残留由下次运行开头兜底）"""
    await _hard_delete_rows_by_prefix()
    yield
    await _hard_delete_rows_by_prefix()


class TestAdminAlertPayload:
    """告警 payload 构造与 fire-and-forget 投递语义"""

    def test_delete_failed_payload_uses_admin_email(self, monkeypatch):
        stub = _DelayCapture()
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        assert notify_delete_failed("file", 7) is True
        assert len(stub.payloads) == 1
        payload = stub.payloads[0]
        config = get_config()
        # 收件人与 SMTP 连接参数必须全部取自 config（密码除外，任务侧走环境变量）
        assert payload["email"] == config.admin_info.email
        assert payload["smtp_server"] == config.email.smtp_server
        assert payload["smtp_port"] == int(config.email.smtp_port)
        assert payload["smtp_username"] == config.email.smtp_username
        assert payload["smtp_from_email"] == config.email.smtp_from_email
        assert "file#7" in payload["subject"] and "DELETE_FAILED" in payload["subject"]
        assert "file" in payload["text_content"] and "\r\n" in payload["text_content"]

    def test_delete_failed_payload_carries_both_file_ids(self, monkeypatch):
        """file 类告警必须同时给出 file_id（对库/对日志）与 public_id（对接口）。

        对外删除接口是 DELETE /files/{public_id}，只给内部 file_id 管理员无法照着重投。
        """
        stub = _DelayCapture()
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        assert notify_delete_failed("file", 42, public_id="Ab3xY9kLmN0pQrSt") is True
        payload = stub.payloads[0]
        assert "file#42" in payload["subject"]
        assert "Ab3xY9kLmN0pQrSt" in payload["subject"]
        body = payload["text_content"]
        assert "42" in body and "Ab3xY9kLmN0pQrSt" in body
        # 可直接复制的重投命令
        assert "DELETE /files/Ab3xY9kLmN0pQrSt" in body

    def test_delete_failed_payload_run_uses_run_id_as_api_path(self, monkeypatch):
        """run 类：对外标识就是 run_id，不给 public_id 也不影响可操作性"""
        stub = _DelayCapture()
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        assert notify_delete_failed("run", 5) is True
        body = stub.payloads[0]["text_content"]
        assert "DELETE /processes/5" in body

    def test_fc_trigger_payload_contains_fc_name_and_target(self, monkeypatch):
        stub = _DelayCapture()
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        assert notify_fc_trigger_failed("fc-tic", file_id=9) is True
        payload = stub.payloads[0]
        assert "fc-tic" in payload["subject"]
        assert "file_id=9" in payload["subject"]

    def test_dispatch_failure_never_raises(self, monkeypatch):
        """broker 不可达等投递失败只返回 False，异常不得上抛影响主流程"""
        stub = _DelayCapture(raise_on_delay=RuntimeError("broker down"))
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        assert notify_delete_failed("run", 3) is False
        assert notify_fc_trigger_failed("fc-algorithm", run_id=3) is False

    def test_missing_recipient_skips_dispatch(self, monkeypatch):
        """admin_info.email 未配置（空串）时跳过投递"""
        stub = _DelayCapture()
        fake_config = SimpleNamespace(
            admin_info=SimpleNamespace(email=""),
            email=get_config().email,
            server=SimpleNamespace(name="TestApp"),
        )
        monkeypatch.setattr(admin_notify, "send_smtp_email_task", stub)
        monkeypatch.setattr(admin_notify, "get_config", lambda: fake_config)
        assert notify_delete_failed("file", 7) is False
        assert stub.payloads == []


@pytest.mark.usefixtures("_sweep_admin_notify_rows")
class TestDeleteFailedTransition:
    """例①：_mark_delete_failed 的跃迁语义，告警挂跃迁上去重（本地库真实行）"""

    async def _insert_file(self, status: FileStatus) -> int:
        suffix = secrets.token_hex(4)
        verify_code = secrets.token_hex(16)
        async with get_standalone_async_session() as session:
            row = FileInDB(
                filename=f"{ADMIN_NOTIFY_FILENAME_PREFIX}_{suffix}",
                path=f"data/{verify_code}/exp_{suffix}.zip",
                status=status,
                storage_type="oss",
                size=1024,
                file_verify_code=verify_code,
                file_type="zip",
                experiment_type="imzML",
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            file_id = row.file_id
        assert file_id is not None
        logger.info("fixture: file_id=%s status=%s", file_id, status)
        return file_id

    async def _insert_run(self, file_id: int, status: ProcessingStatus) -> int:
        """直插一条挂在 file_id 上的 run（experiment_id FK 非空，需先插 origin 行）"""
        async with get_standalone_async_session() as session:
            user = (await session.exec(select(UserInDB).order_by(UserInDB.id).limit(1))).first()
            assert user is not None and user.id is not None, (
                "no user row in local DB; cannot build run fixture"
            )
            user_id = user.id
            suffix = secrets.token_hex(4)
            origin = OriginImzMLInDB(
                experiment_name=f"{ADMIN_NOTIFY_FILENAME_PREFIX}_{suffix}",
                source_file_id=file_id,
                status=ExperimentStatus.FAILED,
                error_message="admin-notify test fixture",
                imzml_path=f"origin/{suffix}/{file_id}/t.imzML",
                imzml_size=1024,
                ibd_path=f"origin/{suffix}/{file_id}/t.ibd",
                ibd_size=1024,
                created_at=utc_time(),
            )
            session.add(origin)
            await session.commit()
            await session.refresh(origin)
            origin_id = origin.id
            assert origin_id is not None
            run = ExperimentProcessRunInDB(
                experiment_id=origin_id,
                source_file_id=file_id,
                user_id=user_id,
                status=status,
            )
            session.add(run)
            await session.commit()
            await session.refresh(run)
            run_id = run.id
        assert run_id is not None
        logger.info("fixture: run_id=%s status=%s", run_id, status)
        return run_id

    async def _read_file_status(self, file_id: int) -> FileStatus | None:
        async with get_standalone_async_session() as session:
            row = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )).first()
        return row.status if row is not None else None

    async def _read_public_id(self, file_id: int) -> str:
        """告警必须带上的对外标识：断言 _mark_delete_failed 从 file 行取到了它"""
        async with get_standalone_async_session() as session:
            row = (await session.exec(
                select(FileInDB).where(FileInDB.file_id == file_id)
            )).first()
        assert row is not None
        return row.public_id

    async def _read_run_status(self, run_id: int) -> ProcessingStatus | None:
        async with get_standalone_async_session() as session:
            row = (await session.exec(
                select(ExperimentProcessRunInDB).where(ExperimentProcessRunInDB.id == run_id)
            )).first()
        return row.status if row is not None else None

    async def test_file_transition_notifies_once_and_flips_status(self, monkeypatch):
        file_id = await self._insert_file(FileStatus.DELETING)
        public_id = await self._read_public_id(file_id)
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        await _mark_delete_failed("file", file_id)
        # 跃迁：告警恰好一次（带上对外标识 public_id）+ 状态翻转为 DELETE_FAILED
        assert calls == [("file", file_id, public_id)]
        assert await self._read_file_status(file_id) == FileStatus.DELETE_FAILED

    async def test_redispatch_after_failure_does_not_renotify(self, monkeypatch):
        """§8 核心：重投时行已是 DELETE_FAILED，告警不重发（跃迁即去重）"""
        file_id = await self._insert_file(FileStatus.DELETING)
        public_id = await self._read_public_id(file_id)
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        # worker 重试链路：首次失败标记 + 两次重投再失败
        await _mark_delete_failed("file", file_id)
        await _mark_delete_failed("file", file_id)
        await _mark_delete_failed("file", file_id)
        assert calls == [("file", file_id, public_id)]
        assert await self._read_file_status(file_id) == FileStatus.DELETE_FAILED

    async def test_pre_failed_row_never_notifies(self, monkeypatch):
        """行直插即为 DELETE_FAILED（非本函数翻转）：幂等重置但不告警"""
        file_id = await self._insert_file(FileStatus.DELETE_FAILED)
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        await _mark_delete_failed("file", file_id)
        assert calls == []
        assert await self._read_file_status(file_id) == FileStatus.DELETE_FAILED

    async def test_missing_row_is_silent(self, monkeypatch):
        """行已被并发 finalize 删除：静默返回，不抛异常、不告警"""
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        await _mark_delete_failed("file", 987654321)
        assert calls == []

    async def test_unknown_kind_is_silent(self, monkeypatch):
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        await _mark_delete_failed("bogus", 1)
        assert calls == []

    async def test_run_transition_notifies_once_and_flips_status(self, monkeypatch):
        """kind=run 分支：查 run 表、按 ProcessingStatus 判定跃迁"""
        file_id = await self._insert_file(FileStatus.COMPLETED)  # 占位文件行，run 挂其上
        run_id = await self._insert_run(file_id, ProcessingStatus.DELETING)
        calls: list[tuple[str, int, str | None]] = []
        monkeypatch.setattr(file_background, "notify_delete_failed", _record_notify(calls))
        await _mark_delete_failed("run", run_id)
        # run 的对外标识就是 run_id，无需额外 public_id
        assert calls == [("run", run_id, None)]
        assert await self._read_run_status(run_id) == ProcessingStatus.DELETE_FAILED


class TestFCTriggerAlert:
    """例②：invoke_fc 返回 None → 5 个 trigger 任务失败分支各告警一次；成功分支静默"""

    async def _run_trigger(self, monkeypatch, task_name: str, kwargs: dict, invoke_result):
        """patch invoke_fc / notify_fc_trigger_failed 后直接驱动原协程，返回告警记录"""
        calls: list[tuple] = []

        async def fake_invoke_fc(*args, **kwargs):
            return invoke_result

        def fake_notify(fc_name: str, *, run_id=None, file_id=None):
            calls.append((fc_name, run_id, file_id))
            return True

        monkeypatch.setattr(processing_background, "invoke_fc", fake_invoke_fc)
        monkeypatch.setattr(processing_background, "notify_fc_trigger_failed", fake_notify)
        task = getattr(processing_background, task_name)
        await task.run(**kwargs)
        return calls

    @pytest.mark.parametrize("task_name,kwargs,expected", [
        ("trigger_fc_algorithm_task",
         dict(run_id=11, imzml_path="p.imzML", config_json="{}", output_path="out/"),
         ("fc-algorithm", 11, None)),
        ("trigger_fc_tic_task",
         dict(file_id=12, imzml_path="p.imzML", public_id="pub1"),
         ("fc-tic", None, 12)),
        ("trigger_fc_feature_task",
         dict(file_id=13, imzml_path="p.imzML", public_id="pub2"),
         ("fc-feature", None, 13)),
        ("trigger_fc_raw_convert_task",
         dict(run_id=14, imzml_path="p.imzML", ibd_path="p.ibd", output_path="out/"),
         ("fc-raw-convert", 14, None)),
        ("trigger_fc_clustering_task",
         dict(run_id=15, source_zarr_path="z/"),
         ("fc-clustering", 15, None)),
    ])
    async def test_trigger_failure_notifies_once(self, monkeypatch, task_name, kwargs, expected):
        calls = await self._run_trigger(monkeypatch, task_name, kwargs, invoke_result=None)
        assert calls == [expected]

    async def test_trigger_success_stays_silent(self, monkeypatch):
        """invoke_fc 成功返回 request_id：不告警"""
        calls = await self._run_trigger(
            monkeypatch, "trigger_fc_algorithm_task",
            dict(run_id=11, imzml_path="p.imzML", config_json="{}", output_path="out/"),
            invoke_result="req-success-1",
        )
        assert calls == []
