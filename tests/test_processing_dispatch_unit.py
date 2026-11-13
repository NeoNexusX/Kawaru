"""processing_background 调度层单测：不入队、不提交真实作业。"""
import inspect
from typing import ClassVar, get_args

import pytest
from pydantic import ValidationError

import background.processing_background as pb
from models.task_models import (
    AlgorithmParams,
    ComputeTaskParams,
    PreviewMode,
    PreviewParams,
)
from tools.ehpc_client import PREVIEW_MODES


def _capture_enqueue(monkeypatch):
    """拦截 celery 入队，返回捕获到的 (task, params) 列表。"""
    calls = []
    monkeypatch.setattr(pb.run_compute_task, "delay",
                        lambda task, params: calls.append((task, params)))
    return calls


def test_preview_mode_literal_matches_submitter():
    """模型里的 mode 字面量与 ehpc_client 的校验常量保持一致。"""
    assert set(get_args(PreviewMode)) == set(PREVIEW_MODES)


def test_registries_stay_in_sync():
    """后端注册表必须覆盖全部载荷模型任务键（_ID_FIELDS 由模型派生，是全量键集）。"""
    tasks = set(pb._ID_FIELDS)  # pylint: disable=protected-access
    backends = set(pb._EHPC_SUBMITTERS) | set(pb._FC_ENDPOINTS)  # pylint: disable=protected-access
    assert tasks == backends


def test_submitters_cover_model_fields():
    """提交器签名必须覆盖对应载荷模型的全部字段（含重复 task 键检查）。

    载荷模型与提交器签名没有静态关联（无类型 Callable），漂移原本只能在
    Worker 运行时以 TypeError 暴露，且被 _execute 的宽泛 except 误报成
    E-HPC 触发失败——对齐检查放测试期（与 test_registries_stay_in_sync 同类）。
    """
    models = get_args(ComputeTaskParams)
    # 两个模型若共用同一 task 键，dict 派生会静默少一项
    assert len(pb._EHPC_SUBMITTERS) == len(models)  # pylint: disable=protected-access
    for model in models:
        submitter = pb._EHPC_SUBMITTERS[model.task]  # pylint: disable=protected-access
        missing = set(model.model_fields) - set(inspect.signature(submitter).parameters)
        assert not missing, (
            f"submitter for {model.task!r} lacks params {sorted(missing)}: "
            "payload model and submitter signature drifted"
        )


def test_dispatch_enqueues_validated_task(monkeypatch):
    """dispatch 接收 API 构造好的载荷模型：归一化后的参数入队。"""
    calls = _capture_enqueue(monkeypatch)

    pb.dispatch(AlgorithmParams(
        run_id=11, imzml_path="origin/a.imzML", config_json="{}",
        output_path="processed/run_11.zarr", metadata=None, input_bytes=123,
    ))

    assert calls == [("algorithm", {
        "run_id": 11, "imzml_path": "origin/a.imzML", "config_json": "{}",
        "output_path": "processed/run_11.zarr", "metadata": {}, "input_bytes": 123,
    })]


def test_model_rejects_unknown_field():
    """拼错的参数名在 API 构造载荷时即报错（extra=forbid）。"""
    with pytest.raises(ValidationError, match="extra_forbidden"):
        AlgorithmParams(run_id=1, imzml_path="a", config_json="{}",
                        output_path="out/", typo_field=1)


def test_model_rejects_invalid_preview_mode():
    """preview 的 mode 由模型约束为 feature/tic。"""
    with pytest.raises(ValidationError, match="mode"):
        PreviewParams(file_id=1, imzml_path="p", image_dest="images/p/", mode="bad")


def test_dispatch_without_backend_raises(monkeypatch):
    """载荷模型存在但没登记后端：dispatch 拒绝入队。"""
    class _OrphanParams(AlgorithmParams):
        task: ClassVar[str] = "orphan"

    calls = _capture_enqueue(monkeypatch)

    with pytest.raises(ValueError, match="no backend registered"):
        pb.dispatch(_OrphanParams(run_id=1, imzml_path="a", config_json="{}", output_path="out/"))
    assert calls == []


async def test_execute_ehpc_success(monkeypatch):
    """Worker 侧 _execute 直接把入队载荷透传给提交器（不再重复校验）。"""
    calls = []

    async def fake_submit(**kwargs):
        calls.append(kwargs)
        return "job-1"

    notified = []
    monkeypatch.setattr(pb, "notify_fc_trigger_failed", lambda *a, **k: notified.append((a, k)))
    monkeypatch.setitem(pb._EHPC_SUBMITTERS, "clustering", fake_submit)  # pylint: disable=protected-access

    result = await pb._execute(  # pylint: disable=protected-access
        "clustering", run_id=21, source_zarr_path="processed/run.zarr", input_bytes=3 * 2**30,
    )

    assert result == "job-1"
    assert calls == [{
        "run_id": 21, "source_zarr_path": "processed/run.zarr", "input_bytes": 3 * 2**30,
    }]
    assert notified == []


async def test_execute_ehpc_failure_notifies(monkeypatch):
    """EHPC 提交失败时告警，标识为 ehpc-clustering + run_id。"""
    async def fail_submit(**kwargs):
        raise RuntimeError("boom")

    notified = []
    monkeypatch.setattr(pb, "notify_fc_trigger_failed",
                        lambda name, **kw: notified.append((name, kw)))
    monkeypatch.setitem(pb._EHPC_SUBMITTERS, "clustering", fail_submit)  # pylint: disable=protected-access

    assert await pb._execute("clustering", run_id=22, source_zarr_path="z") is None  # pylint: disable=protected-access
    assert notified == [("ehpc-clustering", {"run_id": 22})]


async def test_execute_fc_reserved_branch(monkeypatch):
    """FC 预留分支：事件体 = 入队载荷剔除 input_bytes。"""
    calls = []

    async def fake_invoke(url, token, event, timeout):
        calls.append((url, event, timeout))
        return "req-1"

    monkeypatch.setattr(pb, "invoke_fc", fake_invoke)
    monkeypatch.setattr(pb, "notify_fc_trigger_failed",
                        lambda *a, **k: pytest.fail("success must not notify"))
    monkeypatch.setitem(pb._FC_ENDPOINTS, "demo_fc", ("http://fc.example", "tok", 30))  # pylint: disable=protected-access
    monkeypatch.setitem(pb._ID_FIELDS, "demo_fc", "run_id")  # pylint: disable=protected-access

    result = await pb._execute(  # pylint: disable=protected-access
        "demo_fc", run_id=7, imzml_path="a.imzML", config_json="{}",
        output_path="out/", metadata={}, input_bytes=99,
    )

    assert result == "req-1"
    assert calls == [("http://fc.example", {
        "run_id": 7, "imzml_path": "a.imzML", "config_json": "{}",
        "output_path": "out/", "metadata": {},
    }, 30)]


async def test_execute_fc_failure_notifies(monkeypatch):
    """FC 调用失败时按 fc-<task> 告警。"""
    async def fail_invoke(*args, **kwargs):
        return None

    notified = []
    monkeypatch.setattr(pb, "invoke_fc", fail_invoke)
    monkeypatch.setattr(pb, "notify_fc_trigger_failed",
                        lambda name, **kw: notified.append((name, kw)))
    monkeypatch.setitem(pb._FC_ENDPOINTS, "demo_fc", ("http://fc.example", "tok", 30))  # pylint: disable=protected-access
    monkeypatch.setitem(pb._ID_FIELDS, "demo_fc", "run_id")  # pylint: disable=protected-access

    assert await pb._execute(  # pylint: disable=protected-access
        "demo_fc", run_id=8, imzml_path="a.imzML", config_json="{}", output_path="out/",
    ) is None
    assert notified == [("fc-demo_fc", {"run_id": 8})]


async def test_execute_unknown_task_raises():
    """队列里出现未登记任何后端的任务键：直接报错。"""
    with pytest.raises(ValueError, match="no backend registered"):
        await pb._execute("nope", run_id=1)  # pylint: disable=protected-access
