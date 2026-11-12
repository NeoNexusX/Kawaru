"""tools/ehpc_client 纯函数单测：不提交真实作业，不访问网络。"""
from types import SimpleNamespace

import pytest

import tools.ehpc_client as ehpc


def _task(**overrides):
    base = dict(
        command=["python", "/app/algorithm_processing.py"],
        results_prefix="hpc-algorithm",
        callback_env="ALGORITHM_FC_CALLBACK_URL",
        callback_path="/callback/algorithm_callback",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _config(**overrides):
    base = dict(
        image="crpi-test.cn-hangzhou.personal.cr.aliyuncs.com/kawaru/sxo@sha256:abc",
        private_registry=False,
        region="cn-hangzhou",
        callback_enabled=False,
        callback_base_url=None,
        vswitch_id="vsw-1",
        security_group_id="sg-1",
        enable_external_ip=True,
        resources=SimpleNamespace(fallback_any_type=True, tiers=[]),
        tasks={"algorithm": _task()},
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _build(config=None, task=None, args=None, extra_env=None):
    # _build_request 接收的是选档后的 resources（choose_resources 的返回形态）
    resources = SimpleNamespace(
        cores=4, memory_gib=8, system_disk_gib=100,
        instance_types=[], fallback_any_type=True,
    )
    return ehpc._build_request(
        config or _config(), "algorithm", task or _task(), resources=resources, target_id=7,
        args=args if args is not None else ["--run-id", "7", "--imzml-path", "origin/a.imzML"],
        extra_env=extra_env,
        credentials={"AccessKeyId": "STS.x", "AccessKeySecret": "s", "SecurityToken": "t" * 300},
    )


def test_build_request_basics(monkeypatch):
    monkeypatch.setattr(ehpc, "OSS_BUCKET_NAME", "bucket")
    monkeypatch.setattr(ehpc, "OSS_INTERNAL_ENDPOINT", "https://oss-internal")

    request = _build()

    container = request["Tasks"][0]["TaskSpec"]["TaskExecutor"][0]["Container"]
    assert request["JobName"] == "algorithm-7"
    assert request["Tasks"][0]["TaskName"] == "algorithm"
    assert container["Command"] == ["python", "/app/algorithm_processing.py"]
    env = {item["Name"]: item["Value"] for item in container["EnvironmentVars"]}
    assert env["OSS_RESULTS_PREFIX"] == "hpc-algorithm"
    assert "OSS_TOKEN_0" in env and "OSS_TOKEN_1" in env
    assert "ALGORITHM_FC_CALLBACK_URL" not in env
    assert request["DeploymentPolicy"]["Network"] == {
        "Vswitch": ["vsw-1"], "EnableExternalIpAddress": True,
    }
    assert "InstanceTypes" not in request["Tasks"][0]["TaskSpec"]["Resource"]


def test_algorithm_args_limits():
    assert len(ehpc._algorithm_args(7, "origin/a.imzML", "{}", "processed/r.zarr", None)) == 8
    args = ehpc._algorithm_args(7, "origin/a.imzML", "{}", "processed/r.zarr", {"filename": "a"})
    assert len(args) == ehpc.MAX_ARGS
    assert args[-2] == "--metadata-json"


def test_extra_env_is_merged():
    container = _build(extra_env={"MEMORY_LIMIT_MB": "8192"})["Tasks"][0]["TaskSpec"]["TaskExecutor"][0]["Container"]
    env = {item["Name"]: item["Value"] for item in container["EnvironmentVars"]}
    assert env["MEMORY_LIMIT_MB"] == "8192"


def test_callback_uses_task_env_and_path(monkeypatch):
    monkeypatch.setenv("FC_CALLBACK_TOKEN", "tok")
    task = _task(
        results_prefix="hpc-clustering",
        callback_env="CLUSTERING_FC_CALLBACK_URL",
        callback_path="/callback/clustering_callback",
    )
    container = _build(
        config=_config(callback_enabled=True, callback_base_url="http://backend:8000"),
        task=task,
    )["Tasks"][0]["TaskSpec"]["TaskExecutor"][0]["Container"]
    env = {item["Name"]: item["Value"] for item in container["EnvironmentVars"]}
    assert env["CLUSTERING_FC_CALLBACK_URL"] == "http://backend:8000/callback/clustering_callback"
    assert env["CALLBACK_TOKEN"] == "tok"
    assert env["OSS_RESULTS_PREFIX"] == "hpc-clustering"


def test_task_config_requires_algorithm():
    with pytest.raises(ValueError, match="ehpc.tasks.algorithm"):
        ehpc._task_config(_config(tasks={}), "algorithm")


def test_session_policy_scope():
    policy = ehpc._session_policy("bucket", ["origin/org_1"], ["processed/run_7.zarr"])
    statements = {tuple(item["Action"]): item for item in policy["Statement"]}

    assert statements[("oss:GetObject",)]["Resource"] == [
        "acs:oss:*:*:bucket/origin/org_1/*",
    ]
    assert statements[("oss:PutObject", "oss:DeleteObject")]["Resource"] == [
        "acs:oss:*:*:bucket/processed/run_7.zarr/*",
    ]


async def test_submit_clustering_job_contract(monkeypatch):
    """clustering 的参数、读写范围与内存变量符合镜像契约。"""
    captured = {}

    async def fake_submit(config, task_name, **kwargs):
        captured.update(task_name=task_name, **kwargs)
        return "job-1"

    clustering_task = _task(
        command=["python", "/app/clustering_umap_kmeans.py"],
        results_prefix="hpc-clustering",
        callback_env="CLUSTERING_FC_CALLBACK_URL",
        callback_path="/callback/clustering_callback",
    )
    monkeypatch.setattr(ehpc, "_settings",
                        lambda: _config(tasks={"clustering": clustering_task}))
    monkeypatch.setattr(ehpc, "_submit_job", fake_submit)

    job_id = await ehpc.submit_clustering_job(run_id=7, source_zarr_path="processed/run_7.zarr",
                                              input_bytes=5 * 2**30)

    assert job_id == "job-1"
    assert captured["task_name"] == "clustering"
    assert captured["args"] == ["--run-id", "7", "--source-zarr-path", "processed/run_7.zarr"]
    assert captured["readable"] == ["processed/run_7.zarr"]
    assert captured["writable"] == ["processed/run_7.zarr", "hpc-clustering/7"]
    # MEMORY_LIMIT_MB 由 _submit_job 按选档后的内存注入（memory_limit_env=True）
    assert captured["memory_limit_env"] is True
    assert captured["input_bytes"] == 5 * 2**30
    assert captured.get("extra_env") is None


def _tier(max_input_gib, cores, memory_gib, system_disk_gib, instance_types=()):
    return SimpleNamespace(
        max_input_gib=max_input_gib, cores=cores, memory_gib=memory_gib,
        system_disk_gib=system_disk_gib, instance_types=list(instance_types),
    )


class TestChooseResources:
    """按输入大小选档；缺失/为 0 时用最后一档（最大资源，保守）。"""

    def _resources(self, tiers):
        return SimpleNamespace(fallback_any_type=True, tiers=tiers)

    def test_missing_input_uses_last_tier(self):
        tiers = [_tier(2, 2, 2, 10), _tier(8, 4, 4, 30), _tier(20, 4, 8, 50)]
        resources = self._resources(tiers)
        for input_bytes in (None, 0, -1):
            chosen = ehpc.choose_resources(resources, input_bytes)
            assert (chosen.cores, chosen.memory_gib, chosen.system_disk_gib) == (4, 8, 50)

    def test_tier_boundaries(self):
        tiers = [
            _tier(2, 2, 2, 10, ["ecs.c9a.large"]),
            _tier(8, 4, 4, 30, ["ecs.c9a.xlarge"]),
            _tier(20, 4, 8, 50, ["ecs.g9ae.xlarge"]),
        ]
        resources = self._resources(tiers)

        small = ehpc.choose_resources(resources, 2 * 2**30)          # 边界：≤2G 命中第一档
        assert (small.cores, small.memory_gib, small.system_disk_gib) == (2, 2, 10)
        assert small.instance_types == ["ecs.c9a.large"]

        medium = ehpc.choose_resources(resources, int(2.5 * 2**30))   # 2–8G
        assert (medium.cores, medium.memory_gib, medium.system_disk_gib) == (4, 4, 30)

        edge = ehpc.choose_resources(resources, 8 * 2**30)            # 边界：≤8G 命中第二档
        assert (edge.cores, edge.memory_gib, edge.system_disk_gib) == (4, 4, 30)

        large = ehpc.choose_resources(resources, int(8.5 * 2**30))    # 8–20G
        assert (large.cores, large.memory_gib, large.system_disk_gib) == (4, 8, 50)

    def test_beyond_last_tier_uses_last(self):
        tiers = [_tier(2, 2, 2, 10), _tier(20, 4, 8, 50, ["ecs.g9ae.xlarge"])]
        chosen = ehpc.choose_resources(self._resources(tiers), 50 * 2**30)
        assert (chosen.cores, chosen.memory_gib, chosen.system_disk_gib) == (4, 8, 50)
        assert chosen.instance_types == ["ecs.g9ae.xlarge"]

    def test_tier_without_types_means_self_selection(self):
        chosen = ehpc.choose_resources(self._resources([_tier(2, 2, 2, 10)]), 1 * 2**30)
        assert chosen.instance_types == []

    def test_fallback_any_type_inherited(self):
        chosen = ehpc.choose_resources(self._resources([_tier(2, 2, 2, 10)]), 1 * 2**30)
        assert chosen.fallback_any_type is True

    def test_model_requires_at_least_one_tier(self):
        from models.config_models import EhpcResourceSettings
        with pytest.raises(Exception, match="tiers"):
            EhpcResourceSettings()


def test_build_request_uses_chosen_resources():
    """_build_request 用传入的 resources（选档结果），不是 config.resources 的固定值。"""
    chosen = SimpleNamespace(
        cores=4, memory_gib=4, system_disk_gib=40,
        instance_types=["ecs.c9a.xlarge"], fallback_any_type=True,
    )
    request = ehpc._build_request(
        _config(), "algorithm", _task(), resources=chosen, target_id=7,
        args=["--run-id", "7", "--imzml-path", "origin/a.imzML"],
        extra_env=None,
        credentials={"AccessKeyId": "STS.x", "AccessKeySecret": "s", "SecurityToken": "t"},
    )
    resource = request["Tasks"][0]["TaskSpec"]["Resource"]
    assert resource["Cores"] == 4 and resource["Memory"] == 4
    assert resource["Disks"] == [{"Type": "System", "Size": 40}]
    assert resource["InstanceTypes"] == ["ecs.c9a.xlarge"]
