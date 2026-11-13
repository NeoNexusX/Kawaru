"""
E-HPC INSTANT 作业提交（四个计算任务：raw_convert / preview / algorithm / clustering）。

镜像契约（单容器单任务，完成后退出；数据路径均为 bucket 内相对 key，
输出目录键尾带 /，容器原样回传，回调端 check_file_exist 依赖尾斜杠走前缀列举）：
    raw_convert: python /app/imz_to_zarr.py --run-id N --imzml-path <key>
                 [--ibd-path <key>] --output-path <key>/ [--metadata-json <json>]
    preview:     python /app/imzml_preview.py --file-id N --imzml-path <key>
                 --image-dest <key>/ [--mode feature|tic] [--ibd-path <key>]
    algorithm:   python /app/algorithm_processing.py --run-id N --imzml-path <key>
                 --config-json <json> --output-path <key>/ [--metadata-json <json>]
    clustering:  python /app/clustering_umap_kmeans.py --run-id N --source-zarr-path <key>

容器环境变量（由 _job_env 注入；INSTANT 限制 20 个变量、单项 ≤256 字符）：
    OSS_BUCKET_NAME / OSS_ENDPOINT_URL / OSS_RESULTS_PREFIX（manifest 归档前缀）
    STS 临时凭证（OSS_TOKEN_n 分块）
    MEMORY_LIMIT_MB（仅 clustering，按选档内存注入）
    回调 URL + CALLBACK_TOKEN（可选，不设则不回调）：
        RAW_CONVERT_FC_CALLBACK_URL / ALGORITHM_FC_CALLBACK_URL / CLUSTERING_FC_CALLBACK_URL
        FEATURE_FC_CALLBACK_URL 或 TIC_FC_CALLBACK_URL（preview 按 mode 选择）

复用：
    - storage.oss.oss_storage._assume_role 签发按 run 收窄的 STS
    - constants.file_constants 的 bucket / 内网 endpoint

SDK 是同步接口，提交时用 asyncio.to_thread 包装。
"""
import asyncio
import json
import os
import posixpath
from types import SimpleNamespace

from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_credentials.models import Config as CredentialConfig
from alibabacloud_ehpcinstant20230701.client import Client
from alibabacloud_ehpcinstant20230701.models import CreateJobRequest
from alibabacloud_tea_openapi import models as open_api_models

from boot.config import get_config
from constants.file_constants import OSS_BUCKET_NAME, OSS_INTERNAL_ENDPOINT
from storage.oss.oss_storage import _assume_role
from tools.logger import get_logger


logger = get_logger("tools.ehpc_client")

#########################
# 常量：INSTANT 容器限制（官方 SDK models 文档字符串）
#########################
ENV_VALUE_LIMIT = 256          # 单个环境变量值 / Command 单项上限（字符）
MAX_ENV_VARS = 20              # 环境变量总数上限（STS token 分块也占名额）
MAX_ARGS = 10                  # Arg 总数上限
MAX_COMMAND_ITEMS = 20         # Command 条数上限
# 作业脚本约定的退出码（0 成功 / 2 管线失败 / 3 manifest 上传失败 / 64 配置或凭证错误；
# 1 为容器内未预期崩溃），命中即不再重试
NO_RETRY_EXIT_CODES = (1, 2, 3, 64)
_TRUE_VALUES = ("1", "true", "yes", "on")


#########################
# 开关、配置与错误脱敏
#########################
def _flag(name: str, default: bool) -> bool:
    """读取布尔环境变量；未设置时返回默认值（本地调试用覆盖开关）。"""
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in _TRUE_VALUES


def safe_error(exc: Exception) -> str:
    """异常摘要：脱敏 ACR/RAM/回调凭证后截断，避免密码进日志。"""
    text = str(exc)
    for name in ("ACR_PULL_PASSWORD", "ACR_PULL_USERNAME", "OSS_ACCESS_KEY_SECRET", "FC_CALLBACK_TOKEN"):
        secret = os.getenv(name)
        if secret:
            text = text.replace(secret, "***")
    return f"{type(exc).__name__}: {text[:500]}"


def _settings():
    """读取 EHPC 配置并校验运行时凭证（环境变量）。

    配置结构（段完整、基础设施字段非空、oss bucket/内网 endpoint）已在启动时
    由 pydantic 与 check_oss_config 校验，这里只剩无法启动时校验的环境变量
    （CI 可无 ACR 凭证启动，只在真正提交作业时要求）。
    """
    config = get_config().ehpc
    if not (os.getenv("OSS_ACCESS_KEY_ID") and os.getenv("OSS_ACCESS_KEY_SECRET")):
        raise ValueError("OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET are required")
    if config.private_registry and not (
        os.getenv("ACR_PULL_USERNAME") and os.getenv("ACR_PULL_PASSWORD")
    ):
        raise ValueError("ACR_PULL_USERNAME / ACR_PULL_PASSWORD are required")
    return config


def _task_config(config, name: str):
    """取任务入口与回调配置。"""
    task = config.tasks.get(name)
    if task is None:
        raise ValueError(f"ehpc.tasks.{name} is required")
    if not task.results_prefix:
        raise ValueError(f"ehpc.tasks.{name}.results_prefix is required")
    return task


def _manifest_prefix(task, target_id: int, suffix: str | None = None) -> str:
    """manifest 归档前缀，纳入 STS 可写范围。

    suffix 用于按 mode 分目录的任务（preview：{prefix}/{mode}/{file_id}）。
    """
    base = task.results_prefix.strip("/")
    if suffix:
        return f"{base}/{suffix.strip('/')}/{target_id}"
    return f"{base}/{target_id}"


#########################
# STS 凭证与会话策略
#########################
def _session_policy(bucket: str, readable: list[str], writable: list[str]) -> dict:
    """按 run 收窄的会话策略：输入只读，输出与 manifest 前缀可写可删。"""
    listable = list(dict.fromkeys(readable + writable))
    return {"Version": "1", "Statement": [
        {"Effect": "Allow", "Action": ["oss:ListObjects"],
         "Resource": [f"acs:oss:*:*:{bucket}"],
         "Condition": {"StringLike": {"oss:Prefix": [f"{p}/*" for p in listable]}}},
        {"Effect": "Allow", "Action": ["oss:GetObject"],
         "Resource": [f"acs:oss:*:*:{bucket}/{p}/*" for p in readable]},
        {"Effect": "Allow", "Action": ["oss:PutObject", "oss:DeleteObject"],
         "Resource": [f"acs:oss:*:*:{bucket}/{p}/*" for p in writable]},
    ]}


def _credential_env(credentials: dict) -> dict:
    """STS 凭证 → 容器环境变量；SecurityToken 按 256 字符切成 OSS_TOKEN_0..n。"""
    credentials = credentials.get("Credentials", credentials)
    token = credentials["SecurityToken"]
    env = {
        "ALIBABA_CLOUD_ACCESS_KEY_ID": credentials["AccessKeyId"],
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": credentials["AccessKeySecret"],
    }
    for index, start in enumerate(range(0, len(token), ENV_VALUE_LIMIT)):
        env[f"OSS_TOKEN_{index}"] = token[start:start + ENV_VALUE_LIMIT]
    return env


def _callback_env(config, task, *, callback_env: str | None = None,
                  callback_path: str | None = None) -> dict:
    """回调环境变量；EHPC_CALLBACK_ENABLED=false 时不下发（本地无法被回调）。

    preview 按 mode 传入对应的 env/path；其余任务用任务配置里的单组回调。
    """
    if not _flag("EHPC_CALLBACK_ENABLED", config.callback_enabled):
        return {}
    env_name = callback_env or task.callback_env
    path = callback_path or task.callback_path
    if not env_name or not path:
        raise ValueError("callback env/path is required (task.callbacks or callback_env/path)")
    base = config.callback_base_url or get_config().server.public_base_url
    if not base:
        raise ValueError("ehpc.callback_base_url or server.public_base_url is required")
    url = f"{base.rstrip('/')}/{path.lstrip('/')}"
    token = os.getenv("FC_CALLBACK_TOKEN")
    if not token:
        raise ValueError("FC_CALLBACK_TOKEN is required when callback is enabled")
    return {env_name: url, "CALLBACK_TOKEN": token}


#########################
# 资源选档（ehpc.resources 所有任务共用）
#########################
def choose_resources(resources, input_bytes: int | None):
    """按输入大小选档；缺失/为 0 时用最后一档（最大资源，宁可浪费不可 OOM）。"""
    tiers = sorted(resources.tiers, key=lambda tier: tier.max_input_gib)
    tier = tiers[-1]
    if input_bytes and input_bytes > 0:
        input_gib = input_bytes / 2**30
        tier = next((item for item in tiers if input_gib <= item.max_input_gib), tiers[-1])
    return SimpleNamespace(
        cores=tier.cores,
        memory_gib=tier.memory_gib,
        system_disk_gib=tier.system_disk_gib,
        instance_types=list(tier.instance_types),
        fallback_any_type=resources.fallback_any_type,
    )


#########################
# CreateJob 请求构造
#########################
def _job_env(config, task, credentials: dict, extra_env: dict[str, str] | None,
             callback_env: str | None = None, callback_path: str | None = None) -> dict:
    """容器环境变量：OSS 基础 + 任务额外 + STS 分块 + 回调；整体校验 INSTANT 限制。"""
    env = {
        "OSS_BUCKET_NAME": OSS_BUCKET_NAME,
        "OSS_ENDPOINT_URL": OSS_INTERNAL_ENDPOINT,
        "OSS_RESULTS_PREFIX": task.results_prefix,
    }
    env.update(extra_env or {})
    env.update(_credential_env(credentials))
    env.update(_callback_env(config, task, callback_env=callback_env, callback_path=callback_path))
    if len(env) > MAX_ENV_VARS:
        raise ValueError(f"{len(env)} env vars exceed the INSTANT limit of {MAX_ENV_VARS}")
    for name, value in env.items():
        if len(str(value)) > ENV_VALUE_LIMIT:
            raise ValueError(f"env var {name} exceeds {ENV_VALUE_LIMIT} chars")
    return env


def _container_spec(config, task, *, args: list[str], env: dict) -> dict:
    """容器定义：镜像、启动命令、参数、环境变量与私有仓库认证。"""
    if len(task.command) > MAX_COMMAND_ITEMS or any(
            len(str(item)) > ENV_VALUE_LIMIT for item in task.command):
        raise ValueError("container command exceeds the INSTANT limits")
    if len(args) > MAX_ARGS:
        raise ValueError(f"{len(args)} container args exceed the INSTANT limit of {MAX_ARGS}")
    container = {
        "Image": config.image,
        "Command": list(task.command),
        "Arg": args,
        "EnvironmentVars": [{"Name": name, "Value": str(value)} for name, value in env.items()],
    }
    if config.private_registry:
        container["ImageRegistryOptions"] = json.dumps({
            "ImageRegistryType": "https",
            "ImageRegistryServer": config.image.split("/")[0],
            "ImageRegistryUserName": os.environ["ACR_PULL_USERNAME"],
            "ImageRegistryPassword": os.environ["ACR_PULL_PASSWORD"],
        })
    return container


def _resource_spec(resources) -> dict:
    """资源定义：核数、内存、系统盘与可选的钉选规格（选档结果）。"""
    resource = {
        "Cores": resources.cores,
        "Memory": resources.memory_gib,
        "Disks": [{"Type": "System", "Size": resources.system_disk_gib}],
    }
    if resources.instance_types:
        resource["InstanceTypes"] = list(resources.instance_types)
    return resource


def _build_request(config, task_name: str, task, *, resources, target_id: int, args: list[str],
                   extra_env: dict[str, str] | None, credentials: dict,
                   callback_env: str | None = None, callback_path: str | None = None) -> dict:
    """组装 CreateJob 请求 dict（字段与官方 SDK model 一一对应，提交前经 from_map 校验）。

    target_id 为任务标识（run_id 或 file_id），用于 JobName/描述；
    callback_env/callback_path 用于按 mode 覆盖任务默认回调（preview）。
    """
    return {
        "JobName": f"{task_name}-{target_id}",
        "JobDescription": f"Kawaru {task_name} run {target_id}",
        "Tasks": [{
            "TaskName": task_name,
            "TaskSustainable": False,
            "ExecutorPolicy": {"MaxCount": 1,
                               "ArraySpec": {"IndexStart": 0, "IndexEnd": 0, "IndexStep": 1}},
            "TaskSpec": {
                "TaskExecutor": [{"Container": _container_spec(
                    config, task, args=args, env=_job_env(
                        config, task, credentials, extra_env,
                        callback_env=callback_env, callback_path=callback_path,
                    ),
                )}],
                "VolumeMount": [],
                "Resource": _resource_spec(resources),
                "RetryPolicy": {"RetryCount": 1, "ExitCodeActions": [
                    {"ExitCode": code, "Action": "Exit"} for code in NO_RETRY_EXIT_CODES
                ]},
            },
        }],
        "DeploymentPolicy": {"AllocationSpec": "Standard", "Tag": [], "Network": {
            "Vswitch": [config.vswitch_id],
            "EnableExternalIpAddress": config.enable_external_ip,
        }},
        "SecurityPolicy": {"SecurityGroup": {"SecurityGroupIds": [config.security_group_id]}},
    }


#########################
# 提交：CreateJob（含售罄回退）与任务入口
#########################
def _create_job(region: str, request: dict) -> str:
    """同步提交 CreateJob 并返回 JobId；经官方 SDK model 校验，防字段被静默丢弃。"""
    credential = CredentialClient(CredentialConfig(
        type="access_key",
        access_key_id=os.environ["OSS_ACCESS_KEY_ID"],
        access_key_secret=os.environ["OSS_ACCESS_KEY_SECRET"],
    ))
    client = Client(open_api_models.Config(
        credential=credential,
        region_id=region,
        endpoint=f"ehpcinstant.{region}.aliyuncs.com",
        protocol="HTTPS",
        connect_timeout=10000,
        read_timeout=30000,
    ))
    model = CreateJobRequest().from_map(request)
    model.validate()
    if model.to_map() != request:
        raise ValueError("SDK dropped an unsupported request field")
    job_id = client.create_job(model).body.job_id
    if not job_id:
        raise RuntimeError("CreateJob returned no JobId")
    return job_id


def _sold_out(exc: Exception) -> bool:
    """规格售罄：请求阶段被拒，服务端保证没有创建作业，可安全重提。"""
    exc = getattr(exc, "inner_exception", None) or exc
    return "SoldOut" in str(getattr(exc, "code", None) or "")


async def _submit_job(config, task_name: str, *, target_id: int, id_field: str = "run_id",
                      args: list[str],
                      readable: list[str], writable: list[str],
                      input_bytes: int | None = None,
                      memory_limit_env: bool = False,
                      extra_env: dict[str, str] | None = None,
                      callback_env: str | None = None,
                      callback_path: str | None = None) -> str:
    """通用提交：选档 → 收窄会话策略 → AssumeRole → CreateJob（含售罄回退）。

    target_id 为任务标识（run_id 或 file_id），id_field 只用于日志；
    callback_env/callback_path 用于按 mode 覆盖任务默认回调（preview）。
    """
    task = _task_config(config, task_name)
    resources = choose_resources(config.resources, input_bytes)
    if memory_limit_env:
        extra_env = {**(extra_env or {}), "MEMORY_LIMIT_MB": str(resources.memory_gib * 1024)}
    policy = _session_policy(OSS_BUCKET_NAME, readable, writable)  # type: ignore[arg-type]
    credentials = await _assume_role(
        policy, f"ehpc-{task_name}-{target_id}"[:64], config.sts_duration_seconds,
    )
    request = _build_request(
        config, task_name, task, resources=resources, target_id=target_id, args=args,
        extra_env=extra_env, credentials=credentials,
        callback_env=callback_env, callback_path=callback_path,
    )

    try:
        job_id = await asyncio.to_thread(_create_job, config.region, request)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # CreateJob 无 ClientToken 幂等字段：除「售罄（服务端保证未建作业）」外不重试
        if not (resources.instance_types and resources.fallback_any_type and _sold_out(exc)):
            raise
        logger.warning("instance types sold out task=%s %s=%s, retrying without InstanceTypes",
                       task_name, id_field, target_id)
        request["Tasks"][0]["TaskSpec"]["Resource"].pop("InstanceTypes", None)
        job_id = await asyncio.to_thread(_create_job, config.region, request)

    logger.info("ehpc job submitted task=%s %s=%s job_id=%s image=%s",
                task_name, id_field, target_id, job_id, config.image)
    return job_id


def _algorithm_args(run_id: int, imzml_path: str, config_json: str,
                    output_path: str, metadata: dict | None) -> list[str]:
    """预处理命令参数（入口 /app/algorithm_processing.py，契约见模块 docstring）。"""
    args = [
        "--run-id", str(run_id),
        "--imzml-path", imzml_path.strip("/"),
        "--config-json", config_json,
        # zarr 输出是目录：键尾带 /。容器原样回传 out_path，algorithm_callback
        # 的 check_file_exist 靠尾斜杠走前缀列举；丢了斜杠会 HEAD 裸 key 误判「输出缺失」
        "--output-path", f"{output_path.strip('/')}/",
    ]
    if metadata:
        args += ["--metadata-json", json.dumps(metadata, separators=(",", ":"), ensure_ascii=False)]
    return args


async def submit_algorithm_job(*, run_id: int, imzml_path: str, config_json: str,
                               output_path: str, metadata: dict | None = None,
                               input_bytes: int | None = None) -> str:
    """提交预处理作业，返回 INSTANT job_id；失败抛异常。"""
    config = _settings()
    task = _task_config(config, "algorithm")
    input_dir = posixpath.dirname(imzml_path.strip("/"))
    if not input_dir:
        raise ValueError("imzml_path must be under a directory")
    return await _submit_job(
        config, "algorithm", target_id=run_id,
        args=_algorithm_args(run_id, imzml_path, config_json, output_path, metadata),
        readable=[input_dir],
        writable=[output_path.strip("/"), _manifest_prefix(task, run_id)],
        input_bytes=input_bytes,
    )


async def submit_clustering_job(*, run_id: int, source_zarr_path: str,
                                input_bytes: int | None = None) -> str:
    """提交 UMAP 聚类作业（结果写回源 zarr），返回 job_id。

    命令契约见模块 docstring；MEMORY_LIMIT_MB 按选档后的内存自动注入。
    """
    config = _settings()
    task = _task_config(config, "clustering")
    source = source_zarr_path.strip("/")
    if not source.endswith(".zarr"):
        raise ValueError("source_zarr_path must end with .zarr")
    return await _submit_job(
        config, "clustering", target_id=run_id,
        args=["--run-id", str(run_id), "--source-zarr-path", source],
        readable=[source],
        writable=[source, _manifest_prefix(task, run_id)],
        input_bytes=input_bytes, memory_limit_env=True,
    )


def _raw_convert_args(run_id: int, imzml_path: str, ibd_path: str,
                      output_path: str, metadata: dict | None) -> list[str]:
    """原始转换命令参数（入口 /app/imz_to_zarr.py，契约见模块 docstring）。"""
    args = [
        "--run-id", str(run_id),
        "--imzml-path", imzml_path.strip("/"),
        "--ibd-path", ibd_path.strip("/"),
        # 同 _algorithm_args：目录键尾带 /，回调端前缀探测依赖它
        "--output-path", f"{output_path.strip('/')}/",
    ]
    if metadata:
        args += ["--metadata-json", json.dumps(metadata, separators=(",", ":"), ensure_ascii=False)]
    return args


async def submit_raw_convert_job(*, run_id: int, imzml_path: str, ibd_path: str,
                                 output_path: str, metadata: dict | None = None,
                                 input_bytes: int | None = None) -> str:
    """提交原始转换作业（imzML/ibd → 新 Zarr，不启用算法），返回 job_id。"""
    config = _settings()
    task = _task_config(config, "raw_convert")
    input_dir = posixpath.dirname(imzml_path.strip("/"))
    if not input_dir:
        raise ValueError("imzml_path must be under a directory")
    return await _submit_job(
        config, "raw_convert", target_id=run_id,
        args=_raw_convert_args(run_id, imzml_path, ibd_path, output_path, metadata),
        readable=[input_dir],
        writable=[output_path.strip("/"), _manifest_prefix(task, run_id)],
        input_bytes=input_bytes,
    )


PREVIEW_MODES = ("feature", "tic")


def _preview_args(file_id: int, imzml_path: str, image_dest: str,
                  mode: str, ibd_path: str | None) -> list[str]:
    """上传后预览命令参数（入口 /app/imzml_preview.py，契约见模块 docstring）。"""
    args = [
        "--file-id", str(file_id),
        "--imzml-path", imzml_path.strip("/"),
        "--image-dest", image_dest,
        "--mode", mode,
    ]
    if ibd_path:
        args += ["--ibd-path", ibd_path.strip("/")]
    return args


async def submit_preview_job(*, file_id: int, imzml_path: str, image_dest: str,
                             mode: str, ibd_path: str | None = None,
                             input_bytes: int | None = None) -> str:
    """提交上传后预览作业（feature: UMAP/TIC/离子图；tic: 仅 TIC），返回 job_id。

    回调按 mode 取 tasks.preview.callbacks.<mode>；manifest 前缀为 {prefix}/{mode}/{file_id}。
    """
    if mode not in PREVIEW_MODES:
        raise ValueError(f"preview mode must be one of {PREVIEW_MODES}")
    config = _settings()
    task = _task_config(config, "preview")
    callback = task.callbacks.get(mode)
    if callback is None:
        raise ValueError(f"ehpc.tasks.preview.callbacks.{mode} is required")
    input_dir = posixpath.dirname(imzml_path.strip("/"))
    if not input_dir:
        raise ValueError("imzml_path must be under a directory")
    # 图片目录键尾带 /：容器原样回传 image_path，_handle_preview_callback 的
    # check_file_exist 靠尾斜杠走前缀列举，丢斜杠会 HEAD 裸 key 误判缺失。
    # STS 可写前缀用去斜杠形式（策略按 {p}/* 拼接，尾斜杠会产生双斜杠不匹配）
    image_dest = f"{image_dest.strip('/')}/"
    return await _submit_job(
        config, "preview", target_id=file_id, id_field="file_id",
        args=_preview_args(file_id, imzml_path, image_dest, mode, ibd_path),
        readable=[input_dir],
        writable=[image_dest.rstrip("/"), _manifest_prefix(task, file_id, suffix=mode)],
        input_bytes=input_bytes,
        callback_env=callback.env, callback_path=callback.path,
    )
