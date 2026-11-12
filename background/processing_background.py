"""
计算任务触发模块（E-HPC + 预留 FC）。

- dispatch：API 侧唯一入口，接收已由载荷模型校验的参数并投递给 Worker
- _execute：Worker 侧执行入口，按任务注册表选择后端
  - _EHPC_SUBMITTERS 已注册 → E-HPC INSTANT 容器作业（当前四个计算任务）
  - _FC_ENDPOINTS 已注册 → FC HTTP 触发器（预留分支，当前无任务）
- 解压不走本模块：由 OSS 触发器直调 FC，完成后回调 /oss_upload_callback

载荷校验只做一次：API 端点构造模型时。队列唯一生产者是 dispatch（入队的即
模型 dump），Worker 不重复校验；异常载荷由提交器 TypeError → 告警兜底。

容器完成后通过回调接口返回结果。
"""
from typing import get_args

from api_helpers.admin_notify import notify_fc_trigger_failed
from celery_runtime.state import get_celery_app
from models.task_models import ComputeTaskParams
from tools.ehpc_client import (
    safe_error,
    submit_algorithm_job,
    submit_clustering_job,
    submit_preview_job,
    submit_raw_convert_job,
)
from tools.fc_client import invoke_fc
from tools.logger import get_logger


logger = get_logger("background.processing_background")
celery_app = get_celery_app()


#########################
# 调度层：任务注册表 + 统一分发
#########################
# 任务标识字段（日志/告警用 run_id 或 file_id）：由载荷模型派生，与模型定义同源
_ID_FIELDS = {model.task: model.id_field for model in get_args(ComputeTaskParams)}

# 任务 → EHPC 提交器（当前四个计算任务全部走 E-HPC）
_EHPC_SUBMITTERS = {
    "raw_convert": submit_raw_convert_job,
    "preview": submit_preview_job,
    "algorithm": submit_algorithm_job,
    "clustering": submit_clustering_job,
}

# 任务 → FC 端点 (url, token, timeout)：预留分支，当前无任务。
# 新增后端主动触发的 FC 计算时在此登记，并在 config 的 oss 段补端点/超时、
# .env 补 token；事件体由入队载荷剔除 _FC_HIDDEN_KWARGS 得到。
_FC_ENDPOINTS: dict[str, tuple[str | None, str | None, float | int | None]] = {}

# 仅用于 EHPC 选资源档位的参数，不进 FC 事件体。
# 载荷在 API 构造模型时已归一化（metadata）、入队时已剔除 None，这里只剩这一条规则。
_FC_HIDDEN_KWARGS = ("input_bytes",)


def _notify_trigger_failed(task: str, backend: str, identifier: int) -> None:
    """失败告警：标识字段按任务取 run_id/file_id，名称沿用 <backend>-<task> 前缀。"""
    notify_fc_trigger_failed(f"{backend}-{task}", **{_ID_FIELDS[task]: identifier})


def dispatch(params: ComputeTaskParams) -> None:
    """API 侧唯一入口：接收已校验的载荷模型，投递给 Worker。

    参数错误在 API 端点构造模型时已 fail-fast；这里只校验后端已注册并入队。
    后端选择与提交发生在 Worker 侧的 _execute。
    """
    task = params.task
    if task not in _EHPC_SUBMITTERS and task not in _FC_ENDPOINTS:
        raise ValueError(f"no backend registered for task {task!r}")
    run_compute_task.delay(task, params.model_dump(exclude_none=True)) # type: ignore[arg-type]


@celery_app.task()
async def run_compute_task(task: str, params: dict) -> None:
    """Worker 侧执行入口：按注册表提交 E-HPC 作业或调用 FC。

    入队的即已校验的模型 dump（队列唯一生产者是 dispatch），不重复校验。
    """
    await _execute(task, **params)


async def _execute(task: str, **kwargs) -> str | None:
    """按任务注册表选择后端：EHPC 优先，未注册则走 FC（预留）。

    kwargs 为 dispatch 入队的载荷（已校验、已剔除 None），直接信任使用。
    返回 EHPC job_id / FC request_id；失败记录并告警后返回 None。
    """
    submitter = _EHPC_SUBMITTERS.get(task)
    endpoint = _FC_ENDPOINTS.get(task)
    if submitter is None and endpoint is None:
        raise ValueError(f"no backend registered for task {task!r}")
    identifier = kwargs[_ID_FIELDS[task]]
    if submitter is not None:
        try:
            job_id = await submitter(**kwargs)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("trigger ehpc %s failed %s=%s error=%s",
                         task, _ID_FIELDS[task], identifier, safe_error(e))
            _notify_trigger_failed(task, "ehpc", identifier)
            return None
        logger.info("trigger ehpc %s success %s=%s job_id=%s",
                    task, _ID_FIELDS[task], identifier, job_id)
        return job_id

    url, token, timeout = endpoint  # type: ignore[misc]
    event = {k: v for k, v in kwargs.items() if k not in _FC_HIDDEN_KWARGS}
    logger.info("trigger fc %s %s=%s", task, _ID_FIELDS[task], identifier)
    request_id = await invoke_fc(url, token, event, timeout)
    if request_id is None:
        logger.error("trigger fc %s failed %s=%s", task, _ID_FIELDS[task], identifier)
        _notify_trigger_failed(task, "fc", identifier)
        return None
    logger.info("trigger fc %s success %s=%s request_id=%s",
                task, _ID_FIELDS[task], identifier, request_id)
    return request_id
