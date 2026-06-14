"""Celery Beat 独立入口。

Beat 只负责按 beat_schedule 往 broker 投递任务名（字符串），自身不执行任务，
因此**不需要** import 任何 task 模块（不设 ``imports``），也就不会拉入 FastAPI /
SQLAlchemy / OSS SDK / aliyun.log 等重型依赖。

这样 beat 子进程内存可从 ~128MB 降到 ~40MB。Worker 仍走 ``entrypoint:celery_app``
（保留 ``imports``）以便加载并执行任务。
"""
from celery import Celery

from boot.bootstrap import init_env, init_config, init_logging, load_constants
from boot.config import get_config
from celery_runtime.app import build_redis_urls, build_beat_schedule
from celery_runtime.state import set_celery_app
from tools.logger import get_logger


def build_beat_app() -> Celery:
    """构造仅含调度配置的轻量 beat app（无 task 模块导入）。"""
    init_env()
    init_config()
    init_logging()
    constants = load_constants()
    logger = get_logger("celery_runtime.beat")

    cfg = get_config()
    app = Celery("kawaru_beat")
    app.config_from_object({
        **build_redis_urls(cfg),
        "beat_scheduler": "redbeat.schedulers.RedBeatScheduler",
        "timezone": "UTC",
        "enable_utc": True,
        # 刻意不设置 "imports"：beat 无需加载 task 实现
        "beat_schedule": build_beat_schedule(constants),
    })
    set_celery_app(app)
    logger.info("Celery beat runtime initialized (lightweight, no task imports)")
    return app


beat_app = build_beat_app()
