import os
from typing import Optional
from celery import Celery


def init_logging(log_level: Optional[str] = None, log_dir: Optional[str] = None) -> None:
    """显式初始化日志系统（必须先于任何 get_logger 调用）"""
    import tools.logger as logger  # pylint: disable=import-outside-toplevel

    if getattr(logger.LOGGER_MANAGER, "initialized", False):
        return
    logger.LOGGER_MANAGER = logger.LoggerManager()

    logger.LOGGER_MANAGER.init_app(
        log_level=log_level or os.getenv("LOG_LEVEL", "info"),
        log_dir=log_dir or os.getenv("LOG_DIR", "logs"),
    )


def init_env():
    """显式加载 .env,并可选校验必需环境变量"""
    # 延迟 import，避免 envset 模块 import-time 拉起 logger
    from boot.envset import EnvSet  # pylint: disable=import-outside-toplevel

    env = EnvSet()

    return env


def init_config():
    """显式读取 config.yaml 并初始化全局 CONFIG"""
    # 必须在 init_logging 之后 import（因为 envset.config 顶层会取 logger）
    import boot.config  # pylint: disable=import-outside-toplevel

    boot.config.CONFIG = boot.config.ConfigCreate()
    if boot.config.CONFIG.config is None:
        boot.config.CONFIG.read()

    return boot.config.CONFIG


def celery_summary(app: Celery, logger):
    """输出 Celery 配置摘要"""

    # 关键配置信息
    broker = app.conf.get('broker_url', 'N/A')
    backend = app.conf.get('result_backend', 'N/A')
    redbeat_backend = app.conf.get('redbeat_redis_url', 'N/A')
    timezone = app.conf.get('timezone', 'UTC')

    # 任务模块
    tasks = list(app.tasks.keys())
    # 过滤掉 Celery 内部任务
    user_tasks = [t for t in tasks if not t.startswith('celery.')]

    # Beat 调度任务
    beat_schedule = app.conf.get('beat_schedule', {})

    logger.info("=" * 50)
    logger.info("Celery Configuration Summary")
    logger.info("-" * 50)
    logger.info(f"Broker:     {broker}")
    logger.info(f"Backend:    {backend}")
    logger.info(f"Redbeat Backend: {redbeat_backend}")
    logger.info(f"Timezone:   {timezone}")
    logger.info(f"Tasks:      {len(user_tasks)} registered")
    for task in sorted(user_tasks):
        logger.info(f"            - {task}")

    if beat_schedule:
        logger.info(f"Beat Jobs:  {len(beat_schedule)} scheduled")
        for name, conf in beat_schedule.items():
            schedule = conf.get('schedule', 'N/A')
            task_name = conf.get('task', 'N/A')
            logger.info(f"            - {name}: {task_name} ({schedule})")
    else:
        logger.info("Beat Jobs:  None")

    logger.info("=" * 50)


BOOTSTRAP_SUMMARIZED = False  # 标记是否已输出启动概要，防止重复输出启动概要


def bootstrap():
    """统一显式初始化 Env + Logger + Config幂等"""

    ENV = init_env() # pylint: disable=C0103
    CONFIG = init_config() # pylint: disable=C0103
    init_logging()

    #log part
    from tools.logger import get_logger, LOG_LEVELS   # pylint: disable=import-outside-toplevel

    logger = get_logger("watchfiles")
    logger.setLevel(LOG_LEVELS['warning'])  # watchfiles 监控文件变化时会输出大量日志，提升到 warning 级别以上以减少干扰

    logger = get_logger("bootstrap")
    logger.info("runtime environment initialized")

    global BOOTSTRAP_SUMMARIZED # pylint: disable=global-statement
    if not BOOTSTRAP_SUMMARIZED:
        # 初始化 celery 实例必须在 config 初始化之后（因为 celeryconfig 依赖 config），
        from background.celery_app import celery_app  # pylint: disable=import-outside-toplevel

        logger.info("Bootstrap summary:"+"="*60)
        ENV.summary()
        CONFIG.summary()
        celery_summary(celery_app, logger)
        BOOTSTRAP_SUMMARIZED = True
