import os
from typing import Optional
from exceptions.file_exceptions import storage_type_not_allowed_exception


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


def init_celery():
    """显式创建 celery app"""
    import background.celery_app # pylint: disable=import-outside-toplevel

    if background.celery_app.celery_app is None:
        from boot.config import get_config # pylint: disable=import-outside-toplevel
        background.celery_app.celery_app = background.celery_app.celery_create(get_config())

    return background.celery_app.celery_app


def init_oss():
    """显式初始化 oss 客户端"""
    import storage_backend.oss_client as oss_client # pylint: disable=import-outside-toplevel

    if oss_client.oss_client is None:
        oss_client.oss_client = oss_client.create_oss_client()

    return oss_client.oss_client



BOOTSTRAP_SUMMARIZED = False  # 标记是否已输出启动概要，防止重复输出启动概要


def bootstrap():
    """统一显式初始化 Env + Logger + Config幂等"""

    ENV = init_env() # pylint: disable=C0103
    CONFIG = init_config() # pylint: disable=C0103
    init_logging()

    if CONFIG.config.storage.allowed_storage_types == ["oss"]:
        OSS_CLIENT = init_oss() # pylint: disable=unused-variable, invalid-name
    elif CONFIG.config.storage.allowed_storage_types == ["local"]:
        OSS_CLIENT = None # pylint: disable=unused-variable, invalid-name
    else:
        raise storage_type_not_allowed_exception

    CELERY_APP = init_celery() # pylint: disable=C0103


    #log part
    from tools.logger import get_logger, LOG_LEVELS   # pylint: disable=import-outside-toplevel
    from boot.celery_boot import celery_summary # pylint: disable=import-outside-toplevel

    logger = get_logger("watchfiles")
    logger.setLevel(LOG_LEVELS['warning'])  # watchfiles 监控文件变化时会输出大量日志，提升到 warning 级别以上以减少干扰

    logger = get_logger("bootstrap")
    logger.info("runtime environment initialized")

    global BOOTSTRAP_SUMMARIZED # pylint: disable=global-statement
    if not BOOTSTRAP_SUMMARIZED:
        # 初始化 celery 实例必须在 config 初始化之后（因为 celeryconfig 依赖 config），
        logger.info("Bootstrap summary:"+"="*60)
        ENV.summary()
        CONFIG.summary()
        celery_summary(CELERY_APP, logger)
        BOOTSTRAP_SUMMARIZED = True
