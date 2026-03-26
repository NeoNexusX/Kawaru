import os
from typing import Optional


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


def load_constants():
    """集中加载应用所需的常量"""
    from constants.file_constants import CLEANUP_DOWNLOAD_ZIP_INTERVAL_SECONDS  # pylint: disable=import-outside-toplevel
    # 后续可在此添加更多常量导入，例如：
    # from constants.experiment_constants import SOME_EXPERIMENT_CONSTANT
    # from constants.user_constants import SOME_USER_CONSTANT
 
    return {
        'cleanup_download_zip_interval_seconds': CLEANUP_DOWNLOAD_ZIP_INTERVAL_SECONDS,
        # 添加更多常量键值对
    }


def init_celery(constants: dict):
    """显式创建 celery app"""
    import background.celery_app # pylint: disable=import-outside-toplevel

    if background.celery_app.celery_app is None:
        from boot.config import get_config # pylint: disable=import-outside-toplevel
        background.celery_app.celery_app = background.celery_app.celery_create(get_config(), constants)

    return background.celery_app.celery_app


BOOTSTRAP_SUMMARIZED = False  # 标记是否已输出启动概要，防止重复输出启动概要


def bootstrap():
    """统一显式初始化 Env + Logger + Config幂等"""

    ENV = init_env() # pylint: disable=C0103
    CONFIG = init_config() # pylint: disable=C0103
    CONSTANTS = load_constants() # pylint: disable=C0103
    init_logging()
    CELERY_APP = init_celery(CONSTANTS) # pylint: disable=C0103


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
