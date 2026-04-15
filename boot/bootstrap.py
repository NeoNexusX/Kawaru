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
    from constants.file_constants import CLEANUP_DOWNLOAD_ZIP_INTERVAL_SECONDS, CLEANUP_PREFLIGHTS_INTERVAL_SECONDS  # pylint: disable=import-outside-toplevel
    # 后续可在此添加更多常量导入，例如：

    return {
        'cleanup_download_zip_interval_seconds': CLEANUP_DOWNLOAD_ZIP_INTERVAL_SECONDS,
        'cleanup_stale_preflights_interval_seconds': CLEANUP_PREFLIGHTS_INTERVAL_SECONDS,
        # 添加更多常量键值对
    }


def init_celery(const_kwargs: dict):
    """
    显式创建 celery app
    """
    from boot.config import get_config  # pylint: disable=import-outside-toplevel
    from celery_runtime.app import create_celery_app  # pylint: disable=import-outside-toplevel
    from celery_runtime.state import get_celery_app, set_celery_app  # pylint: disable=import-outside-toplevel

    try:
        return get_celery_app()
    except RuntimeError:
        set_celery_app(
            create_celery_app(
                get_config(),
                const_kwargs,
            )
        )
        return get_celery_app()


def init_oss():
    """显式初始化 oss 客户端"""
    import storage_backend.oss_client as oss_client # pylint: disable=import-outside-toplevel

    client = oss_client.create_oss_client()

    return client


BOOTSTRAP_SUMMARIZED = False  # 标记是否已输出启动概要，防止重复输出启动概要


def bootstrap():
    """统一显式初始化 Env + Logger + Config幂等"""

    ENV = init_env() # pylint: disable=C0103
    CONFIG = init_config() # pylint: disable=C0103
    CONSTANTS = load_constants() # pylint: disable=C0103
    init_logging()

    if CONFIG.config is None:
        raise RuntimeError("CONFIG.config is not initialized")

    OSS_CLIENT =  init_oss() if "oss" in CONFIG.config.storage.allowed_storage_types else None# pylint: disable=C0103
    CELERY_APP = init_celery(CONSTANTS) # pylint: disable=C0103

    #log part
    from tools.logger import get_logger, LOG_LEVELS   # pylint: disable=import-outside-toplevel
    from celery_runtime.runtime import celery_summary # pylint: disable=import-outside-toplevel
    from storage_backend.oss_client import init_oss_summary  # pylint: disable=import-outside-toplevel

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
        init_oss_summary(OSS_CLIENT)  # 输出 oss 客户端摘要
        celery_summary(CELERY_APP, CONFIG.config)
        BOOTSTRAP_SUMMARIZED = True
