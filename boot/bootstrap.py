import os
from typing import Optional

def init_logging(log_level: Optional[str] = None, log_dir: Optional[str] = None) -> None:
    """显式初始化日志系统（必须先于任何 get_logger 调用）"""
    import logger

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
    from boot.envset import EnvSet

    env = EnvSet()

    return env


def init_config():
    """显式读取 config.yaml 并初始化全局 CONFIG"""
    # 必须在 init_logging 之后 import（因为 envset.config 顶层会取 logger）
    import boot.config

    boot.config.CONFIG = boot.config.ConfigCreate()
    if boot.config.CONFIG.config is None:
        boot.config.CONFIG.read()

    return boot.config.CONFIG


def bootstrap(*, validate_env: bool = True):
    """统一显式初始化 Env + Logger + Config幂等"""

    ENV = init_env()
    CONFIG = init_config()
    init_logging()

    #log part
    from logger import get_logger
    logger = get_logger("bootstrap")
    logger.info("runtime environment initialized")

    ENV.summary()
    CONFIG.summary()
