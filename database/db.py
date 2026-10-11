import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlalchemy import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from boot.config import get_config
from tools.logger import get_logger

logger = get_logger("database")

_ASYNC_ENGINE = None

"""
    # DigitalOcean 等云数据库通常需要 SSL 连接，添加 ssl 参数
    # 注意：这里假设服务器证书是可信的，或者不验证证书，视具体安全需求而定
"""

def _check_database_env():
    """检查数据库相关环境变量是否存在"""
    db_password = os.getenv('DATABASE_PASSWORD')
    if not db_password:
        logger.error("DATABASE_PASSWORD environment variable is not set")
        raise ValueError("DATABASE_PASSWORD environment variable is required")
    return db_password


def _build_database_url(config, db_password: str) -> URL:
    """用 URL.create 构造连接 URL：密码含 @ : / # 等特殊字符时由 SQLAlchemy 转义，与迁移侧（migration_config）口径一致"""
    return URL.create(
        drivername=f"{config.database.db_type}+asyncmy",
        username=config.database.db_username,
        password=db_password,
        host=config.database.host,
        port=config.database.port,
        database=config.database.db_name,
        query={"charset": "utf8mb4"},
    )


def get_async_engine(standalone: bool = False): #TODO celery可以和主进程分开
    """
    加载异步引擎

    :param standalone: 是否独立运行，避免使用全局单例，适用于celery子进程中的数据库操作

    :return: 异步引擎实例
    """
    if standalone:
        config = get_config()
        db_password = _check_database_env()

        _async_engine = create_async_engine(
            _build_database_url(config, db_password),
            pool_pre_ping=True,
            pool_size=config.database.pool_size,# 连接池大小
            max_overflow=config.database.max_overflow, # 最大溢出连接数
            echo_pool=config.database.echo_pool, # 不输出连接池日志（生产环境建议False）
            pool_recycle=config.database.pool_recycle,# 连接回收时间（秒），防止 MySQL 断开连接
            # connect_args={"ssl": True}  # 暂时禁用 SSL 以解决连接中断问题
        )
        logger.info("Standalone async database engine initialized")
        return _async_engine

    global _ASYNC_ENGINE # pylint: disable=global-statement
    if _ASYNC_ENGINE is None:
        config = get_config()
        db_password = _check_database_env()

        _ASYNC_ENGINE = create_async_engine(
            _build_database_url(config, db_password),
            pool_pre_ping=True,
            pool_size=config.database.pool_size,# 连接池大小
            max_overflow=config.database.max_overflow, # 最大溢出连接数
            echo_pool=config.database.echo_pool, # 不输出连接池日志（生产环境建议False）
            pool_recycle=config.database.pool_recycle,# 连接回收时间（秒），防止 MySQL 断开连接
            # connect_args={"ssl": True}  # 暂时禁用 SSL 以解决连接中断问题
        )
        logger.info("Async database engine initialized")

    return _ASYNC_ENGINE


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    获取异步数据库会话
    """
    try:
        engine = get_async_engine()
        async with AsyncSession(engine) as session:
            yield session

    except SQLAlchemyError as e:
        logger.error(f"Database connection error: {e}")
        raise


@asynccontextmanager
async def get_standalone_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Celery 等独立场景使用的短生命周期会话。

    每次进入上下文都会创建独立 engine；退出时无论成功或异常都主动 dispose 连接池，
    避免 standalone engine 累积导致 MySQL 连接数打满。
    """
    engine = get_async_engine(standalone=True)
    try:
        async with AsyncSession(engine) as session:
            yield session
    finally:
        await engine.dispose()
        logger.debug("Standalone async engine disposed")


# 直接将Session对get_async_session()的依赖写进AsyncSessionDep类型签名，方便后续调用
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session)]
