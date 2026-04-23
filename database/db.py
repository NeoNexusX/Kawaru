import os
from typing import Annotated, AsyncGenerator
from fastapi import Depends
from sqlmodel import SQLModel, create_engine, Session
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from boot.config import get_config
from tools.logger import get_logger

logger = get_logger("database")

_SYNC_ENGINE = None
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


def get_sync_engine():
    """懒加载同步引擎"""
    global _SYNC_ENGINE # pylint: disable=global-statement

    if _SYNC_ENGINE is None:
        config = get_config()
        db_password = _check_database_env()
        db_url = (
            f"{config.database.db_type}+pymysql://{config.database.db_username}:{db_password}"
            f"@{config.database.host}:{config.database.port}/{config.database.db_name}"
            f"?charset=utf8mb4"
        )
        _SYNC_ENGINE = create_engine(db_url,
                                    pool_size=config.database.pool_size,# 连接池大小
                                    max_overflow=config.database.max_overflow, # 最大溢出连接数
                                    echo_pool=config.database.echo_pool, # 不输出连接池日志（生产环境建议False）
                                    pool_recycle=config.database.pool_recycle,# 连接回收时间（秒），防止 MySQL 断开连接
                                    pool_pre_ping=True)
        logger.info("Sync database engine initialized")

    return _SYNC_ENGINE


def get_async_engine(standalone: bool = False): #TODO celery和主进程分开
    """
    加载异步引擎

    :param standalone: 是否独立运行，避免使用全局单例，适用于celery子进程中的数据库操作

    :return: 异步引擎实例
    """
    if standalone:
        config = get_config()
        db_password = _check_database_env()

        _async_engine = create_async_engine(
            f"{config.database.db_type}+asyncmy://{config.database.db_username}:{db_password}"
            f"@{config.database.host}:{config.database.port}/{config.database.db_name}"
            f"?charset=utf8mb4",
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
            f"{config.database.db_type}+asyncmy://{config.database.db_username}:{db_password}"
            f"@{config.database.host}:{config.database.port}/{config.database.db_name}"
            f"?charset=utf8mb4",
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
        raise e


def get_sync_session():
    """
    获取同步数据库会话
    """
    try:
        sync_engine = get_sync_engine()
        with Session(sync_engine) as session:
            yield session

    except SQLAlchemyError as e:
        logger.error(f"Database connection error: {e}")
        raise e


async def ensure_database_exists():
    """
    检查并创建数据库,独立构建引擎连接到系统数据库执行 CREATE DATABASE 语句，避免权限问题导致的连接失败
    """
    logger.info("Ensuring database exists...")
    config = get_config()
    db_password = _check_database_env()
    db_name = config.database.db_name

    # 连接到系统数据库，用于创建目标数据库
    sys_db_url = (
        f"{config.database.db_type}+asyncmy://{config.database.db_username}:{db_password}"
        f"@{config.database.host}:{config.database.port}/mysql?charset=utf8mb4"
    )

    # DDL 语句推荐在 AUTOCOMMIT 模式下执行
    # 暂时禁用 SSL 以解决连接中断问题
    temp_engine = create_async_engine(
        sys_db_url,
        isolation_level="AUTOCOMMIT",
        # connect_args={"ssl": True}
    )
    try:
        async with temp_engine.connect() as conn:
            await conn.execute(
                text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                )
            logger.info(f"Database '{db_name}' ensured")

    except Exception as e:
        # 如果连接数据库失败，可能是权限问题，记录警告但尝试继续（也许库已经存在）
        logger.error(f"Failed to check/create database via {config.database.db_type} db: {e}")
        raise

    finally:
        await temp_engine.dispose()

# automatically create database and tables for the first time
async def create_db_and_tables():
    '''
    应用启动时自动根据模型建立数据库和数据表
    '''
    try:
        await ensure_database_exists()
        engine = get_async_engine()
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        logger.info("Database tables created successfully")

    except SQLAlchemyError as e:
        logger.error(f"Failed to create database tables: {e}")
        raise

# 直接将Session对get_async_session()的依赖写进AsyncSessionDep类型签名，方便后续调用
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session)]
SyncSessionDep = Annotated[Session, Depends(get_sync_session)]
