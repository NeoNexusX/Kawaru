import os
from typing import Annotated, AsyncGenerator
from fastapi import Depends
from sqlmodel import SQLModel, create_engine, Session
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from envset.config import get_config
from envset.envset import EnvSet

config = get_config()
db_name = os.getenv('DATABASE_NAME')
db_host = os.getenv('DATABASE_HOST')
db_port = os.getenv('DATABASE_PORT')
db_username = os.getenv('DATABASE_USERNAME')
db_password = os.getenv('DATABASE_PASSWORD')

# sync engine for startup building database and tables
sync_engine = create_engine(
    f"mariadb+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    f"?charset=utf8mb4",
    pool_pre_ping=True
)

# async engine for query
engine = create_async_engine(
                        f"mariadb+asyncmy://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
                        f"?charset=utf8mb4",
                        pool_pre_ping=True
                      )


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(engine) as session:
        yield session

def get_sync_session():
    with Session(sync_engine) as session:
        yield session

# automatically create database and tables for the first time
def create_db_and_tables():
    '''应用启动时自动根据模型建立数据库和数据表'''
    SQLModel.metadata.create_all(sync_engine)

# 直接将Session对get_session()的依赖写进SessionDep类型签名，方便后续调用
SessionDep = Annotated[Session, Depends(get_session)]
SyncSessionDep = Annotated[Session, Depends(get_sync_session)]
