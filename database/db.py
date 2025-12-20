from typing import Annotated
from fastapi import Depends
from sqlmodel import SQLModel, create_engine, Session
from envset.config import get_config

config = get_config()

# read in settings
sqlite_file_name = config.database.path + config.database.name + ".db"

# create database engine, 
# database storage path given by 'storage.root' + 'database.path' in 'config.yaml'
engine = create_engine(url=f"sqlite:///{sqlite_file_name}",
                       connect_args={"check_same_thread": config.database.thread})


def get_session():
    with Session(engine) as session:
        yield session

# automatically create database and tables for the first time
def create_db_and_tables():
    '''应用启动时自动根据模型建立数据库和数据表'''
    SQLModel.metadata.create_all(engine)

# 直接将Session对get_session()的依赖写进SessionDep类型签名，方便后续调用
SessionDep = Annotated[Session, Depends(get_session)]
