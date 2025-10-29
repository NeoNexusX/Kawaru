from typing import Annotated
from fastapi import Depends
from sqlmodel import SQLModel, create_engine, Session
from envset.config import get_config

config = get_config()

# read in settings
sqlite_file_name = config.database.path + config.database.name + ".db"

engine = create_engine(url=f"sqlite:///{sqlite_file_name}",
                       connect_args={"check_same_thread": config.database.thread})


def get_session():
    with Session(engine) as session:
        yield session


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


SessionDep = Annotated[Session, Depends(get_session)]

