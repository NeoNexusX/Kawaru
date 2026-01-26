"""Here place the many-to-many relationship link models"""
from sqlmodel import Field, SQLModel

class UserFileLink(SQLModel, table=True):
    """链接表：用于链接用户表和文件表的多对多关系"""
    user_id: int | None = Field(default=None, foreign_key="userindb.id", primary_key=True, ondelete="CASCADE")
    file_id: int | None = Field(default=None, foreign_key="fileindb.file_id", primary_key=True, ondelete="CASCADE")
