from urllib.parse import urlparse
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from models.user_models import UserInDB

async def get_user(username: str, async_session: AsyncSession) -> UserInDB | None:
    """通过 Session 查询指定用户名的用户（可返回未激活用户）"""
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.username == username)
    return (await async_session.exec(statement)).first()


async def get_user_by_email(email: str, async_session: AsyncSession) -> UserInDB | None:
    """通过 Session 查询指定邮箱的用户（可返回未激活用户）"""
    statement = select(UserInDB).where(UserInDB.email == email)
    return (await async_session.exec(statement)).first()


def validate_url(url: str) -> bool:
    """
    校验url是否合法。
    合法:返回True;不合法:返回False
    """
    p = urlparse(url)
    if p.scheme not in {'http', 'https'} or not p.netloc:
        return False
    return True
