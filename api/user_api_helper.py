from typing import Any
from urllib.parse import urlparse
from sqlmodel import select, func, SQLModel, distinct
from sqlmodel.sql.expression import SelectOfScalar
from sqlmodel.ext.asyncio.session import AsyncSession
from models.user_models import UserInDB

async def get_user(username: str, async_session: AsyncSession) -> UserInDB | None:
    """通过 Session 查询指定用户名的用户（可返回未激活用户）"""
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.username == username)
    return (await async_session.exec(statement)).first()


async def get_user_by_id(id: int, async_session: AsyncSession) -> UserInDB | None: # pylint: disable=redefined-builtin
    """通过 Session 查询指定用户ID的用户（可返回未激活用户）"""
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.id == id)
    return (await async_session.exec(statement)).first()


async def get_user_by_email(email: str, async_session: AsyncSession) -> UserInDB | None:
    """通过 Session 查询指定邮箱的用户（可返回未激活用户）"""
    statement = select(UserInDB).where(UserInDB.email == email)
    return (await async_session.exec(statement)).first()


# TODO: 已有代码中需要根据查询条件统计记录数量时，可替换为该函数
async def count_items(
    async_session: AsyncSession,
    query: SelectOfScalar[Any],
    model: type[SQLModel] | None = None,
    distinct_on_pk: bool = False
) -> int:
    """
    根据查询条件统计记录数量
    ## Args:
    - async_session: 异步数据库会话
    - query: SQLModel 查询语句 (如: select(UserInDB).where(UserInDB.username == username))
    - model: 可选,指定要统计的模型(默认从查询中提取，复杂查询情况下建议指定模型)
 ## Returns:
    int: 记录数量
    """
    # 构建计数查询：从原查询中提取 WHERE 条件，只计数不取数据
    if model is None:
        # column_descriptions 包含了 select() 中定义的所有实体信息
        if query.column_descriptions:
            # 拿到第一个实体的类型 (比如 UserInDB)
            entity = query.column_descriptions[0].get("entity")
            if isinstance(entity, type) and issubclass(entity, SQLModel):
                model = entity

    # 如果还是没拿到，再用底层方法兜底（这时候可能需要 type: ignore 或者 cast）
    if model is None:
        model = query.get_final_froms()[0]  # type: ignore

    # 如果需要去重，根据主键添加 DISTINCT 子句
    if distinct_on_pk:
        pk_column = model.__table__.primary_key.columns[0] # type: ignore
        count_query = select(func.count(distinct(pk_column))) # pylint: disable=not-callable
    else:
        count_query = select(func.count())  # pylint: disable=not-callable # type: ignore

    count_query = count_query.select_from(model) # type: ignore

    # 复制原查询的 JOIN 条件
    for _join in query._setup_joins: # pylint: disable=protected-access
        target, onclause, _, flags = _join
        count_query = count_query.join(
            target, # type: ignore
            onclause, # type: ignore
            isouter = flags.get("isouter", False) if flags else False,
            full = flags.get("full", False) if flags else False,
        )

    # 复制原查询的 WHERE 条件
    if query.whereclause is not None:
        count_query = count_query.where(query.whereclause)

    # 执行查询并返回结果
    result = await async_session.exec(count_query)
    return result.one()


def validate_url(url: str) -> bool:
    """
    校验url是否合法。
    合法:返回True;不合法:返回False
    """
    p = urlparse(url)
    if p.scheme not in {'http', 'https'} or not p.netloc:
        return False
    return True
