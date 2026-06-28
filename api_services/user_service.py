#########################
# User Service - 业务逻辑层
# 职责：用户相关的数据库查询与业务编排。无 FastAPI 依赖。
#########################
import secrets
from typing import Any

from sqlmodel import select, func, SQLModel, distinct
from sqlmodel.sql.expression import SelectOfScalar
from sqlmodel.ext.asyncio.session import AsyncSession

from api_helpers.user_helper import validate_url
from api_helpers.auth_helper import verify_password, hashed_password
from models.user_models import UserInDB
from constants.user_constants import ALLOWED_POSITION_TYPES, ALLOWED_REGIONS, RESET_CODE_TTL_SECONDS
from database.redis_client import redis_client
from boot.config import get_config
from exceptions.user_exceptions import (
    user_name_bad_exception,
    user_position_not_allowed_exception,
    user_region_not_exist_exception,
    user_orcid_not_exist_exception,
    user_homepage_invalid_exception,
    email_already_exists_exception,
    reset_code_invalid_exception,
    reset_password_too_many_attempts_exception,
    email_send_too_frequent_exception,
)
from background.api_background import validate_orcid_task
from celery_runtime.result import wait_for_result
from tools.logger import get_logger

logger = get_logger("api_services.user_service")


#########################
# 查询
#########################
async def get_user(username: str, async_session: AsyncSession) -> UserInDB | None:
    """通过 Session 查询指定用户名的用户（可返回未激活用户）"""
    statement = select(UserInDB).where(UserInDB.username == username)
    return (await async_session.exec(statement)).first()


async def get_user_by_id(id: int, async_session: AsyncSession) -> UserInDB | None: # pylint: disable=redefined-builtin
    """通过 Session 查询指定用户ID的用户（可返回未激活用户）"""
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


#########################
# 业务
#########################
async def authenticate_user(username: str, password: str, async_session: AsyncSession) -> UserInDB | None:
    """
    校验用户名/密码并返回用户对象。
    为了安全（防枚举），统一返回 False 而不是抛出异常。
    """
    # 1. 直接查库，如果用户不存在直接返回 False
    user = await get_user(username, async_session)
    if not user:
        return None

    # 2. 检查激活状态
    if not user.active:
        return None

    # 3. 检查密码
    if not verify_password(password, user.hashed_password):
        return None

    return user


async def validate_orcid(orcid: str) -> bool:
    """
    校验用户ORCID。
    存在:返回True;
    不存在或网络异常:返回False;
    """
    try:
        task_result = validate_orcid_task.delay(orcid) # type: ignore
        result = await wait_for_result(task_result, poll_interval=0.1)
        return bool(result)
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error(f"ORCID validation task failed for {orcid}: {exc}")
        return False


#########################
# 密码重置验证码（与邮箱绑定，存 Redis，单次有效）
#########################
def _reset_code_key(email: str) -> str:
    """密码重置验证码在 Redis 中的 key，按邮箱隔离"""
    return f"reset_code:{email}"


def _reset_attempt_key(email: str) -> str:
    """密码重置验证码校验失败计数在 Redis 中的 key，按邮箱隔离"""
    return f"reset_attempt:{email}"


def _email_send_cooldown_key(email: str) -> str:
    """验证码邮件发送冷却标记在 Redis 中的 key，按邮箱隔离、不区分用途"""
    return f"email_send_cd:{email}"


async def enforce_send_cooldown(email: str) -> None:
    """
    发送侧统一限流：同一邮箱在冷却窗口内只放行一次，不区分发送用途，防邮件轰炸。
    用 SET NX EX 原子地"占位"，占位失败说明仍在冷却期。
    """
    cooldown = get_config().email.email_send_cooldown_seconds
    acquired = await redis_client.set(
        _email_send_cooldown_key(email), "1", nx=True, ex=cooldown
    ) # type: ignore
    if not acquired:
        raise email_send_too_frequent_exception


async def generate_and_store_reset_code(email: str) -> str:
    """
    为指定邮箱生成 6 位随机重置验证码并写入 Redis（覆盖旧码、重置失败计数）。
    与全局 TOTP 不同，该码与邮箱绑定、单次有效，避免横向越权与重放。
    发送频率限制由 enforce_send_cooldown 在发送入口统一处理。
    """
    code = f"{secrets.randbelow(10 ** 6):06d}"
    await redis_client.set(_reset_code_key(email), code, ex=RESET_CODE_TTL_SECONDS) # type: ignore
    # 新一轮重置开始，清空上一轮的失败计数
    await redis_client.delete(_reset_attempt_key(email)) # type: ignore
    return code


async def verify_reset_code(email: str, code: str) -> None:
    """
    校验邮箱重置验证码。
    - 码不存在/已过期：抛 reset_code_invalid_exception
    - 失败次数超过配置上限：抛 reset_password_too_many_attempts_exception，并作废本轮码
    - 码错误：计数 +1 后抛 reset_code_invalid_exception
    - 校验通过：删除码与计数，防止重放
    """
    code_key = _reset_code_key(email)
    attempt_key = _reset_attempt_key(email)

    stored = await redis_client.get(code_key) # type: ignore
    if stored is None:
        raise reset_code_invalid_exception

    # 先累加尝试次数（首次设置与验证码同寿命的 TTL），再判断是否超限
    attempts = await redis_client.incr(attempt_key) # type: ignore
    if attempts == 1:
        await redis_client.expire(attempt_key, RESET_CODE_TTL_SECONDS) # type: ignore
    if attempts > get_config().email.reset_password_max_attempts:
        await redis_client.delete(code_key) # type: ignore  作废本轮码，强制重新申请
        raise reset_password_too_many_attempts_exception

    # 定长比较，防时序侧信道
    if not secrets.compare_digest(stored, code):
        raise reset_code_invalid_exception

    # 校验通过：清理码与计数
    await redis_client.delete(code_key) # type: ignore
    await redis_client.delete(attempt_key) # type: ignore


async def validate_update_user_data(user_data: dict, user_past: UserInDB, async_session: AsyncSession) -> dict:
    """
    校验更新用户数据的有效性，并处理密码更新
    """
    # 如果修改的用户并非当前已登录用户（通过用户名判断），则抛出异常
    if user_data.get("username") is None or user_past.username != user_data.get("username"):
        raise user_name_bad_exception

    # 校验 email 是否重复
    if "email" in user_data and user_data["email"]:
        existing_user = await get_user_by_email(user_data["email"], async_session)
        if existing_user and existing_user.id != user_past.id:
            raise email_already_exists_exception

    # 校验 position
    if "position" in user_data and user_data["position"] not in ALLOWED_POSITION_TYPES:
        raise user_position_not_allowed_exception

    # 校验 region（ISO2代码）
    if "region" in user_data and user_data["region"] not in ALLOWED_REGIONS:
        raise user_region_not_exist_exception

    # 校验 orcid
    if "orcid" in user_data and user_data["orcid"]:
        if not await validate_orcid(user_data["orcid"]):
            raise user_orcid_not_exist_exception

    # 校验 homepage
    if "homepage" in user_data and user_data["homepage"]:
        if not validate_url(user_data["homepage"]):
            raise user_homepage_invalid_exception

    # 处理密码更新
    if "password" in user_data and user_data["password"] is not None:
        user_past.hashed_password = hashed_password(user_data["password"])
        user_data.pop("password")

    return user_data
