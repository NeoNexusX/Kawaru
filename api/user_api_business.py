
from sqlmodel.ext.asyncio.session import AsyncSession


from api.user_api_helper import get_user, validate_url, get_user_by_email
from api.auth_api import verify_password, hashed_password

from models.user_models import UserInDB
from constants.user_constants import ALLOWED_POSITION_TYPES, ALLOWED_REGIONS
from exceptions.user_exceptions import (
    user_name_bad_exception,
    user_position_not_allowed_exception,
    user_region_not_exist_exception,
    user_orcid_not_exist_exception,
    user_homepage_invalid_exception,
    email_already_exists_exception,
)


from background.api_background import validate_orcid_task
from tools.logger import get_logger
logger = get_logger("api.user_api_business")


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
        # 使用 Celery 任务异步执行 ORCID 验证
        result = validate_orcid_task.delay(orcid).get(timeout=120) # type: ignore
        return result
    except Exception as exc: # pylint: disable=broad-exception-caught
        logger.error(f"ORCID validation task failed for {orcid}: {exc}")
        return False


async def validate_update_user_data(user_data: dict, user_past: UserInDB, async_session: AsyncSession) -> dict:
    """
    校验更新用户数据的有效性，并处理密码更新
    """
    # 如果修改的用户并非当前已登录用户（通过用户名判断），则抛出异常
    if user_data.get("username") is None or user_past.username != user_data.get("username"):
        raise user_name_bad_exception

    # 校验 email 是否重复 TODO 只需要在这里验证验证码
    if "email" in user_data and user_data["email"]:
        existing_user = await get_user_by_email(user_data["email"], async_session)
        if existing_user and existing_user.id != user_past.id:
            raise email_already_exists_exception

    # 校验 position
    if "position" in user_data and user_data["position"] not in ALLOWED_POSITION_TYPES:
        raise user_position_not_allowed_exception

    # 校验 region
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
