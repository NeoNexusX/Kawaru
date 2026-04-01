import os
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel.ext.asyncio.session import AsyncSession

from database.redis_client import connect_new_redis
from api.user_api_helper import get_user, validate_url, get_user_by_email
from api.auth_api import verify_password, hashed_password
from api.file_api_business import (
    get_file_record_by_id,
    assert_file_owner,
    unlink_user_from_file,
    delete_file_and_parts,
)
from api.experiment_api_business import delete_experiment_files
from models.user_models import UserInDB
from models.auth import SECRET_KEY
from models.auth import RedisLoginData
from boot.config import get_config
from constants.user_constants import ALLOWED_POSITION_TYPES, ALLOWED_REGIONS
from exceptions.user_exceptions import (
    user_name_bad_exception,
    user_position_not_allowed_exception,
    user_region_not_exist_exception,
    user_orcid_not_exist_exception,
    user_homepage_invalid_exception,
    email_already_exists_exception,
)
from exceptions.file_exceptions import permission_denied_exception

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


async def check_user_login_status(token: str) -> str | bool:
    """
    用于在任务队列中验证用户登录状态的辅助函数，确保只有登录用户的任务被执行
        - 1. 验证 JWT token 的有效性和过期时间
        - 2. 从 Redis 中查询登录状态，确保 token 对应的用户仍然登录中
        - 3. 确保 Redis 中的登录数据与 JWT token 中的用户信息一致
    ## Args:
        token (str): 用户的 JWT token
    ## Returns:
        str | bool: 如果 token 有效且用户登录状态正常，返回用户名；否则返回 False
    """
    secret_key = os.getenv("SECRET_KEY", SECRET_KEY) # 优先从子进程环境变量获取，确保和主进程一致
    _redis_client = None

    try:
        payload = jwt.decode(
            token, secret_key, algorithms=[get_config().token.algorithm]
        )
        username = payload.get("sub")
        if username is None:
            return False

        # Internally connect to Redis, independent from main process
        redis_cfg = get_config().redis
        _redis_client = connect_new_redis(db_index=redis_cfg.default_db_index)

        # Retrieve login status
        login_key = f"login_token:{token}"
        login_data = await _redis_client.hgetall(login_key) # type:ignore

        # 1. Check if login data exists
        if not login_data:
            return False
        login_data = RedisLoginData(**(login_data))

        # 2. Check if login data correspond to current user
        if login_data.username != username:
            return False

        return username

    except ExpiredSignatureError:
        logger.warning("Token expired while checking user login status.")
        return False

    except InvalidTokenError:
        logger.warning("Invalid token while checking user login status.")
        return False

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error(f"Error while checking user login status: {exc}")
        return False

    finally:
        if _redis_client is not None:
            try:
                await _redis_client.aclose()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to close Redis client after checking login status: %s", exc)


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

    # 校验 email 是否重复
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


async def delete_user_file_business(
    session: AsyncSession,
    file_id: int,
    user: UserInDB,
) -> None:
    """
    删除用户文件的业务逻辑（用于用户删除时清理文件）
    """
    file_record = await get_file_record_by_id(file_id, session)
    if not await assert_file_owner(session, file_record, user):
        raise permission_denied_exception

    # 解除用户与文件的关联
    has_other_owners = await unlink_user_from_file(session, user, file_record)

    if has_other_owners:
        # 还有其他用户持有，只解除关联
        logger.info(f"unlinked file reference only user={user.id} file_id={file_id}")
    else:
        # 没有其他用户持有，执行完全删除
        if file_record.experiment_type == "imzML":
            await delete_experiment_files(session, file_id)

        # 删除文件、分片和数据库记录
        await delete_file_and_parts(session, file_id, user)
        logger.info(f"deleted file completely user={user.id} file_id={file_id}")
