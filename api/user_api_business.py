import os
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel.ext.asyncio.session import AsyncSession

from database.redis_client import connect_new_redis
from api.user_api_helper import get_user
from api.auth_api import verify_password
from models.user_models import UserInDB
from models.auth import SECRET_KEY
from models.auth import RedisLoginData
from boot.config import get_config

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
