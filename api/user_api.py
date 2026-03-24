#########################
# API
#########################
import random
from datetime import timedelta
from typing import Annotated
import jwt
from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.exc import IntegrityError
from api.auth_api import (
    credentials_exception,
    create_access_token,
    hashed_password,
    login_expired_exception,
    token_invalid_exception,
    token_expired_exception,
)
from api.user_api_business import authenticate_user, validate_orcid, validate_update_user_data
from api.user_api_helper import (
    validate_url,
    get_user,
    get_user_by_email
)
from constants.user_constants import * # noqa: F401, F403 # pylint: disable=W0401, W0614
from constants.file_constants import CLEANUP_PREFLIGHTS_INTERVAL_SECONDS, PREFLIGHT_TTL_MINUTES
from exceptions.user_exceptions import * # noqa: F401, F403 # pylint: disable=W0401, W0614
from database.db import AsyncSessionDep, get_async_session
from database.redis_client import redis_client
from background.celery_app import TaskManager
from boot.config import get_config
from models.auth import Token, RedisLoginData, oauth2_scheme, SECRET_KEY
from models.email_models import TOTP
from models.user_models import UserCreate, UserInDB, UserUpdate, UserPublic
from tools.logger import get_logger
from tools.time_tools import now_local_timezone, TIME_STR_FORMAT

logger = get_logger("api.user_api")


#########################
# 业务层，API 逻辑，不应该抛出任何异常
#########################

async def validate_signup_info(user: UserCreate, async_session: AsyncSession):
    """
    校验用户注册信息是否合法（总校验逻辑）
    """

    # 1. 验证identity必须为None或空字符串（包括仅空白字符）
    if user.identity and user.identity.strip():
        raise identity_not_allowed_exception

    # 2. 验证邮箱是否已被注册
    existing_user = await get_user_by_email(user.email, async_session)
    if existing_user:
        raise email_already_exists_exception

    # 3. 验证用户职位必须在职位允许列表内
    if user.position not in ALLOWED_POSITION_TYPES:
        raise user_position_not_allowed_exception

    # 4. 验证用户地区是否存在
    if user.region not in ALLOWED_REGIONS:
        raise user_region_not_exist_exception

    # 5. 验证用户ORCID是否存在
    if user.orcid:
        is_orcid_exist = await validate_orcid(orcid=user.orcid)
        if not is_orcid_exist:
            raise user_orcid_not_exist_exception

    # 6. 验证用户主页是否为有效URL
    if user.homepage and not validate_url(user.homepage):
        raise user_homepage_invalid_exception


async def get_authenticated_user(username: str, password: str, async_session: AsyncSession) -> UserInDB:
    """
    校验用户名和密码，获取认证通过的用户对象。

    主要功能:
    调用 authenticate_user 进行校验，如果校验失败（返回 False)
    则直接抛出统一的凭证认证异常 (credentials_exception)。

    Args:
        username (str): 用户名
        password (str): 用户密码（明文）
        db (SessionDep): 数据库会话依赖或者db session对象

    Returns:
        UserInDB: 认证成功返回对应的用户数据库对象

    Raises:
        credentials_exception: 当用户名/密码错误、用户不存在或未激活时抛出
    """
    user = await authenticate_user(username, password, async_session)
    if user is None:
        raise credentials_exception
    else:
        return user

#########################
# API层，最顶层 API 逻辑
#########################

async def get_activate_user(username: str, async_session: AsyncSessionDep) -> UserInDB:
    """获取已激活用户，不存在或未激活都会抛出异常"""
    user = await get_user(username, async_session)

    if user is None or not user.active:
        raise user_not_active_exception

    return user


async def token_authen(token: Annotated[str, Depends(oauth2_scheme)], async_session: AsyncSessionDep):
    """解析并校验 JWT Token 返回当前登录用户"""
    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[get_config().token.algorithm]
        )
        username = payload.get("sub")
        if username is None:
            raise token_invalid_exception

        # Retrieve login status
        login_key = f"login_token:{token}"
        login_data = await redis_client.hgetall(login_key) # type:ignore

        # 1. Check if login data exists
        if not login_data:
            raise login_expired_exception
        login_data = RedisLoginData(**(login_data))

        # 2. Check if login data correspond to current user
        if login_data.username != username:
            raise login_expired_exception

        await redis_client.expire(login_key, LOGIN_EXPIRE_SECONDS)

    except ExpiredSignatureError as exc:
        raise token_expired_exception from exc

    except InvalidTokenError as exc:
        raise token_invalid_exception from exc

    user = await get_activate_user(username, async_session)

    return user


async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    async_session: AsyncSessionDep
    ) -> Token:

    """用户登录，返回 access token"""
    try:
        user = await get_authenticated_user(form_data.username, form_data.password, async_session)

        # Create JWT Token
        access_token_expires = timedelta(minutes=get_config().token.token_expire_minutes)
        access_token = create_access_token(data={"sub": user.username},
                                           expires_delta=access_token_expires)

        # Create redis login information
        last_access_time = now_local_timezone().strftime(TIME_STR_FORMAT)
        login_key = f"login_token:{access_token}"
        login_data = RedisLoginData(username=user.username,
                                    last_access_time=last_access_time)
        await redis_client.hset(login_key, mapping=login_data.model_dump()) # type:ignore
        await redis_client.expire(login_key, LOGIN_EXPIRE_SECONDS) # set default expiration time

        # After user login, create a Celery task to clean up staled preflights
        task_name = f"cleanup_stale_preflights_{access_token[0:8]}"  # Use part of token to ensure uniqueness while avoiding too long task name
        await TaskManager.add_interval_task(task_name=task_name,
                                      task_path="background.file_background.cleanup_stale_preflights",
                                      interval_seconds=CLEANUP_PREFLIGHTS_INTERVAL_SECONDS,
                                      # interval_seconds = 10, # for testing
                                      task_kwargs={"user_token": access_token, "ttl_minutes": PREFLIGHT_TTL_MINUTES})

        # return Token
        return Token(access_token=access_token, token_type="bearer")

    except Exception as e:
        logger.error(e)
        raise


async def logout(token: Annotated[str, Depends(oauth2_scheme)], async_session: AsyncSessionDep):
    """用户登出，返回登出的用户信息"""
    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[get_config().token.algorithm]
        )
        username = payload.get("sub")
        if username is None:
            raise token_invalid_exception

        # Retrieve login status
        login_key = f"login_token:{token}"
        login_data = await redis_client.hgetall(login_key) # type:ignore

        # 1. Check if login data exists
        if not login_data:
            raise login_expired_exception
        login_data = RedisLoginData(**(login_data))

        # 2. Check if login data correspond to current user
        if login_data.username != username:
            raise login_expired_exception

        await redis_client.delete(login_key)

        # 3. Remove corresponding tasks
        await TaskManager.remove_task(f"cleanup_stale_preflights_{token[0:8]}") # type:ignore
        logger.warning(f"User {username} has logged out. Task has been removed from schedule.")

    except ExpiredSignatureError as exc:
        raise token_expired_exception from exc

    except InvalidTokenError as exc:
        raise token_invalid_exception from exc

    user = await get_activate_user(username, async_session)

    return user


async def create_user(user: UserCreate, async_session: AsyncSessionDep) -> UserPublic:

    """注册普通用户，自动校验验证码并设置默认身份"""

    # 校验基本的注册信息（包括邮箱唯一性）
    await validate_signup_info(user, async_session)

    if TOTP.verify(user.verify_code, valid_window=get_config().email.totp_window):  # 有效期默认±30个时间片=±15分钟
        user_past = await get_user(user.username,async_session)  # 检查是否已存在用户名相同的用户

        if user_past is None:
            user_data = user.model_dump(exclude={'password'})
            user_data["identity"] = "user"  # 由API设置为普通用户
            user = UserInDB(**user_data,hashed_password=hashed_password(user.password)) # type:ignore
            async_session.add(user)
            await async_session.commit()
            await async_session.refresh(user)
        else:
            logger.error("cannot create user ,user already exists, username:%s", user.username)
            raise user_already_exists_exception

        return user # type:ignore

    raise user_verification_bad_exception


# 创建管理员用户的API函数，需要当前用户是管理员才能调用
async def create_admin_user_api(user: UserCreate,
                                current_admin: Annotated[UserInDB, Depends(token_authen)],
                                async_session: AsyncSessionDep,) -> UserPublic:
    """管理员接口:创建新的管理员账号"""

    # 校验基本的注册信息（包括邮箱唯一性）
    await validate_signup_info(user, async_session)

    #  TOTP验证
    if TOTP.verify(user.verify_code, valid_window=get_config().email.totp_window):  # 有效期默认±30个时间片=±15分钟
        user_past = await get_user(user.username, async_session)

        if user_past is None:
            # 排除password，显式设置identity为"admin"
            user_data = user.model_dump(exclude={'password'})
            user_data["identity"] = "admin"  # 由API设置身份为管理员
            user = UserInDB(**user_data, hashed_password=hashed_password(user.password)) # type:ignore
            async_session.add(user)
            await async_session.commit()
            await async_session.refresh(user)
            await async_session.refresh(current_admin)
            logger.info( f"Admin user {current_admin.username} created new admin user {user.username}")

            return user # type:ignore
        else:
            raise user_already_exists_exception
    else:
        raise user_verification_bad_exception


async def update_user(
    user: UserUpdate,
    user_past: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
)-> UserPublic:
    """
    更新当前用户信息，支持可选字段及密码变更
    """
    user_data = user.model_dump(exclude_unset=True)  # 只更新已传字段

    # 验证邮箱验证码（update 接口 require）
    if not (
        "verify_code" in user_data
        and user_data["verify_code"]
        and TOTP.verify(user_data["verify_code"], valid_window=get_config().email.totp_window)):
        raise user_verification_bad_exception

    # 移除 verify_code，不更新到数据库
    user_data.pop("verify_code")

    # 校验其他字段
    user_data = await validate_update_user_data(user_data, user_past, async_session)

    # 再更新除密码外剩下的用户信息
    user_past.sqlmodel_update(user_data)
    async_session.add(user_past)
    await async_session.commit()

    return user # type:ignore


async def del_user(
    user: Annotated[UserInDB, Depends(token_authen)], async_session: AsyncSessionDep
):
    """删除当前登录用户"""
    await async_session.delete(user)  # 删除当前用户
    await async_session.commit()  # 提交数据库操作
    return user


async def create_admin_user():
    """应用启动时检查并创建初始管理员用户，异步函数"""
    _config = get_config()
    admin_info = _config.admin_info
    # admin_info 如果是 Pydantic/SQLModel 模型，通常有 model_dump()
    admin_data = admin_info.model_dump()  # 如果这里报错，再改用 vars(admin_info) 并过滤字段

    random_serc = str(random.randint(10**7, 10**8 - 1))  # 随机创建8位初始密码
    user = UserInDB(
        **admin_data,
        identity="admin",
        active=True,
        hashed_password=hashed_password(random_serc),
    )

    session_gen = get_async_session()
    session = await anext(session_gen)
    try:
        user_past = await has_admin_user(session, user.username)
        if not user_past:
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info(f"create admin user {user.username}, password {random_serc}")
        else:
            logger.warning(f"cannot create user,user already exists, username:{user.username}")

    except IntegrityError as e:
        logger.error(f"failed to create admin user\n"
                     f"admin user info is not right, please check config file!\n {e}")
        raise

    except Exception as e:
        logger.error(f"failed to create admin user: {e}")
        raise

    finally:
        await session_gen.aclose()


async def has_admin_user(session: AsyncSession, username: str) -> UserInDB | None:
    """检查指定用户名的管理员是否已存在"""
    statement = select(UserInDB).where((UserInDB.identity == "admin") & (UserInDB.username == username))
    result = (await session.exec(statement)).first()
    return result


##########################
# 以下为依赖注入创建的类型别名
##########################
UserDep = Annotated[UserInDB, Depends(get_activate_user)]  # 获取当前已激活用户
UserLoginDep = Annotated[Token, Depends(login)]  # 用户登陆
UserLogoutDep = Annotated[UserPublic, Depends(logout)]  # 用户登出
UserCreateDep = Annotated[UserCreate, Depends(create_user)]  # 创建普通用户
AdminCreateDep = Annotated[UserPublic, Depends(create_admin_user_api)]  # 新增（管理员用户）
UserUpdateDep = Annotated[UserPublic, Depends(update_user)]  # 更新当前用户信息
UserDeleDep = Annotated[UserPublic, Depends(del_user)]  # 删除当前登录用户
TokenDep = Annotated[UserInDB, Depends(token_authen)]  # 通过解析 JWT Token 返回当前登录用户
