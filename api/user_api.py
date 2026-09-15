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
from sqlmodel import select, col
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.exc import IntegrityError
from api_helpers.auth_helper import create_access_token, hashed_password
from api_helpers.user_helper import validate_url
from api_services.file_service import get_user_file_ids
from api_services.user_service import (
    authenticate_user,
    validate_orcid,
    validate_update_user_data,
    verify_email_code,
    get_user,
    get_user_by_id,
    get_user_by_email,
    count_items,
)

from constants.user_constants import (
    ALLOWED_POSITION_TYPES,
    ALLOWED_REGIONS,
    LOGIN_EXPIRE_SECONDS,
)
from exceptions.user_exceptions import (
    credentials_exception,
    login_expired_exception,
    token_expired_exception,
    token_invalid_exception,
    user_already_exists_exception,
    email_already_exists_exception,
    user_not_active_exception,
    user_position_not_allowed_exception,
    user_region_not_exist_exception,
    user_orcid_not_exist_exception,
    user_homepage_invalid_exception,
    identity_not_allowed_exception,
    user_not_found_exception,
    reset_code_invalid_exception,
)
from exceptions.common_exceptions import (
    non_admin_operation_not_allowed_exception,
    internal_server_error_exception,
    admin_cannot_delete_self_exception,
)
from database.db import AsyncSessionDep, get_async_session
from database.redis_client import redis_client
from celery_runtime.result import wait_for_result
from background.file_background import clean_user_all_task
from boot.config import get_config
from models.auth import Token, RedisLoginData, oauth2_scheme, SECRET_KEY
from models.email_models import EmailPurpose
from models.user_models import UserCreate, UserInDB, UserUpdate, UserPublic, UserDetailPublic, UserQuotaPublic, UserQuotaLimits, PasswordResetConfirm
from models.filter_models import UserFilter, PaginationMeta, ListDataResponse
from tools import utc_time, TIME_STR_FORMAT,get_logger
logger = get_logger("api.user_api")

config = get_config()

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

    # 4. 验证用户地区是否存在（ISO2代码）
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
            token, SECRET_KEY, algorithms=[config.token.algorithm]
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
        access_token_expires = timedelta(minutes=config.token.token_expire_minutes)
        access_token = create_access_token(data={"sub": user.username},
                                           expires_delta=access_token_expires)

        # Create redis login information
        last_access_time = utc_time().strftime(TIME_STR_FORMAT)
        login_key = f"login_token:{access_token}"
        login_data = RedisLoginData(username=user.username,
                                    last_access_time=last_access_time)
        await redis_client.hset(login_key, mapping=login_data.model_dump()) # type:ignore
        await redis_client.expire(login_key, LOGIN_EXPIRE_SECONDS) # set default expiration time

        # return Token
        return Token(access_token=access_token, token_type="bearer")

    except Exception as e:
        logger.error(e)
        raise


async def logout(token: Annotated[str, Depends(oauth2_scheme)], async_session: AsyncSessionDep):
    """用户登出，返回登出的用户信息"""
    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[config.token.algorithm]
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

    user_past = await get_user(user.username,async_session)  # 检查是否已存在用户名相同的用户
    if user_past is not None:
        logger.error("cannot create user ,user already exists, username:%s", user.username)
        raise user_already_exists_exception

    # 验证码必须是发往该注册邮箱的码（单次有效）；放在其余校验之后，避免校验失败白白消耗验证码
    await verify_email_code(EmailPurpose.REGISTER, user.email, user.verify_code)

    user_data = user.model_dump(exclude={'password'})
    user_data["identity"] = "user"  # 由API设置为普通用户
    user = UserInDB(**user_data,hashed_password=hashed_password(user.password)) # type:ignore
    async_session.add(user)
    await async_session.commit()
    await async_session.refresh(user)

    return user # type:ignore


async def create_admin_user_api(user: UserCreate,
                                current_admin: Annotated[UserInDB, Depends(token_authen)],
                                async_session: AsyncSessionDep,) -> UserPublic:
    """管理员接口:创建新的管理员账号"""
    # 校验当前用户是否为管理员
    if current_admin.identity != "admin":
        raise non_admin_operation_not_allowed_exception

    # 校验基本的注册信息（包括邮箱唯一性）
    await validate_signup_info(user, async_session)

    user_past = await get_user(user.username, async_session)
    if user_past is not None:
        raise user_already_exists_exception

    # 验证码必须是发往新管理员邮箱的注册码（单次有效）；放在其余校验之后，避免校验失败白白消耗验证码
    await verify_email_code(EmailPurpose.REGISTER, user.email, user.verify_code)

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


async def update_user(
    user: UserUpdate,
    user_past: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
)-> UserPublic:
    """
    更新当前用户信息，支持可选字段及密码变更
    """
    user_data = user.model_dump(exclude_unset=True)  # 只更新已传字段
    new_email = user_data.get("email")
    verify_code = user_data.pop("verify_code", None)  # 验证码不更新到数据库

    # 校验其他字段
    user_data = await validate_update_user_data(user_data, user_past, async_session)

    # 更新邮箱时校验发往新邮箱的验证码（单次有效）；放在字段校验之后，避免校验失败白白消耗验证码
    if new_email is not None:
        await verify_email_code(EmailPurpose.UPDATE, new_email, verify_code)

    # 再更新除密码外剩下的用户信息
    user_past.sqlmodel_update(user_data)
    async_session.add(user_past)
    await async_session.commit()

    return user # type:ignore


async def reset_password(
    data: PasswordResetConfirm,
    async_session: AsyncSessionDep,
) -> UserPublic:
    """
    忘记密码：未登录用户凭邮箱 + 验证码重置密码。
    验证码与用途、邮箱绑定且单次有效（见 user_service.verify_email_code）。
    """
    user = await get_user_by_email(data.email, async_session)
    # 邮箱不存在或账号未激活时本不该有有效验证码，统一按验证码错误处理，
    # 既禁止未激活账号重置密码，又不泄露邮箱是否注册/是否激活
    if user is None or not user.active:
        raise reset_code_invalid_exception

    await verify_email_code(EmailPurpose.RESET_PASSWORD, data.email, data.verify_code)

    user.hashed_password = hashed_password(data.new_password)
    async_session.add(user)
    await async_session.commit()
    # commit 后对象属性被过期，需 refresh 重新加载，否则序列化为响应模型时
    # 会在异步会话外触发懒加载导致 500
    await async_session.refresh(user)

    return user # type:ignore


async def del_user(
    user_id: int,
    current_admin: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> dict[str, int | str]:
    """删除指定用户（仅管理员可操作）

    不返回 UserPublic：用户行删除并 commit 后 ORM 对象属性已过期，
    在异步会话外序列化会触发懒加载导致 500，且对已删除行 refresh 也会失败。
    与其他删除端点保持一致，返回被删资源标识与状态。
    """

    if current_admin.identity != "admin":
        raise non_admin_operation_not_allowed_exception

    if user_id == current_admin.id:
        raise admin_cannot_delete_self_exception

    user = await get_user_by_id(user_id, async_session)
    if user is None:
        raise user_not_found_exception
    user_id = user.id # type: ignore

    file_ids = await get_user_file_ids(async_session, user.id) # type: ignore
    await async_session.delete(user)
    await async_session.commit()

    if file_ids:
        task = clean_user_all_task.delay(file_ids, user_id)  # type: ignore
        await wait_for_result(task, poll_interval=1.0)

    return {"user_id": user_id, "status": "deleted"}


async def create_admin_user():
    """应用启动时检查并创建初始管理员用户，异步函数"""
    _config = config
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


async def get_user_list(
    page: int,
    size: int,
    filters: UserFilter,
    current_admin: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep
) -> ListDataResponse:
    """获取用户列表"""
    # 校验当前用户是否为管理员
    if current_admin.identity != "admin":
        raise non_admin_operation_not_allowed_exception

    # 根据filters条件构建查询
    query = select(UserInDB).filter(
        col(UserInDB.username).like(f"%{filters.username}%") if filters.username else True, # type:ignore # pylint: disable=no-member
        UserInDB.active == filters.active if filters.active else True, # type:ignore
        UserInDB.institution == filters.institution if filters.institution else True, # type:ignore
        UserInDB.region == filters.region if filters.region else True # type:ignore
    )

    try:
        # 先根据filter条件查询用户数量
        total_records = await count_items(async_session, query)

        # 计算总页数
        total_pages = (total_records + size - 1) // size

        # 分页查询用户列表
        offset = (page - 1) * size

        data = (await async_session.exec(query.offset(offset).limit(size))).all() or None

        # 分页元数据
        pagination_meta = PaginationMeta(
            current_page=page,
            current_records=len(data) if data else 0,
            total_pages=total_pages,
            total_records=total_records
        )

        # 构建响应模型
        list_user_response = ListDataResponse(
            meta=pagination_meta,
            data=[UserDetailPublic(**user.model_dump()) for user in data] if data else []
        )

        return list_user_response

    except Exception as e:
        logger.error(f"failed to get user list: {e}")
        raise internal_server_error_exception from e


async def get_user_quota(
    current_user: Annotated[UserInDB, Depends(token_authen)],
) -> UserQuotaPublic:
    """返回当前用户的配额信息（从用户数据库记录读取）"""

    return UserQuotaPublic(
        total_uploaded_size_bytes=current_user.total_file_size,
        file_count=current_user.file_count,
        max_files_per_user=current_user.max_file_count,
        max_total_file_size=current_user.max_total_file_size,
        max_processing_size_gb=current_user.max_processing_size,
        total_processed_size_bytes=current_user.processed_file_size,
        download_used=current_user.download_used or 0,
        max_download_count=current_user.max_download_count,
    )


async def update_user_quota(
    user_id: int,
    quota: UserQuotaLimits,
    current_admin: Annotated[UserInDB, Depends(token_authen)],
    async_session: AsyncSessionDep,
) -> UserQuotaLimits:
    """管理员接口：按用户 ID 修改指定用户的配额信息"""
    # 校验管理员身份
    if current_admin.identity != "admin":
        raise non_admin_operation_not_allowed_exception

    # 查找目标用户
    user = await get_user_by_id(user_id, async_session)
    if user is None:
        raise user_not_found_exception

    # 只更新传入的非 None 字段
    quota_data = quota.model_dump(exclude_unset=True)
    for field, value in quota_data.items():
        if value is not None:
            setattr(user, field, value)

    async_session.add(user)
    await async_session.commit()
    await async_session.refresh(user)

    logger.info(f"Admin updated quota for user {user.username} (id={user_id}): {quota_data}")

    return UserQuotaLimits(
        max_total_file_size=user.max_total_file_size,
        max_file_count=user.max_file_count,
        max_processing_size=user.max_processing_size,
        max_download_count=user.max_download_count,
    )


##########################
# 以下为依赖注入创建的类型别名
##########################
UserDep = Annotated[UserInDB, Depends(get_activate_user)]  # 获取当前已激活用户
UserLoginDep = Annotated[Token, Depends(login)]  # 用户登陆
UserLogoutDep = Annotated[UserPublic, Depends(logout)]  # 用户登出
UserCreateDep = Annotated[UserCreate, Depends(create_user)]  # 创建普通用户
AdminCreateDep = Annotated[UserPublic, Depends(create_admin_user_api)]  # 新增（管理员用户）
UserUpdateDep = Annotated[UserPublic, Depends(update_user)]  # 更新当前用户信息
ResetPasswordDep = Annotated[UserPublic, Depends(reset_password)]  # 忘记密码：凭邮箱验证码重置
UserDeleDep = Annotated[dict[str, int | str], Depends(del_user)]  # 根据用户 ID 删除指定用户
TokenDep = Annotated[UserInDB, Depends(token_authen)]  # 通过解析 JWT Token 返回当前登录用户
ListUsersDep = Annotated[ListDataResponse, Depends(get_user_list)]  # 获取用户列表依赖
UserQuotaDep = Annotated[UserQuotaPublic, Depends(get_user_quota)]  # 获取用户配额
UpdateUserQuotaDep = Annotated[UserQuotaLimits, Depends(update_user_quota)]  # 管理员修改用户配额
