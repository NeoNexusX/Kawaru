#########################
# API
#########################
import random
from datetime import timedelta
from typing import Annotated
from urllib.parse import urlparse
import jwt
from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette import status
from httpx import AsyncClient, HTTPStatusError, RequestError
from api.auth_api import (
    credentials_exception,
    create_access_token,
    hashed_password,
    verify_password,
    token_invalid_exception,
    token_expired_exception,
)
from exceptions.user_exceptions import (
    identity_not_allowed_exception,
    user_already_exists_exception,
    user_homepage_invalid_exception,
    user_name_bad_exception,
    user_not_active_exception,
    user_orcid_not_exist_exception,
    user_position_not_allowed_exception,
    user_region_not_exist_exception,
    user_verification_bad_exception,
)
from database.db import SessionDep, get_session
from envset.config import get_config, ALLOWED_REGIONS

from logger import get_logger
from models.auth import Token, oauth2_scheme, SECRET_KEY
from models.email_models import TOTP
from models.user_models import UserCreate, UserInDB, UserUpdate, UserPublic

logger = get_logger("main.user_api")


#########################
# 业务层，API 逻辑，不应该抛出任何异常
#########################

async def get_user(username: str, db: SessionDep) -> UserInDB | None:
    """通过 Session 查询指定用户名的用户（可返回未激活用户）"""
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.username == username)
    return (await db.exec(statement)).first()


async def authenticate_user(username: str, password: str, db) -> UserInDB | None:
    """
    校验用户名/密码并返回用户对象。
    为了安全（防枚举），统一返回 False 而不是抛出异常。
    """
    # 1. 直接查库，如果用户不存在直接返回 False
    user = await get_user(username, db)
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
    存在：返回True；不存在或网络异常：返回False
    """
    client = AsyncClient(timeout=100)
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    
    try:
        r = await client.head(url, headers={'Accept': 'application/json'})
        if r.status_code == status.HTTP_200_OK:        # 200
            return True
        return False
    except (HTTPStatusError, RequestError):  # Network Errors
        return False
    
def validate_url(url: str) -> bool:
    """
    校验url是否合法。
    合法：返回True；不合法：返回False
    """
    p = urlparse(url)
    if p.scheme not in {'http', 'https'} or not p.netloc:
        return False
    return True

async def validate_signup_info(user: UserCreate):
    """
    校验用户注册信息是否合法（总校验逻辑）
    """

    config = get_config()
    user_config = config.user
    
    # 1. 验证identity必须为None或空字符串（包括仅空白字符）
    if user.identity and user.identity.strip():
        raise identity_not_allowed_exception
    
    # 2. 验证用户职位必须在职位允许列表内
    allowed_position_types = set(user_config.allowed_position_types)
    if user.position not in allowed_position_types:
        raise user_position_not_allowed_exception
    
    # 3. 验证用户地区是否存在
    if user.region not in ALLOWED_REGIONS:
        raise user_region_not_exist_exception
    
    # 4. 验证用户ORCID是否存在
    if user.orcid:
        is_orcid_exist = await validate_orcid(orcid=user.orcid)
        if not is_orcid_exist:
            raise user_orcid_not_exist_exception
    
    # 6. 验证用户主页是否为有效URL
    if user.homepage and not validate_url(user.homepage):
        raise user_homepage_invalid_exception


#########################
# 依赖层，最顶层 API 逻辑
#########################

async def get_activate_user(username: str, db: SessionDep) -> UserInDB:
    """获取已激活用户，不存在或未激活都会抛出异常"""
    user = await get_user(username, db)

    if user is None or not user.active:
        raise user_not_active_exception

    return user


async def get_authenticated_user(username: str, password: str, db: SessionDep) -> UserInDB:
    """
    校验用户名和密码，获取认证通过的用户对象。

    主要功能：
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
    user = await authenticate_user(username, password, db)
    if user is None:
        raise credentials_exception
    else:
        return user


async def token_authen(token: Annotated[str, Depends(oauth2_scheme)], db: SessionDep):
    """解析并校验 JWT Token 返回当前登录用户"""
    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[get_config().token.algorithm]
        )
        username = payload.get("sub")
        if username is None:
            raise token_invalid_exception

    except ExpiredSignatureError as exc:
        raise token_expired_exception from exc

    except InvalidTokenError as exc:
        raise token_invalid_exception from exc

    user = await get_activate_user(username, db)

    return user


async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: SessionDep
    ) -> Token:

    """用户登录，返回 access token"""
    try:
        user = await get_authenticated_user(form_data.username, form_data.password, db)

        # Create JWT Token
        access_token_expires = timedelta(minutes=get_config().token.token_expire_minutes)
        access_token = create_access_token(data={"sub": user.username},
                                           expires_delta=access_token_expires)
        # return Token
        return Token(access_token=access_token, token_type="bearer")

    except Exception as e:
        logger.error(e)
        # 此处可以添加自定义异常处理
        raise


async def create_user(user: UserCreate, session: SessionDep) -> UserPublic:

    """注册普通用户，自动校验验证码并设置默认身份"""

    # 校验基本的注册信息
    await validate_signup_info(user)

    if TOTP.verify(user.verify_code, valid_window=get_config().email.totp_window):  # 有效期默认±30个时间片=±15分钟
        user_past = await get_user(user.username,session)  # 检查是否已存在用户名相同的用户

        if user_past is None:
            user_data = user.model_dump(exclude={'password'})
            user_data["identity"] = "user"  # 由API设置为普通用户
            user = UserInDB(**user_data,hashed_password=hashed_password(user.password))
            session.add(user)
            await session.commit()
            await session.refresh(user)
        else:
            logger.error("cannot create user ,user already exists, username:%s", user.username)
            raise user_already_exists_exception

        return user # type:ignore

    raise user_verification_bad_exception


# 创建管理员用户的API函数，需要当前用户是管理员才能调用
async def create_admin_user_api(user: UserCreate,
                                current_admin: Annotated[UserInDB, Depends(token_authen)],
                                session: SessionDep,) -> UserPublic:
    """管理员接口：创建新的管理员账号"""

    # 校验基本的注册信息
    await validate_signup_info(user)

    #  TOTP验证
    if TOTP.verify(user.verify_code, valid_window=get_config().email.totp_window):  # 有效期默认±30个时间片=±15分钟
        user_past = await get_user(user.username, session)

        if user_past is None:
            # 排除password，显式设置identity为"admin"
            user_data = user.model_dump(exclude={'password'})
            user_data["identity"] = "admin"  # 由API设置身份为管理员
            user = UserInDB(**user_data, hashed_password=hashed_password(user.password))
            session.add(user)
            await session.commit()
            await session.refresh(user)
            await session.refresh(current_admin)
            logger.info( f"Admin user {current_admin.username} created new admin user {user.username}")

            return user # type:ignore
        else:
            raise user_already_exists_exception
    else:
        raise user_verification_bad_exception


async def update_user(
    user: UserUpdate,
    user_past: Annotated[UserInDB, Depends(token_authen)],
    session: SessionDep,
):
    """
    更新当前用户信息，支持可选字段及密码变更
    """
    user_data = user.model_dump(exclude_unset=True)  # 只更新已传字段
    # 如果修改的用户并非当前已登录用户（通过用户名判断），则抛出异常
    if user_past.username != user.username:
        raise user_name_bad_exception

    # 处理密码更新
    if "password" in user_data and user_data["password"] is not None:
        user_past.hashed_password = hashed_password(user_data["password"])
        user_data.pop("password")

    # 再更新除密码外剩下的用户信息
    user_past.sqlmodel_update(user_data)
    session.add(user_past)
    await session.commit()

    return user


async def del_user(
    user: Annotated[UserInDB, Depends(token_authen)], session: SessionDep
):
    """删除当前登录用户"""
    await session.delete(user)  # 删除当前用户
    await session.commit()  # 提交数据库操作
    return user


async def create_admin_user():
    """应用启动时检查并创建初始管理员用户，异步函数"""
    config = get_config()
    admin_info = config.admin_info
    random_serc = str(random.randint(10**7, 10**8 - 1))  # 随机创建8位初始密码
    user = UserInDB(
        username=config.server.name,
        institution=admin_info.institution,
        position=admin_info.position,
        research_field=admin_info.research_field,
        region=admin_info.region,
        orcid=admin_info.orcid,
        homepage=admin_info.homepage,
        email=admin_info.email,
        identity="admin",
        active=True,
        hashed_password=hashed_password(random_serc),
    )
    session_gen = get_session()
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
UserCreateDep = Annotated[UserCreate, Depends(create_user)]  # 创建普通用户
AdminCreateDep = Annotated[UserPublic, Depends(create_admin_user_api)]  # 新增（管理员用户）
UserUpdateDep = Annotated[UserPublic, Depends(update_user)]  # 更新当前用户信息
UserDeleDep = Annotated[UserPublic, Depends(del_user)]  # 删除当前登录用户
TokenDep = Annotated[UserInDB, Depends(token_authen)]  # 通过解析 JWT Token 返回当前登录用户
