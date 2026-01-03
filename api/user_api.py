#########################
# API
#########################
import random
from datetime import timedelta
from typing import Annotated
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel import select, Session
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette import status
from api.auth_api import credentials_exception, create_access_token, hashed_password, \
    verify_password, token_invalid_exception, token_expired_exception
from database.db import SessionDep, get_session, get_sync_session
from envset.config import get_config

from logger import get_logger
from models.auth import ACCESS_TOKEN_EXPIRE_MINUTES, Token, oauth2_scheme, SECRET_KEY, ALGORITHM, TokenData
from models.email_models import TOTP
from models.user_models import UserCreate, UserInDB, UserUpdate, UserPublic

logger = get_logger("main.user_api")

user_already_exists_exception = HTTPException(
    status_code=status.HTTP_409_CONFLICT,  # 409 Conflict
    detail="Username already exists",  # 明确提示用户已存在
)

user_not_exists_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="Username not exists",  # 明确提示用户不存在
)

user_not_active_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="Username not  activate",  # 明确提示用户没激活
)

user_verification_bad = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="verification code is wrong",  # 明确提示用户验证失败
)

user_name_bad = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,  # 401 Unauthorized
    detail="user is unauthorized",  # 明确提示操作用户不一致
)
identity_not_allowed_exception = HTTPException(#异常定义，identity设为none，不允许传入
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Identity field must be None, it will be set by the API endpoint",
)

admin_create_not_allowed_exception = HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin users can create admin accounts",
        )
# 获取用户基础函数
async def get_user(username: str, db: SessionDep):
    """通过 Session 查询指定用户名的用户（可返回未激活用户）"""
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.username == username)
    return (await db.exec(statement)).first()


# 检查用户是否激活的函数
async def get_activate_user(username: str, db: SessionDep) -> UserInDB:
    """获取已激活用户，不存在或未激活都会抛出异常"""
    user = await get_user(username, db)
    if not user:
        raise user_not_exists_exception
    if not user.active:
        raise user_not_active_exception
    return user


#########################
# token api for user
#########################
async def authenticate_user(username: str, password: str, db: SessionDep):
    """校验用户名/密码并返回用户对象，失败返回 False"""
    user = await get_activate_user(username, db)
    if not verify_password(password, user.hashed_password):
        return False
    return user


async def get_authenticated_user(username: str, password: str, db: SessionDep):
    """包装 authenticate_user，失败时抛出统一凭证异常"""
    user = await authenticate_user(username, password, db)
    if not user:
        raise credentials_exception
    return user


async def token_authen(token: Annotated[str, Depends(oauth2_scheme)], db: SessionDep):
    """解析并校验 JWT Token，返回当前登录用户"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise token_invalid_exception
        token_data = TokenData(username=username)

    except ExpiredSignatureError:
        raise token_expired_exception

    except InvalidTokenError:
        raise token_invalid_exception

    user = await get_activate_user(username, db)

    return user


async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], db: SessionDep) -> Token:
    """用户登录，返回 access token"""
    try:
        user = await get_authenticated_user(form_data.username, form_data.password, db)
        if not user:
            raise credentials_exception
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)

        return Token(access_token=access_token, token_type="bearer")

    except Exception as e:
        logger.error(e)
        # 此处可以添加自定义异常处理
        raise


async def create_user(user: UserCreate, session: SessionDep):
    """注册普通用户，自动校验验证码并设置默认身份"""
    # 1. 验证identity必须为None或空字符串（包括仅空白字符）
    if user.identity and user.identity.strip():
        raise identity_not_allowed_exception
    if TOTP.verify(user.verify_code, valid_window=30): # 有效期±30个时间片=±15分钟

        user_past = await get_user(user.username, session) # 检查是否已存在用户名相同的用户

        if not user_past:
            user_data = user.model_dump(exclude={'password'})
            user_data['identity']="user" #由API设置为普通用户
            user = UserInDB(**user_data, hashed_password=hashed_password(user.password))
            session.add(user)
            await session.commit()
            await session.refresh(user)
        else:
            logger.error("cannot create user")
            raise user_already_exists_exception

        return user
    else:
        raise user_verification_bad

#创建管理员用户的API函数，需要当前用户是管理员才能调用
async def create_admin_user_api(user: UserCreate, current_admin: Annotated[UserInDB, Depends(token_authen)], session: SessionDep):
    """管理员接口：创建新的管理员账号"""

    #  验证当前用户是管理员
    if current_admin.identity != "admin":
        raise admin_create_not_allowed_exception 
    
    #  验证identity必须为None或空字符串（包括仅空白字符）
    if user.identity and user.identity.strip():
        raise identity_not_allowed_exception
    
    #  TOTP验证
    if TOTP.verify(user.verify_code, valid_window=30):
        user_past = await get_user(user.username, session)
        
        if not user_past:
            # 排除password，显式设置identity为"admin"
            user_data = user.model_dump(exclude={'password'})
            user_data['identity'] = "admin"  # 由API设置身份为管理员
            user = UserInDB(**user_data, hashed_password=hashed_password(user.password))
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info(f"Admin user {current_admin.username} created new admin user {user.username}")
            return user
        else:
            raise user_already_exists_exception
    else:
        raise user_verification_bad

async def update_user(user: UserUpdate, user_past: Annotated[UserInDB, Depends(token_authen)], session: SessionDep):
    """
    更新当前用户信息，支持可选字段及密码变更
    """
    user_data = user.model_dump(exclude_unset=True) # 只更新已传字段
    if user_past.username != user.username: # 如果修改的用户并非当前已登录用户（通过用户名判断），则抛出异常
        raise user_name_bad
    
    # 处理密码更新：如果提供了新密码，需要hash后更新hashed_password
    # 先更新hash后密码
    if 'password' in user_data and user_data['password'] is not None:
        user_past.hashed_password = hashed_password(user_data['password'])
        user_data.pop('password')
    
    user_past.sqlmodel_update(user_data) # 再更新除密码外剩下的用户信息
    session.add(user_past)
    await session.commit()
    await session.refresh(user_past)
    return user

async def del_user(user: Annotated[UserInDB, Depends(token_authen)], session: SessionDep):
    """删除当前登录用户"""
    session.delete(user) # 删除当前用户
    await session.commit() # 提交数据库操作
    return user


async def create_admin_user():#此函数只是服务器启动时调用一次创建一个管理员账户，然后其他的没啥用了 (此处感觉用同步应该不影响)
    """应用启动时检查并创建初始管理员用户"""
    config = get_config()
    random_serc = str(random.randint(10 ** 7, 10 ** 8 - 1)) # 随机创建8位初始密码
    user = UserInDB(username=config.server.name,
                    identity="admin",
                    active=True,
                    hashed_password=hashed_password(random_serc),
                    email=config.server.admin_email)
    session = next(get_sync_session())
    user_past = has_admin_user(session, user.username)
    if not user_past:
        session.add(user)
        session.commit()
        session.refresh(user)
        logger.info(f"create admin user {user.username}, password {random_serc}")
    else:
        logger.warning(f"cannot create user,user already exists, username:{user.username}") 


def has_admin_user(session: Session, username: str) -> UserInDB | None:
    """检查指定用户名的管理员是否已存在"""
    statement = select(UserInDB).where((UserInDB.identity == "admin") & (UserInDB.username == username))
    result = session.exec(statement).first()
    return result

##########################
# 以下为依赖注入创建的类型别名
##########################
UserDep = Annotated[UserInDB, Depends(get_activate_user)] # 当前已激活用户
UserLoginDep = Annotated[Token, Depends(login)] # 用户令牌
UserCreateDep = Annotated[UserCreate, Depends(create_user)] # 创建普通用户
AdminCreateDep = Annotated[UserPublic, Depends(create_admin_user_api)]  # 新增（管理员用户）
UserUpdateDep = Annotated[UserPublic, Depends(update_user)] #更新当前用户信息
UserDeleDep = Annotated[UserPublic, Depends(del_user)] # 删除当前登录用户
TokenDep = Annotated[UserInDB, Depends(token_authen)] # 通过解析 JWT Token 返回当前登录用户
