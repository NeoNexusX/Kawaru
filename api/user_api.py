#########################
# API
#########################
from datetime import timedelta
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlmodel import select
from starlette import status

from api.auth_api import credentials_exception, create_access_token, hashed_password, \
    verify_password, token_invalid_exception, token_expired_exception
from database.db import SessionDep
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
    detail="Username not exists",  # 明确提示用户已存在
)

user_not_active_exception = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="Username not  activate",  # 明确提示用户已存在
)

user_verification_bad = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,  # 409 Conflict
    detail="verification code is wrong",  # 明确提示用户已存在
)


# 获取用户基础函数
async def get_user(username: str, db: SessionDep):
    # 通过 Session 查询用户
    statement = select(UserInDB).where(UserInDB.username == username)
    return db.exec(statement).first()


# 检查用户是否激活的函数
async def get_activate_user(username: str, db: SessionDep) -> UserInDB:
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
    user = await get_activate_user(username, db)
    if not verify_password(password, user.hashed_password):
        return False
    return user


async def get_authenticated_user(username: str, password: str, db: SessionDep):
    user = await authenticate_user(username, password, db)
    if not user:
        raise credentials_exception
    return user


async def token_authen(token: Annotated[str, Depends(oauth2_scheme)], db: SessionDep):
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
    try:
        user = await get_authenticated_user(form_data.username, form_data.password, db)
        if not user:
            raise credentials_exception
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)

        return Token(access_token=access_token, token_type="bearer")

    except Exception as e:
        logger.error(e)
        raise


async def create_user(user: UserCreate, session: SessionDep):
    if TOTP.verify(user.verify_code, valid_window=30):

        user_past = await get_user(user.username, session)

        if not user_past:
            user = UserInDB(**user.model_dump(), hashed_password=hashed_password(user.password))
            session.add(user)
            session.commit()
            session.refresh(user)
        else:
            logger.error("cannot create user")
            raise user_already_exists_exception

        return user
    else:
        raise user_verification_bad


async def update_user(user: UserUpdate, user_past: Annotated[UserInDB, Depends(token_authen)], session: SessionDep):
    user_data = user.model_dump(exclude_unset=True)
    user_past.sqlmodel_update(user_data)
    session.add(user_past)
    session.commit()
    session.refresh(user_past)
    return user


async def del_user(user: Annotated[UserInDB, Depends(token_authen)], session: SessionDep):
    session.delete(user)
    session.commit()
    return user


UserDep = Annotated[UserInDB, Depends(get_activate_user)]
UserLoginDep = Annotated[Token, Depends(login)]
UserCreateDep = Annotated[UserCreate, Depends(create_user)]
UserUpdateDep = Annotated[UserPublic, Depends(update_user)]
UserDeleDep = Annotated[UserPublic, Depends(del_user)]
TokenDep = Annotated[UserInDB, Depends(token_authen)]
