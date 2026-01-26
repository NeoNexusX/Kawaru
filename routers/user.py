from typing import Annotated
from fastapi import APIRouter, Depends
from models.auth import Token
from models.email_models import EmailConfirmRequest
from models.user_models import UserInDB, UserPublic
from api.user_api import UserLoginDep, token_authen, UserCreateDep, AdminCreateDep,UserUpdateDep, UserDeleDep
from api.email_api import EmailConfirmDep, EmailConfirmSMTPDep

user_router = APIRouter(tags=["user manager"]) # 挂接了用户管理相关的api
#########################
# 用户管理 API
#########################

@user_router.post("/login")
async def login(token: UserLoginDep) -> Token:
    """
    User log in:
    Method:Post
    Params(FormData):username, password
    """
    try:
        return token
    except Exception as e:
        raise e

@user_router.get("/user", response_model=UserPublic)
async def get_current_usr(user: Annotated[UserInDB, Depends(token_authen)]):
    """
    Get current user:
    Method:Get
    Params:N/A
    """
    return user


@user_router.post("/user_signup", response_model=UserPublic)
async def create_user(user: UserCreateDep):
    """
    User sign up:
    Method:Post
    Params(Body):"username", "active", "identity=None", "email", "password", "verify_code"
    """
    return user


@user_router.post("/admin_signup", response_model=UserPublic)#新增管理员用户接口
async def create_admin_user_endpoint(user: AdminCreateDep):
    """
    Admin user sign up:
    Method:Post
    Params(Body):"username", "active", "identity"=“admin”, "email", "password", "verify_code"
    """
    return user


@user_router.post("/user_change", response_model=UserPublic)
async def update_user(user: UserUpdateDep):
    """
    Update user information:
    Method:Post
    Params(Body):"username", "active", "identity"(None), "email", "password"
    """
    return user


@user_router.delete("/user_delete")
async def del_user(user: UserDeleDep):
    """
    Delete current user:
    Method:Delete
    Params:N/A
    """
    return user


@user_router.post("/emailrequest", response_model=EmailConfirmRequest)#MAILGUN邮箱服务
async def create_user_email_server(email: EmailConfirmDep):
    """
    Use MailGun to send verification code:
    Method:Post
    Params(Body):"email", "send_time"
    """
    return email


@user_router.post("/emailsmtprequest", response_model=EmailConfirmSMTPDep)#SMTP邮箱服务
async def create_user_smtp_email_server(email: EmailConfirmSMTPDep):
    """
    Use SMTP to send verification code:
    Method:Post
    Params(Body):"email", "send_time"
    """
    return email
