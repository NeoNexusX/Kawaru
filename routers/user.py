from fastapi import APIRouter
from models.auth import Token
from models.email_models import EmailConfirmRequest
from models.user_models import UserPublic,UserDetailPublic
from models.filter_models import ListDataResponse
from api.user_api import (UserLoginDep, TokenDep, UserCreateDep, ListUsersDep,
                          AdminCreateDep,UserUpdateDep, UserDeleDep, UserLogoutDep)
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


@user_router.post("/logout")
async def logout(user: UserLogoutDep) -> UserPublic:
    """
    User log out:
    Method:Post
    Params:Authenticated user
    """
    return user


@user_router.get("/user", response_model=UserDetailPublic)
async def get_current_usr(user: TokenDep):
    """
    Get current user:
    Method:Get
    Params:Authenticated user
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


@user_router.delete("/user_delete/{user_id}")
async def del_user(user: UserDeleDep):
    """
    Delete user by ID (admin only):
    Method:Delete
    Params:path user_id
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


@user_router.post("/list_users", response_model=ListDataResponse)
async def list_users(list_user_response: ListUsersDep):
    """
    Get user list:
    Method:Post
    Params:filter
    """
    return list_user_response
