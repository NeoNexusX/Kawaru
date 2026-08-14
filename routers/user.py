import asyncio

from fastapi import APIRouter
from models.auth import Token
from models.user_models import UserPublic,UserDetailPublic, UserQuotaPublic, UserQuotaLimits
from models.filter_models import ListDataResponse
from api.user_api import (UserLoginDep, TokenDep, UserCreateDep, ListUsersDep,
                          AdminCreateDep,UserUpdateDep, ResetPasswordDep, UserDeleDep, UserLogoutDep, UserQuotaDep,
                          UpdateUserQuotaDep)
from api.email_api import EmailConfirmSMTPDep
from database.visit_stats import incr_visit

user_router = APIRouter(tags=["user manager"]) # 挂接了用户管理相关的api
#########################
# 用户管理 API
#########################

@user_router.post("/login")
async def login(token: UserLoginDep) -> Token:
    """
    User log in:
    Method:Post
    Params(OAuth2PasswordRequestForm): username, password
    """
    try:
        asyncio.create_task(incr_visit())
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
    asyncio.create_task(incr_visit())
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


@user_router.post("/password_reset", response_model=UserPublic)
async def reset_password(user: ResetPasswordDep):
    """
    Reset password via email (forgot password, no login required):
    Method:Post
    Params(Body):"email", "verify_code", "new_password"
    先调用 /emailsmtprequest（purpose=reset_password）获取验证码，再调用本接口重置。
    """
    return user


@user_router.delete("/user_delete/{user_id}")
async def del_user(user: UserDeleDep):
    """
    Delete user by ID (admin only):
    Method:Delete
    Params:path user_id
    Returns: {"user_id": ..., "status": "deleted"}
    """
    return user


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


@user_router.get("/users/quota", response_model=UserQuotaPublic)
async def get_user_quota(quota: UserQuotaDep):
    """
    获取当前用户的配额信息

    返回：
    - 上传相关: 单文件大小限额、已上传总大小、文件数、最大文件数、上传总大小限额
    - 处理相关: 处理累计限额、已处理总大小、下载文件数量、最大下载文件数量
    """
    return quota


@user_router.post("/users/{user_id}/quota", response_model=UserQuotaLimits)
async def update_user_quota(result: UpdateUserQuotaDep):
    """
    管理员修改指定用户的配额信息

    Params:
    - user_id: 目标用户 ID（路径参数）
    - Body: UserQuotaLimits（所有字段可选，只更新传入的非 None 字段）
      - max_total_file_size: 用户最大上传文件总大小（GB）
      - max_file_count: 用户最大上传总文件数
      - max_processing_size: 用户最大可算法处理文件总大小（GB）
      - max_download_count: 用户最大可下载文件次数
    返回：更新后四个限额值（UserQuotaLimits）
    """
    return result
