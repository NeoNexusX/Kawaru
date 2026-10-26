#########################
# Auth Helper - 纯工具函数
# 职责：密码哈希/校验、JWT 生成。无 DB、无 FastAPI 依赖。
#########################
from typing import Union
from datetime import timedelta, datetime, timezone
import jwt

from anyio import to_thread

from models.auth import PWD_CONTEXT, SECRET_KEY
from boot.config import get_config


def hashed_password(password: str) -> str:
    """将明文密码加密为哈希值（同步阻塞 ~200ms，仅限启动等非请求路径）。"""
    return PWD_CONTEXT.hash(password)


def verify_password(plain_pwd: str, hashed_pwd: str) -> bool:
    """校验密码与哈希密码是否匹配（同步阻塞 ~200ms，仅限非请求路径）。"""
    return PWD_CONTEXT.verify(plain_pwd, hashed_pwd)


# bcrypt 是 ~200ms 级的 CPU 密集同步操作，绝不能在事件循环线程里直接跑：
# 100 并发登录时循环被连续的 bcrypt 串行阻塞，DB/Redis 的等待被饿死，
# 实测在 2 核 CI 上把 redis 连接池挤到打满、单请求拿到 500（2026-10 CI 事故）。
# 请求路径一律用 *_async 版本，经 anyio 线程池执行（默认 40 并发额度，超额排队）。
async def hashed_password_async(password: str) -> str:
    """hashed_password 的异步版：注册/改密等请求路径使用。"""
    return await to_thread.run_sync(PWD_CONTEXT.hash, password)


async def verify_password_async(plain_pwd: str, hashed_pwd: str) -> bool:
    """verify_password 的异步版：登录等请求路径使用。"""
    return await to_thread.run_sync(PWD_CONTEXT.verify, plain_pwd, hashed_pwd)


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    """生成带有过期时间的 JWT 访问令牌。"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=get_config().token.algorithm)
    return encoded_jwt
