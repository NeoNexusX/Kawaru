"""Tools relevant to identifier generation"""

import secrets
from typing import Annotated

from fastapi import Path

# public_id 字母表：base62（数字 + 大小写字母），URL 中无需转义
PUBLIC_ID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

# public_id 长度：62^16 ≈ 2^95，暴力枚举不可行（防 URL 枚举爬取）
PUBLIC_ID_LENGTH = 16

# public_id 格式校验正则：与生成规则同源（base62 字母表 + 固定长度）
PUBLIC_ID_PATTERN = rf"^[0-9A-Za-z]{{{PUBLIC_ID_LENGTH}}}$"

# public_id 路径参数类型：格式不符由 FastAPI 直接返回 422，不查库
PublicIdPath = Annotated[str, Path(pattern=PUBLIC_ID_PATTERN)]


def generate_public_id() -> str:
    """生成随机 public_id（base62 随机串，16 位）

    用作集合公开页 URL 的对外标识，替代自增 id 与 32 位 uuid4 hex：
    URL 长度减半，同时保持不可枚举（secrets 为密码学安全随机源）。
    唯一性由列上的唯一索引兜底。
    """
    return "".join(secrets.choice(PUBLIC_ID_ALPHABET) for _ in range(PUBLIC_ID_LENGTH))
