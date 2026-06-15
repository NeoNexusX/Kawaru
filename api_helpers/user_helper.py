#########################
# User Helper - 纯工具函数
# 职责：用户相关的纯校验逻辑。无 DB、无 FastAPI 依赖。
#########################
from urllib.parse import urlparse


def validate_url(url: str) -> bool:
    """
    校验url是否合法。
    合法:返回True;不合法:返回False
    """
    p = urlparse(url)
    if p.scheme not in {'http', 'https'} or not p.netloc:
        return False
    return True
