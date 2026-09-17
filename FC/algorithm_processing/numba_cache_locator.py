# -*- coding: utf-8 -*-
"""使用源码内容哈希校验跨 FC 实例共享的 Numba 缓存（当前 Numba 0.65.1）。"""

import hashlib
from functools import cache
from pathlib import Path

from numba.core.caching import UserProvidedCacheLocator


@cache
def _source_sha256(source_file: str) -> str:
    """计算只读 Layer 源码的稳定内容指纹。"""
    return hashlib.sha256(Path(source_file).read_bytes()).hexdigest()


class ContentHashCacheLocator(UserProvidedCacheLocator):
    """使用源码 SHA256 代替文件修改时间判断缓存是否过期。"""

    def get_source_stamp(self) -> str:
        return _source_sha256(self._py_file)
