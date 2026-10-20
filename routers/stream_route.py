#########################
# 流式下载端点注册辅助
# 职责: 把返回 Response 字节流的下载 handler 以 GET+HEAD 双注册挂到 router
# 说明: 文件中转与 SI 注释库 CSV 两条流式端点共用（原先各自复制一份注册代码）
#########################
from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Response


def add_stream_download_route(router: APIRouter, path: str, endpoint: Callable[..., Awaitable[Response]]) -> None:
    """注册流式透传下载端点（GET/HEAD 各一条），文件中转与 SI 注释库共用。

    - FastAPI 不允许对 Response 子类使用 Depends 注入，故用 add_api_route
      直接注册带完整签名的处理函数
    - GET/HEAD 拆成两条：共用一条（methods=["GET","HEAD"]）会让两个 operation
      共享同一个 operationId（FastAPI 的 unique_id 只取 methods[0]），触发
      Duplicate Operation ID 警告
    - HEAD 仅供下载工具探测用，不进 OpenAPI 文档（include_in_schema=False）
    """
    router.add_api_route(path, endpoint, methods=["GET"])
    router.add_api_route(path, endpoint, methods=["HEAD"], include_in_schema=False)
