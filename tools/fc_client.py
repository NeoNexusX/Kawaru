"""
FC（函数计算）统一 HTTP 调用工具。

所有需要触发 FC 函数的模块应使用 invoke_fc() 而非自行构造 httpx 请求。
"""
from httpx import AsyncClient

from fastapi import status

from tools.logger import get_logger


logger = get_logger("tools.fc_client")


async def invoke_fc(
    url: str | None,
    token: str | None,
    event: dict,
    timeout: float | int | None,
    *,
    async_invocation: bool = True,
) -> str | None:
    """
    统一的 FC HTTP 触发函数。

    Args:
        url: FC 函数的 HTTP 触发器地址
        token: Bearer token
        event: 事件体（将作为 JSON 发送）
        timeout: 请求超时（秒），仅控制发送耗时，不等待 FC 执行完成
        async_invocation:
            True  → 加 X-Fc-Invocation-Type: Async header，期望 202 Accepted，返回 request_id
            False → 同步调用，期望 200 OK，返回空字符串

    Returns:
        async_invocation=True  → request_id（成功）或 None（失败）
        async_invocation=False → 空字符串（成功）或 None（失败）
    """
    if not url or not token:
        logger.error("fc url or token not configured")
        return None

    effective_timeout = timeout if timeout and timeout > 0 else 20

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if async_invocation:
        headers["X-Fc-Invocation-Type"] = "Async"

    async with AsyncClient(timeout=effective_timeout) as client:
        res = await client.post(url, json=event, headers=headers)

    if async_invocation:
        if res.status_code != status.HTTP_202_ACCEPTED:
            logger.error("fc async invoke failed: status=%s content=%s", res.status_code, res.text)
            return None
        request_id = res.headers.get("X-FC-Request-ID", "")
        logger.info("fc async invoked, request_id=%s", request_id)
        return request_id
    else:
        if res.status_code != status.HTTP_200_OK:
            logger.error("fc sync invoke failed: status=%s content=%s", res.status_code, res.text)
            return None
        return ""
