#########################
# Relay Service - 中转下载（流式透传）
# 职责: 签发/校验中转 token、拼接中转 URL、把 OSS 对象经服务器流式透传给用户
# 说明: OSS→服务器 走内网 endpoint（免费），服务器→用户 走本机出站带宽（套餐内免费）
# 2026-10 抽出共享内核 stream_oss_object：文件中转与 SI 注释库 CSV 共用同一传输实现
#########################
from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path
from typing import Literal
from urllib.parse import quote

import jwt
from fastapi import HTTPException, Response
from fastapi.responses import StreamingResponse
from sqlmodel.ext.asyncio.session import AsyncSession
from starlette.background import BackgroundTask

from alibabacloud_oss_v2 import GetObjectRequest, HeadObjectRequest

from api_services.experiment_service import get_origin_imzml_by_file_id
from api_services.file_service import assert_origin_files_ready, get_file_record_by_public_id
from boot.config import get_config
from storage.oss.oss_client import create_internal_oss_client

from constants.file_constants import (
    OSS_BUCKET_NAME,
    OSS_DEFAULT_TIMEOUT_SECONDS,
    RELAY_ACQUIRE_TIMEOUT,
    RELAY_IDLE_TIMEOUT,
    RELAY_MAX_CONCURRENCY,
    RELAY_TOKEN_EXPIRE_SECONDS,
)
from exceptions.common_exceptions import internal_server_error_exception
from exceptions.file_exceptions import file_content_missing_exception
from models.auth import SECRET_KEY
from models.experiment_models import OriginImzMLInDB
from models.file_models import FileInDB, OssDownloadFile
from tools.logger import get_logger

logger = get_logger("api_services.relay")

# 并发信号量：限制同时进行的中转下载数，保护服务器带宽与 OSS 连接数。
# 注意：uvicorn 多 worker 时每个进程各持有独立信号量，实际并发 = worker 数 × RELAY_MAX_CONCURRENCY。
# 文件中转与 SI 注释库 CSV 共用这一个信号量（不区分来源）。
RELAY_SEMAPHORE = asyncio.Semaphore(RELAY_MAX_CONCURRENCY)

RelayPart = Literal["imzml", "ibd"]

RELAY_TOKEN_INVALID_DETAIL = "下载链接无效或已过期"

_CTRL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

# 中转 token 一律对称签名；登录 token 的算法由配置控制，两者互不复用。
_RELAY_JWT_ALGORITHM = "HS256"


def _encode_relay_payload(payload: dict) -> str:
    """中转 token 统一签发入口（文件中转与 SI 注释库共用同一签名口径）。"""
    return jwt.encode(payload, SECRET_KEY, algorithm=_RELAY_JWT_ALGORITHM)


def _decode_relay_payload(token: str) -> dict | None:
    """中转 token 统一解码入口：签名或有效期不合法均返回 None。"""
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[_RELAY_JWT_ALGORITHM])
    except jwt.PyJWTError:
        return None


def issue_relay_token(public_id: str, part: RelayPart) -> str:
    """签发中转下载 token：绑定 public_id + part，短时有效。

    token 可复用（不做一次性消费）：下载器/浏览器断点续传会并发多条 Range 连接，
    共用同一个 URL 与 token。
    绑定 public_id 而非内部 file_id：JWT payload 对持有者可解码，
    内部自增 id 不得泄露进对外 URL。
    """
    payload = {
        "public_id": public_id,
        "part": part,
        "exp": int(time.time()) + RELAY_TOKEN_EXPIRE_SECONDS,
    }
    return _encode_relay_payload(payload)


def verify_relay_token(token: str, public_id: str, part: RelayPart) -> bool:
    """校验中转 token：签名有效且 public_id/part 与请求一致，防止被拿来取任意 OSS 对象。"""
    data = _decode_relay_payload(token)
    if data is None:
        return False
    return data.get("public_id") == public_id and data.get("part") == part


def _resolve_relay_origin(origin: OriginImzMLInDB, part: RelayPart) -> str:
    """按 part 解析 origin 的真实 OSS 路径（key 一律来自数据库，绝不信任请求入参）。"""
    return origin.imzml_path if part == "imzml" else origin.ibd_path


def _content_disposition(filename: str) -> str:
    """构造 Content-Disposition：ASCII 回退 + RFC 5987 UTF-8 编码（支持中文文件名），并过滤控制字符防头注入。"""
    name = _CTRL_CHARS.sub("", filename) or "download"
    ascii_fallback = (
        name.encode("ascii", "ignore").decode("ascii").replace("\\", "_").replace('"', "_")
        or "download"
    )
    return f'attachment; filename="{ascii_fallback}"; filename*=UTF-8\'\'{quote(name, safe="")}'


def resolve_relay_base_url(request_base_url: str) -> str:
    """中转 URL 公网前缀：server.public_base_url 配置优先，未配置则回退请求的 base_url 推导。

    API 层负责从 Request 取 base_url 传入，本函数只做配置决策，不耦合 fastapi.Request。
    """
    configured = get_config().server.public_base_url
    return (configured or request_base_url).rstrip("/")


async def _close_client(client) -> None:
    """安全关闭 OSS 客户端：关闭失败不能阻断调用方的信号量释放（并发槽有限）。"""
    try:
        await client.close()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("relay close oss client failed: %s", exc)


async def prepare_download_relay_oss(
    session: AsyncSession,
    file_record: FileInDB,
    base_url: str,
) -> list[OssDownloadFile]:
    """中转下载元数据：不签名，拼两个指向后端的中转 URL（内嵌短时 token）。

    与 prepare_download_raw_oss 对称，仅把 OSS 预签名 URL 换成后端中转 URL；
    origin 就绪校验复用 assert_origin_files_ready。不写 file_record.path，
    不产生中间产物，与被清理任务无关。
    """
    origin = await assert_origin_files_ready(session, file_record)

    public_id = file_record.public_id

    def relay_url(part: RelayPart) -> str:
        token = issue_relay_token(public_id, part)
        return f"{base_url.rstrip('/')}/files/{public_id}/relay/download?part={part}&token={token}"

    return [
        OssDownloadFile(
            filename=Path(origin.imzml_path).name,
            url=relay_url("imzml"),
            size=origin.imzml_size,
        ),
        OssDownloadFile(
            filename=Path(origin.ibd_path).name,
            url=relay_url("ibd"),
            size=origin.ibd_size,
        ),
    ]


async def stream_oss_object(
    *,
    oss_path: str,
    range_header: str | None,
    method: str,
    log_ctx: str,
) -> Response:
    """流式透传共享内核：把一个 OSS 对象的字节流交给客户端（文件中转与 SI 注释库共用）。

    调用方负责 token 校验与 OSS key 解析，本函数只做传输：
    - 下载文件名取 key 的 basename（Content-Disposition 用），调用方不再单独推导
    - 并发信号量覆盖传输全程：acquire 在构造响应前（拿不到返回 429）；
      交接前的失败（含 CancelledError 等 BaseException）由 finally 统一释放，
      交接后由 generator 的 finally / BackgroundTask 的 _cleanup 释放，不漏放
    - get_object 在构造响应前发起：响应状态码（200/206/416）依赖 res.status_code
    - 逐 chunk 空闲超时：客户端停止消费 → 背压使 OSS read 阻塞 → 超时中断传输
    - HEAD 走 head_object 分支不拉正文（uvicorn 对 HEAD 只是丢弃响应体，
      StreamingResponse 的生成器仍会被完整消费，等于每次 HEAD 拉一遍整文件）
    - log_ctx 标记来源（如 "file_id=1 part=ibd" / "si_db"），供日志区分与统计
    """
    download_name = Path(oss_path).name
    # 有界等待并发槽：在构造响应前获取，超时可干净返回 429
    try:
        await asyncio.wait_for(RELAY_SEMAPHORE.acquire(), timeout=RELAY_ACQUIRE_TIMEOUT)
    except asyncio.TimeoutError as exc:
        logger.warning("relay download busy %s", log_ctx)
        raise HTTPException(status_code=429, detail="当前下载繁忙，请稍后重试") from exc

    client = create_internal_oss_client()
    if client is None:
        RELAY_SEMAPHORE.release()
        logger.error("relay download unavailable: internal oss client not created %s", log_ctx)
        raise internal_server_error_exception

    # 响应交给调用方之前的所有失败路径（含 CancelledError 等 BaseException）由 finally 统一释放；
    # 一旦交出 StreamingResponse，则改由生成器 finally / BackgroundTask 的 _cleanup 负责。
    handed_off = False
    try:
        if method == "HEAD":
            head = await client.head_object(
                HeadObjectRequest(bucket=OSS_BUCKET_NAME, key=oss_path) # type: ignore
            )
            headers = {
                "Accept-Ranges": "bytes",
                "Content-Disposition": _content_disposition(download_name),
            }
            if head.content_type:
                headers["Content-Type"] = head.content_type
            if head.content_length:
                headers["Content-Length"] = str(head.content_length)
            return Response(status_code=head.status_code or 200, headers=headers)

        res = await client.get_object(
            GetObjectRequest(bucket=OSS_BUCKET_NAME, key=oss_path, range_header=range_header) # type: ignore
        )

        cleanup_done = False

        async def _cleanup() -> None:
            """幂等释放（OSS 流 + 客户端 + 并发槽）：并发槽有限，漏放会逐步废掉整个中转。

            每步单独兜异常，保证任何一步失败都不会阻断最后的信号量释放。
            """
            nonlocal cleanup_done
            if cleanup_done:
                return
            cleanup_done = True
            try:
                await res.body.close() # type: ignore
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("relay close oss body failed %s: %s", log_ctx, exc)
            await _close_client(client)
            RELAY_SEMAPHORE.release()

        async def gen():
            try:
                # iter_bytes 是异步工厂方法：必须 await 才能拿到异步迭代器（read/iter_bytes 形状不对称）
                iterator = await res.body.iter_bytes() # type: ignore
                while True:
                    try:
                        chunk = await asyncio.wait_for(anext(iterator), timeout=RELAY_IDLE_TIMEOUT)
                    except StopAsyncIteration:
                        # 流正常结束。StopAsyncIteration 绝不能逸出 async generator
                        # （PEP 479：逸出会被替换为 RuntimeError），必须 return 正常收尾
                        return
                    yield chunk
            except asyncio.TimeoutError:
                logger.warning("relay download idle timeout, aborting %s", log_ctx)
                raise
            finally:
                # 正常结束、空闲超时、客户端断开（CancelledError）都走这里
                await _cleanup()

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Disposition": _content_disposition(download_name),
        }
        if res.content_type:
            headers["Content-Type"] = res.content_type
        if res.content_length:
            headers["Content-Length"] = str(res.content_length)
        if res.content_range:
            headers["Content-Range"] = res.content_range

        response = StreamingResponse(
            gen(),
            status_code=res.status_code or 200,
            headers=headers,
            # 兜底：极端情况下生成器未被启动（客户端在建流前就断开）时也能释放资源
            background=BackgroundTask(_cleanup),
        )
        handed_off = True
        return response
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("relay %s failed %s: %s", method, log_ctx, exc)
        raise internal_server_error_exception from exc
    finally:
        if not handed_off:
            # 先还槽位再关客户端：取消可能在 await _close_client 期间再次抛出，
            # 槽位必须无条件归还（_close_client 自身已吞异常）
            RELAY_SEMAPHORE.release()
            await _close_client(client)


async def stream_relay_file(
    session: AsyncSession,
    public_id: str,
    part: RelayPart,
    token: str,
    range_header: str | None,
    method: str = "GET",
) -> Response:
    """文件中转下载：验 token → 解析真实 oss key → 交给共享内核 stream_oss_object。

    - 对外入参为 public_id：验 token 后在库内换回内部 file_id 定位 origin
    - 文件存在性（含删除流程过滤）；origin 路径从数据库解析，绝不信任请求入参
    - 传输细节（信号量/流式/空闲超时/清理）见 stream_oss_object
    """
    if not verify_relay_token(token, public_id, part):
        logger.warning("relay download rejected: invalid or expired token public_id=%s part=%s", public_id, part)
        raise HTTPException(status_code=401, detail=RELAY_TOKEN_INVALID_DETAIL)

    file_record = await get_file_record_by_public_id(public_id, session)
    file_id = file_record.file_id
    origin = await get_origin_imzml_by_file_id(session, file_id) # type: ignore
    if not origin:
        logger.warning("relay download origin missing public_id=%s file_id=%s", public_id, file_id)
        raise file_content_missing_exception

    oss_path = _resolve_relay_origin(origin, part)
    if not oss_path:
        logger.warning("relay download path empty public_id=%s file_id=%s part=%s", public_id, file_id, part)
        raise file_content_missing_exception

    return await stream_oss_object(
        oss_path=oss_path,
        range_header=range_header,
        method=method,
        log_ctx=f"file_id={file_id} part={part}",
    )


#########################
# SI 注释库 CSV 中转（key 来自配置，无 DB 记录）
#########################
SI_DB_TOKEN_SCOPE = "si_db"
SI_DB_NOT_CONFIGURED_DETAIL = "注释库文件未配置，请联系管理员"


def issue_si_db_relay_token() -> str:
    """签发 SI 注释库 CSV 中转 token：只绑 scope + exp，短时有效。

    - 不绑路径：OSS key 唯一来源是 config.yaml 的 oss_si_db_key，token 与请求都无法影响它
    - scope 与文件中转 token（{public_id, part, exp}）的 payload 结构不同，互相不可冒充
    - 可复用（不做一次性消费）：断点续传的并发 Range 连接共用同一个 URL 与 token
    """
    payload = {
        "scope": SI_DB_TOKEN_SCOPE,
        "exp": int(time.time()) + RELAY_TOKEN_EXPIRE_SECONDS,
    }
    return _encode_relay_payload(payload)


def verify_si_db_relay_token(token: str) -> bool:
    """校验 SI 注释库中转 token：签名有效且 scope 匹配。"""
    data = _decode_relay_payload(token)
    return data is not None and data.get("scope") == SI_DB_TOKEN_SCOPE


def get_si_db_oss_key() -> str | None:
    """CSV 的 OSS key：唯一来源是配置 oss_si_db_key（空白视为未配置，返回 None）。"""
    return (get_config().oss.oss_si_db_key or "").strip() or None


async def head_si_db_size(oss_key: str) -> int | None:
    """对内网 endpoint 做一次 head_object 拿 CSV 真实大小（兼验证对象存在）。

    供 /download 元数据接口填 OssDownloadFile.size（必填 int）；不占中转信号量
    （一次元数据请求体积极小）。key 由调用方传入（调用方已做配置闸），本函数
    只负责探测；客户端不可用 / 超时 / 对象不存在均返回 None，报错口径由调用方决定。
    超时用 OSS_DEFAULT_TIMEOUT_SECONDS 兜底：内网 endpoint 卡住时不会挂到 SDK
    读超时（60s × 3 次重试）。
    """
    client = create_internal_oss_client()
    if client is None:
        return None
    try:
        head = await asyncio.wait_for(
            client.head_object(HeadObjectRequest(bucket=OSS_BUCKET_NAME, key=oss_key)), # type: ignore
            timeout=OSS_DEFAULT_TIMEOUT_SECONDS,
        )
        return head.content_length
    except asyncio.TimeoutError:
        logger.error("si_db head timed out after %ss key=%s", OSS_DEFAULT_TIMEOUT_SECONDS, oss_key)
        return None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("si_db head failed key=%s: %s", oss_key, exc)
        return None
    finally:
        await _close_client(client)


async def stream_si_db_csv(token: str, range_header: str | None, method: str = "GET") -> Response:
    """SI 注释库 CSV 中转透传：验 token → 读配置 key → 共享内核（无 DB 参与）。

    key 只来自配置：请求里没有任何字段能影响服务端去读哪个对象。
    """
    if not verify_si_db_relay_token(token):
        logger.warning("si_db relay download rejected: invalid or expired token")
        raise HTTPException(status_code=401, detail=RELAY_TOKEN_INVALID_DETAIL)

    oss_key = get_si_db_oss_key()
    if not oss_key:
        logger.warning("si_db relay download unavailable: oss_si_db_key not configured")
        raise HTTPException(status_code=503, detail=SI_DB_NOT_CONFIGURED_DETAIL)

    return await stream_oss_object(
        oss_path=oss_key,
        range_header=range_header,
        method=method,
        log_ctx="si_db",
    )
