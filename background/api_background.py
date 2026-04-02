import asyncio
from typing import Dict, Any, Optional
import httpx
from httpx import HTTPStatusError, RequestError
from starlette import status

from background.celery_app import celery_app
from tools.logger import get_logger

logger = get_logger("background.api_background")


@celery_app.task() # type: ignore
async def generic_http_request_task(url: str, method: str = 'GET', headers: Optional[Dict[str, str]] = None, # TODO 写成类 ，先全部写出来普通的task，然后再合并
                                    data: Optional[Dict[str, Any]] = None, timeout: int = 30,
                                    max_retries: int = 3) -> Dict[str, Any]: # pyright: ignore[reportReturnType]
    """
    通用异步 HTTP 请求任务。
    返回 {'success': bool, 'status_code': int, 'data': Any 或 'error': str}
    """
    for attempt in range(max_retries + 1):
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                if method.upper() == 'GET':
                    response = await client.get(url, headers=headers or {})
                elif method.upper() == 'POST':
                    response = await client.post(url, headers=headers or {}, json=data or {})
                elif method.upper() == 'HEAD':
                    response = await client.head(url, headers=headers or {})
                else:
                    return {'success': False, 'error': f'Unsupported method: {method}'}

                if response.status_code == status.HTTP_200_OK:
                    return {'success': True, 'status_code': response.status_code, 'data': response.text}
                else:
                    return {'success': False, 'status_code': response.status_code, 'error': f'HTTP {response.status_code}'}
            except (HTTPStatusError, RequestError) as e:
                logger.warning(f"HTTP request failed for {url} (attempt {attempt + 1}/{max_retries + 1}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                    continue
                else:
                    return {'success': False, 'error': str(e)}
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(f"Unexpected error in HTTP request for {url}: {e}")
                return {'success': False, 'error': str(e)}


@celery_app.task() # type: ignore
async def validate_orcid_task(orcid: str) -> bool:
    """
    校验 ORCID 的 Celery 任务。
    直接执行异步 HTTP 请求。
    """
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    headers = {'Accept': 'application/json'}

    result = await generic_http_request_task(url, method='HEAD', headers=headers, timeout=30, max_retries=3)

    if result.get('success'):
        return True
    else:
        logger.error(f"ORCID validation failed for {orcid}: {result.get('error')}")
        return False
