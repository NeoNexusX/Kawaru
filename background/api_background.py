import asyncio
from typing import Dict, Any, Optional
import httpx
from httpx import HTTPStatusError, RequestError
from starlette import status

from background.celery_app import celery_app
from tools.logger import get_logger

logger = get_logger("background.api_background")


@celery_app.task(bind=True, autoretry_for=(HTTPStatusError, RequestError), retry_kwargs={'max_retries': 3, 'countdown': 5}) # type: ignore
def generic_http_request_task(self, url: str, method: str = 'GET', headers: Optional[Dict[str, str]] = None,
                              data: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Dict[str, Any]:
    """
    通用异步 HTTP 请求任务，支持重试。
    返回 {'success': bool, 'status_code': int, 'data': Any 或 'error': str}
    """
    async def _request():
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
                logger.error(f"HTTP request failed for {url}: {e}")
                raise  # 触发重试
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(f"Unexpected error in HTTP request for {url}: {e}")
                return {'success': False, 'error': str(e)}

    # 由于 CeleryAsyncTask 会运行协程，这里直接返回协程结果
    return asyncio.run(_request())


@celery_app.task(bind=True) # type: ignore
def validate_orcid_task(orcid: str) -> bool:
    """
    校验 ORCID 的 Celery 任务。
    调用通用 HTTP 请求任务。
    """
    url = f"https://pub.orcid.org/v3.0/{orcid}/person"
    headers = {'Accept': 'application/json'}

    result = generic_http_request_task.delay(url, method='HEAD', headers=headers, timeout=100)

    # 等待结果
    task_result = result.get(timeout=120)  # 等待最多 2 分钟

    if task_result.get('success'):
        return True
    else:
        logger.error(f"ORCID validation failed for {orcid}: {task_result.get('error')}")
        return False
