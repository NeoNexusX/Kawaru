from celery import Celery

from celery_runtime.runtime import build_celery_runtime, is_celery_runtime_enabled

if is_celery_runtime_enabled():
    celery_app = build_celery_runtime()
else:
    celery_app: Celery | None = None
