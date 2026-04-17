from celery_runtime.runtime import build_celery_runtime
from celery_runtime.state import set_celery_app


celery_app = build_celery_runtime()
