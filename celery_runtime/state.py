from celery import Celery


_celery_app: Celery | None = None


def set_celery_app(app: Celery | None) -> Celery | None:
    """Persist the current process Celery app for task module imports."""
    global _celery_app  # pylint: disable=global-statement
    _celery_app = app
    return _celery_app


def get_celery_app() -> Celery:
    """Return the initialized Celery app or fail fast with context."""
    if _celery_app is None:
        raise RuntimeError(
            "Celery app is not initialized. "
            "Run bootstrap() in the API process or use celery_runtime.entrypoint for Celery CLI."
        )
    return _celery_app
