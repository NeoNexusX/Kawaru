"""Application startup guard for Alembic-managed schemas."""

from database.migration import verify_database_revision


async def require_current_schema() -> None:
    """Prevent application startup against an unversioned or stale schema."""
    await verify_database_revision()
