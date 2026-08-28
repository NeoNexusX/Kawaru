"""Central registry for every SQLModel table used by Alembic."""

from importlib import import_module

MODEL_MODULES = (
    "models.link_models",
    "models.user_models",
    "models.file_models",
    "models.experiment_models",
    "models.processing_models",
)


def import_all_models() -> None:
    """Import all table models so they are present in SQLModel.metadata."""
    for module_name in MODEL_MODULES:
        import_module(module_name)
