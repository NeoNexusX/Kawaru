from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

from database.migration_config import get_alembic_config


def test_revision_graph_has_one_root_and_one_head() -> None:
    script = ScriptDirectory.from_config(get_alembic_config())

    assert len(script.get_bases()) == 1
    assert len(script.get_heads()) == 1


def test_baseline_ddl_matches_registered_metadata() -> None:
    """Fast dialect-neutral smoke test; CI also repeats this against MariaDB."""
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        config = get_alembic_config()
        config.attributes["connection"] = connection

        command.upgrade(config, "head")
        command.check(config)

        assert set(inspect(connection).get_table_names()) == {
            "alembic_version",
            "experiment_process_run",
            "fileindb",
            "origin_imzml",
            "userfilelink",
            "userindb",
        }

        command.downgrade(config, "base")
        assert set(inspect(connection).get_table_names()) == {"alembic_version"}
