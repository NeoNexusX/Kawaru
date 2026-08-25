"""baseline current schema

Revision ID: 35048e902958
Revises: 
Create Date: 2026-08-25 11:33:39.061926
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '35048e902958'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "fileindb",
        sa.Column("filename", sa.String(length=100), nullable=False),
        sa.Column("file_type", sa.String(length=64), nullable=False),
        sa.Column("experiment_type", sa.String(length=128), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("storage_type", sa.String(length=64), nullable=False),
        sa.Column("file_verify_code", sa.String(length=32), nullable=True),
        sa.Column("organism", sa.String(length=128), nullable=False),
        sa.Column("organism_part", sa.String(length=128), nullable=False),
        sa.Column("condition", sa.String(length=128), nullable=False),
        sa.Column("sample_growth_conditions", sa.String(length=128), nullable=False),
        sa.Column("sample_stabilization", sa.String(length=128), nullable=False),
        sa.Column("tissue_modification", sa.String(length=128), nullable=False),
        sa.Column("maldi_matrix", sa.String(length=128), nullable=False),
        sa.Column("maldi_matrix_application", sa.String(length=128), nullable=False),
        sa.Column("solvent", sa.String(length=128), nullable=False),
        sa.Column("spectrum_mode", sa.String(length=32), nullable=False),
        sa.Column("storage_mode", sa.String(length=32), nullable=False),
        sa.Column("polarity", sa.String(length=128), nullable=False),
        sa.Column("ionisation_source", sa.String(length=128), nullable=False),
        sa.Column("analyzer", sa.String(length=128), nullable=False),
        sa.Column("pixel_size_horizontal", sa.Float(), nullable=False),
        sa.Column("pixel_size_vertical", sa.Float(), nullable=False),
        sa.Column("resolving_power", sa.Float(), nullable=False),
        sa.Column("mz", sa.Float(), nullable=False),
        sa.Column("file_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("is_public", sa.Boolean(), nullable=False),
        sa.Column("path", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("first_uploaded_by", sa.String(length=128), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("last_downloaded_at", sa.DateTime(), nullable=False),
        sa.Column("download_count", sa.Integer(), nullable=False),
        sa.Column("total_parts", sa.Integer(), nullable=True),
        sa.Column("image_path", sa.String(length=255), nullable=True),
        sa.Column("default_run_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("file_id"),
    )
    op.create_index("ix_fileindb_uploaded_at", "fileindb", ["uploaded_at"])
    op.create_index("ix_fileindb_last_downloaded_at", "fileindb", ["last_downloaded_at"])

    op.create_table(
        "userindb",
        sa.Column("username", sa.String(length=128), nullable=False),
        sa.Column("institution", sa.String(length=128), nullable=False),
        sa.Column("position", sa.String(length=128), nullable=False),
        sa.Column("research_field", sa.String(length=128), nullable=True),
        sa.Column("region", sa.String(length=128), nullable=True),
        sa.Column("orcid", sa.String(length=128), nullable=True),
        sa.Column("homepage", sa.String(length=128), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("identity", sa.String(length=10), nullable=False),
        sa.Column("email", sa.String(length=128), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("hashed_password", sa.String(length=256), nullable=False),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("total_file_size", sa.BigInteger(), nullable=False),
        sa.Column("download_used", sa.Integer(), nullable=False),
        sa.Column("processed_file_size", sa.BigInteger(), nullable=False),
        sa.Column("max_total_file_size", sa.BigInteger(), nullable=False),
        sa.Column("max_file_count", sa.Integer(), nullable=False),
        sa.Column("max_processing_size", sa.BigInteger(), nullable=False),
        sa.Column("max_download_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )

    op.create_table(
        "origin_imzml",
        sa.Column("experiment_name", sa.String(length=255), nullable=False),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_file_id", sa.Integer(), nullable=False),
        sa.Column("imzml_path", sa.String(length=255), nullable=False),
        sa.Column("imzml_size", sa.BigInteger(), nullable=False),
        sa.Column("ibd_path", sa.String(length=255), nullable=False),
        sa.Column("ibd_size", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(length=255), nullable=False),
        sa.Column("error_message", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["source_file_id"], ["fileindb.file_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_origin_imzml_source_file_id",
        "origin_imzml",
        ["source_file_id"],
        unique=True,
    )

    op.create_table(
        "userfilelink",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("file_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["file_id"], ["fileindb.file_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["userindb.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "file_id"),
    )

    op.create_table(
        "experiment_process_run",
        sa.Column("params_json", sa.String(length=4096), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error_message", sa.String(length=1024), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("clustering_status", sa.String(length=32), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("experiment_id", sa.Integer(), nullable=False),
        sa.Column("source_file_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("out_path", sa.String(length=255), nullable=True),
        sa.Column("output_size", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["experiment_id"], ["origin_imzml.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_file_id"], ["fileindb.file_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["userindb.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_experiment_process_run_experiment_id",
        "experiment_process_run",
        ["experiment_id"],
    )
    op.create_index(
        "ix_experiment_process_run_source_file_id",
        "experiment_process_run",
        ["source_file_id"],
    )
    op.create_index(
        "ix_experiment_process_run_user_id",
        "experiment_process_run",
        ["user_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_experiment_process_run_user_id", table_name="experiment_process_run")
    op.drop_index("ix_experiment_process_run_source_file_id", table_name="experiment_process_run")
    op.drop_index("ix_experiment_process_run_experiment_id", table_name="experiment_process_run")
    op.drop_table("experiment_process_run")
    op.drop_table("userfilelink")
    op.drop_index("ix_origin_imzml_source_file_id", table_name="origin_imzml")
    op.drop_table("origin_imzml")
    op.drop_table("userindb")
    op.drop_index("ix_fileindb_last_downloaded_at", table_name="fileindb")
    op.drop_index("ix_fileindb_uploaded_at", table_name="fileindb")
    op.drop_table("fileindb")
