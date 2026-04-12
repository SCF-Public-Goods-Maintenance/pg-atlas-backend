"""add gitlog artifacts audit table

Revision ID: 9f2a1c4d5e6f
Revises: ee8115d35fef
Create Date: 2026-04-11 12:00:00+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "9f2a1c4d5e6f"
down_revision: Union[str, Sequence[str], None] = "ee8115d35fef"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create gitlog processing audit table."""

    submission_status_enum = postgresql.ENUM(
        "pending",
        "processed",
        "failed",
        name="submission_status",
        create_type=False,
    )

    op.create_table(
        "gitlog_artifacts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("artifact_path", sa.String(length=1024), nullable=True),
        sa.Column("gitlog_content_hash", sa.LargeBinary(length=32), nullable=True),
        sa.Column("status", submission_status_enum, nullable=False),  # pyright: ignore[reportUnknownArgumentType]
        sa.Column("error_detail", sa.String(length=4096), nullable=True),
        sa.Column("since_months", sa.Integer(), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["repo_id"], ["repos.id"], name=op.f("fk_gitlog_artifacts_repo_id_repos")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_gitlog_artifacts")),
    )
    op.create_index("ix_gitlog_artifacts_repo_id", "gitlog_artifacts", ["repo_id"], unique=False)
    op.create_index("ix_gitlog_artifacts_submitted_at", "gitlog_artifacts", ["submitted_at"], unique=False)


def downgrade() -> None:
    """Drop gitlog processing audit table."""

    op.drop_index("ix_gitlog_artifacts_submitted_at", table_name="gitlog_artifacts")
    op.drop_index("ix_gitlog_artifacts_repo_id", table_name="gitlog_artifacts")
    op.drop_table("gitlog_artifacts")
