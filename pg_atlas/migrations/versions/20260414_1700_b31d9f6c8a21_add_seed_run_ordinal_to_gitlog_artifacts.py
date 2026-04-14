"""add seed run ordinal to gitlog artifacts

Revision ID: b31d9f6c8a21
Revises: 9f2a1c4d5e6f
Create Date: 2026-04-14 17:00:00+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b31d9f6c8a21"
down_revision: Union[str, Sequence[str], None] = "9f2a1c4d5e6f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add seed run ordinal used by dormancy scheduling."""

    op.add_column(
        "gitlog_artifacts",
        sa.Column("seed_run_ordinal", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )
    op.create_index(
        "ix_gitlog_artifacts_repo_seed_ordinal",
        "gitlog_artifacts",
        ["repo_id", "seed_run_ordinal"],
        unique=False,
    )


def downgrade() -> None:
    """Drop seed run ordinal support from gitlog artifacts."""

    op.drop_index("ix_gitlog_artifacts_repo_seed_ordinal", table_name="gitlog_artifacts")
    op.drop_column("gitlog_artifacts", "seed_run_ordinal")
