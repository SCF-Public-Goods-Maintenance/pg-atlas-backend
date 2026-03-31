"""metadata cleanup: delete E&C project repos, drop legacy scf_category

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-31 00:02:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Data quality cleanup for metadata.

    1. Delete any Repos linked to "Education & Community" projects.
       These projects were incorrectly associated — they don't produce code repos.
    2. Remove the legacy ``scf_category`` key from project metadata JSONB.
       Category is now a top-level column on the projects table.
    """
    projects = sa.table(
        "projects",
        sa.column("id", sa.Integer),
        sa.column("category", sa.String),
        sa.column("metadata", postgresql.JSONB),
    )
    repos = sa.table(
        "repos",
        sa.column("id", sa.Integer),
        sa.column("project_id", sa.Integer),
    )

    # 1. Delete the E&C project repos.
    ec_project_ids = sa.select(projects.c.id).where(projects.c.category == "Education & Community").scalar_subquery()
    op.execute(repos.delete().where(repos.c.project_id.in_(ec_project_ids)))

    # 2. Remove legacy scf_category from metadata.
    op.execute(
        projects.update()
        .where(projects.c.metadata.has_key("scf_category"))  # noqa: W601
        .values(metadata=projects.c.metadata.op("-")("scf_category"))
    )


def downgrade() -> None:
    """
    Non-reversible data cleanup.

    The E&C association nullification and scf_category removal cannot be
    automatically reversed — a re-bootstrap repopulates correct data.
    """

    pass
