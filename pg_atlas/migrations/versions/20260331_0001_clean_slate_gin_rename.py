"""clean slate: delete non-github repos, GIN index, rename git_org_url

Revision ID: a1b2c3d4e5f6
Revises: 72d340393f83
Create Date: 2026-03-31 00:01:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "72d340393f83"
branch_labels: Union[str, Sequence[str], None] = ("atlas",)
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Clean slate + GIN index + column rename.

    1. Delete Repo rows whose canonical_id does NOT start with 'pkg:github/'.
       These were incorrectly created by the old per-package upsert_repo loop
       (issue #9). Edges are deleted first to satisfy FK constraints.
    2. Create a GIN index on repos.releases for JSONB containment queries
       used by find_repo_by_release_purl().
    3. Rename projects.git_org_url → git_owner_url.
    """
    # --- 1. Delete violating Repo rows ---
    # Use lightweight sa.table/sa.column references for data migration.
    # vertex_type is a PG enum, so we cast the literal to match.
    vertex_type_enum = sa.Enum("repo", "external-repo", name="vertex_type", create_type=False)

    repo_vertices = sa.table(
        "repo_vertices",
        sa.column("id", sa.Integer),
        sa.column("canonical_id", sa.String),
        sa.column("vertex_type", vertex_type_enum),
    )
    repos = sa.table("repos", sa.column("id", sa.Integer))
    depends_on = sa.table(
        "depends_on",
        sa.column("in_vertex_id", sa.Integer),
        sa.column("out_vertex_id", sa.Integer),
    )

    # Subquery: IDs of Repo vertices with non-github canonical_ids.
    bad_ids = (
        sa.select(repo_vertices.c.id)
        .where(
            repo_vertices.c.vertex_type == sa.cast(sa.literal("repo"), vertex_type_enum),
            ~repo_vertices.c.canonical_id.startswith("pkg:github/"),
        )
        .scalar_subquery()
    )

    # 1a. Delete depends_on edges referencing bad vertices (both directions).
    op.execute(
        depends_on.delete().where(
            sa.or_(
                depends_on.c.in_vertex_id.in_(bad_ids),
                depends_on.c.out_vertex_id.in_(bad_ids),
            )
        )
    )

    # 1b. Delete repos child rows.
    op.execute(repos.delete().where(repos.c.id.in_(bad_ids)))

    # 1c. Delete repo_vertices base rows.
    op.execute(
        repo_vertices.delete().where(
            repo_vertices.c.vertex_type == sa.cast(sa.literal("repo"), vertex_type_enum),
            ~repo_vertices.c.canonical_id.startswith("pkg:github/"),
        )
    )

    # --- 2. GIN index on repos.releases ---
    op.create_index(
        "ix_repos_releases_gin",
        "repos",
        ["releases"],
        postgresql_using="gin",
        postgresql_ops={"releases": "jsonb_path_ops"},
    )

    # --- 3. Rename column ---
    op.alter_column("projects", "git_org_url", new_column_name="git_owner_url")


def downgrade() -> None:
    """
    Reverse GIN index and column rename.

    Row deletions (step 1) are non-reversible — a re-bootstrap repopulates
    the correct data. This is intentional: downgrade restores schema, not data.
    """

    op.alter_column("projects", "git_owner_url", new_column_name="git_org_url")

    op.drop_index("ix_repos_releases_gin", table_name="repos")
