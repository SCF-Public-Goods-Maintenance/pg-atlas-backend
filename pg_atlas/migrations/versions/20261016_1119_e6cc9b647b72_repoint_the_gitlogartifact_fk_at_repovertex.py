"""repoint the GitLogArtifact FK at RepoVertex (was: Repo)

Revision ID: e6cc9b647b72
Revises: ec6b67ab0800
Create Date: 2026-10-16 11:19:58.749290+00:00

"""

import logging
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e6cc9b647b72"
down_revision: Union[str, Sequence[str], None] = "ec6b67ab0800"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    """Move gitlog artifact references from Repo rows to RepoVertex identities."""
    op.add_column("gitlog_artifacts", sa.Column("repo_vertex_id", sa.Integer(), nullable=True))

    copied_rows = op.get_bind().execute(sa.text("UPDATE gitlog_artifacts SET repo_vertex_id = repo_id")).rowcount
    logger.info(f"Copied repo_id to repo_vertex_id for {copied_rows} gitlog_artifacts rows")

    op.alter_column("gitlog_artifacts", "repo_vertex_id", nullable=False)
    op.create_foreign_key(
        op.f("fk_gitlog_artifacts_repo_vertex_id_repo_vertices"),
        "gitlog_artifacts",
        "repo_vertices",
        ["repo_vertex_id"],
        ["id"],
    )

    op.drop_index(op.f("ix_gitlog_artifacts_repo_id"), table_name="gitlog_artifacts")
    op.drop_index(op.f("ix_gitlog_artifacts_repo_seed_ordinal"), table_name="gitlog_artifacts")
    op.create_index(
        "ix_gitlog_artifacts_repo_seed_ordinal", "gitlog_artifacts", ["repo_vertex_id", "seed_run_ordinal"], unique=False
    )
    op.create_index("ix_gitlog_artifacts_repo_vertex_id", "gitlog_artifacts", ["repo_vertex_id"], unique=False)

    op.drop_constraint(op.f("fk_gitlog_artifacts_repo_id_repos"), "gitlog_artifacts", type_="foreignkey")
    op.drop_column("gitlog_artifacts", "repo_id")


def downgrade() -> None:
    """Restore Repo-only references if every artifact still refers to a Repo."""
    connection = op.get_bind()
    incompatible_count = connection.execute(
        sa.text("""
            SELECT count(*)
            FROM gitlog_artifacts AS artifact
            WHERE NOT EXISTS (
                SELECT 1
                FROM repos
                WHERE repos.id = artifact.repo_vertex_id
            )
            """)
    ).scalar_one()
    if incompatible_count:
        incompatible_ids = (
            connection.execute(
                sa.text("""
                SELECT DISTINCT artifact.repo_vertex_id
                FROM gitlog_artifacts AS artifact
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM repos
                    WHERE repos.id = artifact.repo_vertex_id
                )
                ORDER BY artifact.repo_vertex_id
                LIMIT 10
                """)
            )
            .scalars()
            .all()
        )
        logger.error(
            f"Cannot downgrade gitlog_artifacts: {incompatible_count} rows reference non-Repo vertices; "
            f"sample repo_vertex_id values: {incompatible_ids}"
        )
        raise RuntimeError(f"Cannot downgrade gitlog_artifacts: {incompatible_count} rows reference non-Repo vertices")

    op.add_column("gitlog_artifacts", sa.Column("repo_id", sa.Integer(), nullable=True))
    copied_rows = connection.execute(sa.text("UPDATE gitlog_artifacts SET repo_id = repo_vertex_id")).rowcount
    logger.info(f"Copied repo_vertex_id to repo_id for {copied_rows} gitlog_artifacts rows")
    op.alter_column("gitlog_artifacts", "repo_id", nullable=False)

    op.create_foreign_key(
        op.f("fk_gitlog_artifacts_repo_id_repos"),
        "gitlog_artifacts",
        "repos",
        ["repo_id"],
        ["id"],
    )

    op.drop_index("ix_gitlog_artifacts_repo_vertex_id", table_name="gitlog_artifacts")
    op.drop_index("ix_gitlog_artifacts_repo_seed_ordinal", table_name="gitlog_artifacts")
    op.create_index(
        op.f("ix_gitlog_artifacts_repo_seed_ordinal"), "gitlog_artifacts", ["repo_id", "seed_run_ordinal"], unique=False
    )
    op.create_index(op.f("ix_gitlog_artifacts_repo_id"), "gitlog_artifacts", ["repo_id"], unique=False)

    op.drop_constraint(
        op.f("fk_gitlog_artifacts_repo_vertex_id_repo_vertices"),
        "gitlog_artifacts",
        type_="foreignkey",
    )
    op.drop_column("gitlog_artifacts", "repo_vertex_id")
