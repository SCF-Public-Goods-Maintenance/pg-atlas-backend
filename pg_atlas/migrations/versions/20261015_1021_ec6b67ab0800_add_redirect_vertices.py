"""add redirect vertices

Revision ID: ec6b67ab0800
Revises: a0f47bdaf820
Create Date: 2026-10-15 10:21:00.999078+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ec6b67ab0800"
down_revision: Union[str, Sequence[str], None] = "a0f47bdaf820"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add the redirect discriminator and JTI subtype table."""
    op.execute("ALTER TYPE vertex_type ADD VALUE 'redirect'")

    op.create_table(
        "redirect_vertices",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("target_vertex_id", sa.Integer(), nullable=False),
        sa.Column("predicate", sa.Enum("fork-of", "package-of", name="redirect_predicate"), nullable=False),
        sa.ForeignKeyConstraint(
            ["id"], ["repo_vertices.id"], name=op.f("fk_redirect_vertices_id_repo_vertices"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["target_vertex_id"],
            ["repo_vertices.id"],
            name=op.f("fk_redirect_vertices_target_vertex_id_repo_vertices"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_redirect_vertices")),
    )
    op.create_index(op.f("ix_redirect_vertices_target_vertex_id"), "redirect_vertices", ["target_vertex_id"], unique=False)


def downgrade() -> None:
    """Remove the redirect subtype and its discriminator value."""
    connection = op.get_bind()
    redirect_count = connection.execute(
        sa.text("SELECT count(*) FROM repo_vertices WHERE vertex_type = 'redirect'")
    ).scalar_one()
    if redirect_count:
        raise RuntimeError("Cannot downgrade while RedirectVertex rows exist; migrate or delete them first")

    op.drop_index(op.f("ix_redirect_vertices_target_vertex_id"), table_name="redirect_vertices")
    op.drop_table("redirect_vertices")
    op.execute("DROP TYPE redirect_predicate")

    op.execute("ALTER TYPE vertex_type RENAME TO vertex_type_old")
    op.execute("CREATE TYPE vertex_type AS ENUM ('repo', 'external-repo')")
    op.execute("ALTER TABLE repo_vertices ALTER COLUMN vertex_type TYPE vertex_type USING vertex_type::text::vertex_type")
    op.execute("DROP TYPE vertex_type_old")
