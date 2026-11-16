"""
Enforce case-insensitive repository vertex identity.

Existing case-only duplicates must be reviewed before deployment. Index creation
fails transactionally if any exist; this migration never merges or deletes data.
"""

import sqlalchemy as sa
from alembic import op

revision = "f19c7e1a2001"
down_revision = "e6cc9b647b72"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Reject case-only duplicates and index case-insensitive lookups.
    """

    op.create_index("uq_repo_vertices_canonical_id_lower", "repo_vertices", [sa.text("lower(canonical_id)")], unique=True)


def downgrade() -> None:
    """
    Remove case-insensitive uniqueness without modifying stored identities.
    """

    op.drop_index("uq_repo_vertices_canonical_id_lower", table_name="repo_vertices")
