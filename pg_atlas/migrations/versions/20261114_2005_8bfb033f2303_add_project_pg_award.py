"""add project pg_award

Revision ID: 8bfb033f2303
Revises: c46f7c7497a6
Create Date: 2026-11-14 20:05:30.555691+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8bfb033f2303"
down_revision: Union[str, Sequence[str], None] = "c46f7c7497a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("projects", sa.Column("pg_award", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("projects", "pg_award")
