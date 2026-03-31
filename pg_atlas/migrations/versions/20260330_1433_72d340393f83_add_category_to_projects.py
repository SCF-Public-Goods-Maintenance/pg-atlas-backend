"""add category to projects

Revision ID: 72d340393f83
Revises: f3d946ade07e
Create Date: 2026-03-30 14:33:03.262583+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "72d340393f83"
down_revision: Union[str, Sequence[str], None] = "f3d946ade07e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.add_column("projects", sa.Column("category", sa.String(length=128), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_column("projects", "category")
