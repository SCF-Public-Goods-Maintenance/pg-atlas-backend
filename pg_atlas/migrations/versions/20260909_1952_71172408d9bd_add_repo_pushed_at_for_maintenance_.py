"""add repo pushed_at for maintenance signals

Revision ID: 71172408d9bd
Revises: a0f47bdaf820
Create Date: 2026-09-09 19:52:07.737250+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "71172408d9bd"
down_revision: Union[str, Sequence[str], None] = "a0f47bdaf820"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("repos", sa.Column("pushed_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("repos", "pushed_at")
