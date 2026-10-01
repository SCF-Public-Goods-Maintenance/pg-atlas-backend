"""add repo pushed_at_observed_at

Revision ID: c46f7c7497a6
Revises: 71172408d9bd
Create Date: 2026-09-25 11:46:41.948217+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c46f7c7497a6"
down_revision: Union[str, Sequence[str], None] = "71172408d9bd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("repos", sa.Column("pushed_at_observed_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("repos", "pushed_at_observed_at")
