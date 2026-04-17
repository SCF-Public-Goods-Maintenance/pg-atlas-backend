"""quantize adoption score as a 2-digit exponent Numeric

Revision ID: 66ac36af6383
Revises: b31d9f6c8a21
Create Date: 2026-04-17 09:08:42.147230+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "66ac36af6383"
down_revision: Union[str, Sequence[str], None] = "b31d9f6c8a21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Store project adoption_score as quantized NUMERIC(5,2)."""
    op.alter_column(
        "projects",
        "adoption_score",
        existing_type=sa.DOUBLE_PRECISION(precision=53),
        type_=sa.Numeric(precision=5, scale=2),
        existing_nullable=True,
    )


def downgrade() -> None:
    """Restore project adoption_score to double precision."""
    op.alter_column(
        "projects",
        "adoption_score",
        existing_type=sa.Numeric(precision=5, scale=2),
        type_=sa.DOUBLE_PRECISION(precision=53),
        existing_nullable=True,
    )
