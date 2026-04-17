"""quantize adoption score storage

Revision ID: 5a3f2e7c1d9a
Revises: b31d9f6c8a21
Create Date: 2026-04-16 23:00:00+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5a3f2e7c1d9a"
down_revision: Union[str, Sequence[str], None] = "b31d9f6c8a21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Store project adoption_score as quantized NUMERIC(5,2)."""

    op.execute(
        """
        ALTER TABLE projects
        ALTER COLUMN adoption_score
        TYPE NUMERIC(5, 2)
        USING ROUND(adoption_score::numeric, 2)
        """
    )


def downgrade() -> None:
    """Restore project adoption_score to double precision."""

    op.execute(
        """
        ALTER TABLE projects
        ALTER COLUMN adoption_score
        TYPE DOUBLE PRECISION
        USING adoption_score::double precision
        """
    )
