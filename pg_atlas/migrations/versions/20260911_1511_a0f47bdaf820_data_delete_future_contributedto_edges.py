"""data: delete future ContributedTo edges

Revision ID: a0f47bdaf820
Revises: 5d5833f64218
Create Date: 2026-09-11 15:11:21.658164+00:00

"""

import datetime as dt
import logging
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a0f47bdaf820"
down_revision: Union[str, Sequence[str], None] = "5d5833f64218"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    """data migration: delete future ContributedTo edges"""
    tomorrow = dt.datetime.now(dt.UTC) + dt.timedelta(days=1)
    contributed_to = sa.table("contributed_to", sa.column("last_commit_date", sa.DateTime(timezone=True)))
    bind = op.get_bind()

    before_count = bind.execute(sa.select(sa.func.count()).select_from(contributed_to)).scalar() or 0
    delete_stmt = contributed_to.delete().where(contributed_to.c.last_commit_date > tomorrow)
    bind.execute(delete_stmt)
    after_count = bind.execute(sa.select(sa.func.count()).select_from(contributed_to)).scalar() or 0
    deleted_count = before_count - after_count

    if deleted_count:
        logger.info(f"ContributedTo before_count={before_count} after_count={after_count} deletions={deleted_count}")
    else:
        logger.info("no ContributedTo edges were deleted")


def downgrade() -> None:
    """irreversible deletion"""
    pass
