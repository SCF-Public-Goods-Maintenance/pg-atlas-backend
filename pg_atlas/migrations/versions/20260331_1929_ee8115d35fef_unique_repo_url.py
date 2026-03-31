"""add unique constraint to repos.repo_url

Revision ID: ee8115d35fef
Revises: b2c3d4e5f6a7
Create Date: 2026-03-31 19:29:41.051189+00:00

We add the unique constraint on repos.repo_url to prevent any regressions with
git repositories that are stored as `pkg:github/{owner}/{repo}` and as (multiple)
`pkg:{system}/{package}` canonical IDs.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ee8115d35fef"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add UNIQUE constraint to repos.repo_url."""
    op.create_unique_constraint("uq_repos_repo_url", "repos", ["repo_url"])


def downgrade() -> None:
    """Drop UNIQUE constraint on repos.repo_url."""
    op.drop_constraint("uq_repos_repo_url", "repos", type_="unique")
