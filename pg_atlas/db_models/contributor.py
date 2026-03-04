"""
Contributor ORM model.

Contributors are derived from git commit history. Email addresses are hashed so
that PII is never stored in plain text, while still allowing cross-repo
contributor reconciliation (two commits by the same email = same contributor).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Index, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import PgBase, content_hash, intpk

if TYPE_CHECKING:
    from pg_atlas.db_models.contributed_to import ContributedTo


class Contributor(PgBase):
    """
    A unique contributor derived by hashing commit author emails.

    ``email_hash`` is a SHA-256 hex digest of the normalised email address (lowercased,
    stripped). It serves as the de-duplication key across repos without storing PII.
    The ``name`` field stores the most-recently-seen commit author name and may change
    across refreshes.
    """

    __tablename__ = "contributors"

    # --- identity ---
    id: Mapped[intpk] = mapped_column(init=False)

    #: SHA-256 hex digest of the lowercased, stripped commit email. Unique per contributor.
    email_hash: Mapped[content_hash]

    # --- display ---
    name: Mapped[str] = mapped_column(String(256))

    # --- relationships ---
    contribution_edges: Mapped[list[ContributedTo]] = relationship(
        back_populates="contributor",
        lazy="selectin",
        init=False,
        repr=False,
    )


# Index for fast email-hash deduplication lookups across repos.
idx_contributor_email_hash = Index("ix_contributors_email_hash", Contributor.email_hash)
