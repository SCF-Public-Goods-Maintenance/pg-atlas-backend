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

    Lock ordering invariant: any transaction that updates more than one
    ``Contributor`` row (e.g. ``persist_repo_result``) MUST process those rows
    sorted ascending by ``email_hash``. Two overlapping transactions that both
    follow this order can only ever contend for the same row in the same
    sequence, never in reverse — which is what prevents an AB-BA deadlock
    between them. Sorting by a different key (including the surrogate ``id``)
    breaks this guarantee for any writer that shares contributors with one
    that sorts by ``email_hash``.
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
