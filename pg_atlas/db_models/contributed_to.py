"""
ContributedTo edge ORM model.

Directed edge: ``Contributor`` → ``Repo`` with git activity statistics as edge properties.
Unlike ``depends_on``, both endpoints are always concrete (``Contributor`` and ``Repo``),
so FK constraints reference their respective tables directly.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import PgBase

if TYPE_CHECKING:
    from pg_atlas.db_models.contributor import Contributor
    from pg_atlas.db_models.repo_vertex import Repo


class ContributedTo(PgBase):
    """
    Records that a ``Contributor`` has committed to a specific ``Repo``.

    The composite primary key ``(contributor_id, repo_id)`` means there is at most
    one edge between a contributor and a repo; the edge properties are updated in-place
    on each git log refresh (upsert semantics).
    """

    __tablename__ = "contributed_to"

    contributor_id: Mapped[int] = mapped_column(ForeignKey("contributors.id"), primary_key=True)
    repo_id: Mapped[int] = mapped_column(ForeignKey("repos.id"), primary_key=True)

    # --- edge properties ---
    number_of_commits: Mapped[int]
    first_commit_date: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))
    last_commit_date: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True))

    # --- relationships ---
    contributor: Mapped[Contributor] = relationship(
        back_populates="contribution_edges",
        lazy="selectin",
        init=False,
        repr=False,
    )
    repo: Mapped[Repo] = relationship(
        back_populates="contributor_edges",
        lazy="selectin",
        init=False,
        repr=False,
    )
