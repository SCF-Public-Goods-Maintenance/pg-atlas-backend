"""
Project ORM model.

A Project represents a funded project or recognised public good in the Stellar/Soroban
ecosystem. Sourced primarily from OpenGrants. One Project has many Repos (enforced
via a foreign key on the ``repos`` table rather than an association table, since the
relationship is strictly 1-to-many).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, Numeric, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import (
    ActivityStatus,
    PgBase,
    ProjectType,
    canonical_id,
    enum_values,
    intpk,
)

if TYPE_CHECKING:
    from pg_atlas.db_models.repo_vertex import Repo


class Project(PgBase):
    """
    A funded project or recognised public good in the Stellar/Soroban ecosystem.

    Vertex properties follow the PG Atlas property-graph data model. Metric columns
    (``pony_factor``, ``criticality_score``, ``adoption_score``) are materialized by
    the background computation pipeline and start as NULL.
    """

    __tablename__ = "projects"

    # --- identity ---
    id: Mapped[intpk] = mapped_column(init=False)
    canonical_id: Mapped[canonical_id]

    # --- descriptive ---
    display_name: Mapped[str] = mapped_column(String(512))
    project_type: Mapped[ProjectType] = mapped_column(Enum(ProjectType, name="project_type", values_callable=enum_values))
    activity_status: Mapped[ActivityStatus] = mapped_column(
        Enum(ActivityStatus, name="activity_status", values_callable=enum_values)
    )
    category: Mapped[str | None] = mapped_column(String(128), default=None)
    git_owner_url: Mapped[str | None] = mapped_column(String(512), default=None)

    # --- materialized metrics (computed by background pipeline, null until first run) ---
    pony_factor: Mapped[int | None] = mapped_column(default=None)
    criticality_score: Mapped[int | None] = mapped_column(default=None)
    adoption_score: Mapped[Decimal | None] = mapped_column(Numeric(5, 2, asdecimal=True), default=None)

    # --- flexible metadata (anything to display but not traverse/query) ---
    # NB: 'metadata' is reserved by SQLAlchemy DeclarativeBase; the Python attribute
    # is named 'project_metadata' while the DB column is named 'metadata'.
    project_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, default=None)

    # --- audit ---
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        init=False,
    )

    # --- relationships ---
    #: all repos that belong to this project
    repos: Mapped[list[Repo]] = relationship(
        back_populates="project",
        lazy="selectin",
        init=False,
        repr=False,
    )

    # TODO: write `async def has_published_packages()` for project_type check
