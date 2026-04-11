"""
RepoVertex hierarchy: RepoVertex (JTI base), Repo, and ExternalRepo ORM models.

Uses SQLAlchemy 2.x Joined Table Inheritance (JTI) so that both ``Repo`` and
``ExternalRepo`` share a common ``repo_vertices`` table for their identity columns
(``id``, ``canonical_id``, ``vertex_type``).  Edge tables (``depends_on``) carry FK
constraints pointing to ``repo_vertices.id``, which gives full referential integrity
while allowing a single FK to target either subtype — analogous to a property-graph
vertex registry.

Graph analytics (NetworkX) resolve ``canonical_id`` strings via a JOIN against
``repo_vertices`` rather than storing duplicate string columns on edge rows.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import PgBase, RepoVertexType, Visibility, canonical_id, enum_values, intpk

if TYPE_CHECKING:
    from pg_atlas.db_models.contributed_to import ContributedTo
    from pg_atlas.db_models.depends_on import DependsOn
    from pg_atlas.db_models.project import Project


# ---------------------------------------------------------------------------
# JTI base: RepoVertex
# ---------------------------------------------------------------------------


class RepoVertex(PgBase):
    """
    Joined-table-inheritance base for all repository-like vertices.

    Stores the identity columns shared by ``Repo`` and ``ExternalRepo``. All edge
    tables carry FK constraints to ``repo_vertices.id`` so that a single FK column
    can reference either subtype without sacrificing referential integrity.

    Do not instantiate ``RepoVertex`` directly; use the concrete subclasses.
    """

    __tablename__ = "repo_vertices"

    __mapper_args__ = {
        "polymorphic_on": "vertex_type",
        "polymorphic_identity": None,
    }

    # --- identity (shared across all subtypes) ---
    id: Mapped[intpk] = mapped_column(init=False)
    canonical_id: Mapped[canonical_id]

    #: Polymorphic discriminator — set automatically by SQLAlchemy on INSERT.
    vertex_type: Mapped[str] = mapped_column(
        Enum(
            RepoVertexType,
            name="vertex_type",
            values_callable=enum_values,
        ),
        init=False,
    )


# ---------------------------------------------------------------------------
# Concrete subtype: Repo
# ---------------------------------------------------------------------------


class Repo(RepoVertex):
    """
    A single git repository or published package within the Stellar/Soroban ecosystem.

    Created/updated by SBOM ingestion and registry crawls. The ``project_id`` FK
    enforces the 1-to-many Project → Repo relationship at the schema level.

    Metric columns (``pony_factor``, ``criticality_score``, adoption signals) are
    materialized by the background computation pipeline.
    """

    __tablename__ = "repos"

    __mapper_args__ = {
        "polymorphic_identity": RepoVertexType.repo,
    }

    #: FK to ``repo_vertices.id`` — required by SQLAlchemy JTI pattern.
    id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("repo_vertices.id"),
        primary_key=True,
        init=False,
        repr=False,
    )

    # --- descriptive ---
    display_name: Mapped[str] = mapped_column(String(512))
    visibility: Mapped[Visibility] = mapped_column(Enum(Visibility, name="visibility", values_callable=enum_values))

    # --- versioning (required; comes before optional FK so dataclass ordering is valid) ---
    latest_version: Mapped[str] = mapped_column(String(256))

    # --- project membership (optional: we may ingest SBOMs before the project exists) ---
    project_id: Mapped[int | None] = mapped_column(ForeignKey("projects.id"), default=None)
    latest_commit_date: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    repo_url: Mapped[str | None] = mapped_column(String(512), default=None, unique=True)

    # --- materialised metrics ---
    pony_factor: Mapped[int | None] = mapped_column(default=None)
    criticality_score: Mapped[int | None] = mapped_column(default=None)
    adoption_downloads: Mapped[int | None] = mapped_column(default=None)
    adoption_stars: Mapped[int | None] = mapped_column(default=None)
    adoption_forks: Mapped[int | None] = mapped_column(default=None)

    # --- flexible data ---
    releases: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, default=None)
    # NB: 'metadata' is reserved by SQLAlchemy DeclarativeBase; the Python attribute
    # is named 'repo_metadata' while the DB column is named 'metadata'.
    repo_metadata: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, default=None)

    # --- audit ---
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        init=False,
    )

    # --- relationships ---
    project: Mapped[Project | None] = relationship(
        back_populates="repos",
        lazy="selectin",
        init=False,
        repr=False,
    )

    #: Edges where this repo is the *source* (i.e. this repo depends on something).
    outgoing_deps: Mapped[list[DependsOn]] = relationship(
        foreign_keys="DependsOn.in_vertex_id",
        primaryjoin="Repo.id == DependsOn.in_vertex_id",
        lazy="selectin",
        overlaps="in_node",
        init=False,
        repr=False,
    )

    #: Contributor activity edges pointing to this repo.
    contributor_edges: Mapped[list[ContributedTo]] = relationship(
        back_populates="repo",
        lazy="selectin",
        init=False,
        repr=False,
    )


idx_releases = Index(
    "ix_repos_releases_gin",
    Repo.releases,
    postgresql_using="gin",
    postgresql_ops={"releases": "jsonb_path_ops"},
)

# ---------------------------------------------------------------------------
# Concrete subtype: ExternalRepo
# ---------------------------------------------------------------------------


class ExternalRepo(RepoVertex):
    """
    An upstream dependency outside the Stellar/Soroban ecosystem.

    Tracked for blast-radius analysis only; no project-level data is maintained.
    Created by SBOM ingestion (when a dependency cannot be mapped to a known ``Repo``)
    and registry crawls.
    """

    __tablename__ = "external_repos"

    __mapper_args__ = {
        "polymorphic_identity": RepoVertexType.external_repo,
    }

    #: FK to ``repo_vertices.id`` — required by SQLAlchemy JTI pattern.
    id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("repo_vertices.id"),
        primary_key=True,
        init=False,
        repr=False,
    )

    # --- descriptive ---
    display_name: Mapped[str] = mapped_column(String(512))

    # --- versioning ---
    latest_version: Mapped[str] = mapped_column(String(256))
    repo_url: Mapped[str | None] = mapped_column(String(512), default=None)

    # --- materialised metrics ---
    criticality_score: Mapped[int | None] = mapped_column(default=None)

    # --- flexible data ---
    releases: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, default=None)

    # --- audit ---
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        init=False,
    )
