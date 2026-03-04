"""
DependsOn edge ORM model.

Directed edge: source ``Repo`` depends on a target ``RepoVertex`` (which may be a
``Repo`` within the ecosystem or an ``ExternalRepo`` outside it). Both endpoints carry
FK constraints pointing to ``repo_vertices.id``, providing full referential integrity
while allowing edges to target either subtype via the JTI hierarchy.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import EdgeConfidence, PgBase, enum_values

if TYPE_CHECKING:
    from pg_atlas.db_models.repo_vertex import RepoVertex


class DependsOn(PgBase):
    """
    Directed dependency edge: ``in_vertex`` depends on ``out_vertex``.

    Both FK columns reference the ``repo_vertices`` JTI base table so that:
    - ``in_vertex_id`` always resolves to a ``Repo`` (SBOM ingestion enforces this at
      the application layer, but the DB allows any RepoVertex for flexibility).
    - ``out_vertex_id`` may resolve to either a ``Repo`` (within-ecosystem dependency)
      or an ``ExternalRepo`` (outside-ecosystem dependency).

    The composite primary key ``(in_vertex_id, out_vertex_id)`` enforces that at most
    one edge exists between any two vertices (bulk replace on re-ingestion: delete all
    edges from the source repo, then insert the new set).

    For NetworkX loading, resolve canonical IDs via a JOIN to ``repo_vertices``:

        SELECT rv_in.canonical_id AS in_vertex, rv_out.canonical_id AS out_vertex,
               d.version_range, d.confidence
        FROM depends_on d
        JOIN repo_vertices rv_in  ON rv_in.id  = d.in_vertex_id
        JOIN repo_vertices rv_out ON rv_out.id = d.out_vertex_id
    """

    __tablename__ = "depends_on"

    in_vertex_id: Mapped[int] = mapped_column(ForeignKey("repo_vertices.id"), primary_key=True)
    out_vertex_id: Mapped[int] = mapped_column(ForeignKey("repo_vertices.id"), primary_key=True)

    # --- edge properties ---
    version_range: Mapped[str | None] = mapped_column(String(256), default=None)
    confidence: Mapped[EdgeConfidence] = mapped_column(
        Enum(EdgeConfidence, name="edge_confidence", values_callable=enum_values),
        default=EdgeConfidence.inferred_shadow,
    )

    # --- relationships ---
    in_node: Mapped[RepoVertex] = relationship(
        foreign_keys=[in_vertex_id],
        lazy="selectin",
        init=False,
        repr=False,
    )
    out_node: Mapped[RepoVertex] = relationship(
        foreign_keys=[out_vertex_id],
        lazy="selectin",
        init=False,
        repr=False,
    )
