"""
GitHub dependents observation storage: crawl runs and per-dependent rows.

The GitHub dependents crawler records what it saw without touching the
canonical dependency graph: no ``DependsOn`` edges, no ``RepoVertex`` creation.
One ``GithubDependentsCrawlRun`` row exists per attempted crawl of a tracked
source repository, created durably before any HTTP so failed and abandoned
attempts stay visible. One ``GithubDependentObservation`` row exists per
``(source repo, dependent GitHub identity)`` pair and carries current state
plus first/last-seen audit fields; disappearance is recorded by soft
retirement, never deletion, so a row reactivates in place when the dependent
returns. This is current-state audit history: exact membership of past
snapshots is not reconstructable from these rows alone.

Reconciliation contract: only a run with ``listing_complete=true`` may retire
observations it did not see, and only for its own source repository. Partial
runs add and refresh what they positively observed. Run ids are monotonic and
assigned before HTTP, so they double as the start-order guard: a snapshot that
finishes after a newer run has applied is marked ``superseded`` and mutates
nothing.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import DateTime, Enum, ForeignKey, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from pg_atlas.db_models.base import GithubDependentsRunStatus, HexBinary, PgBase, enum_values, intpk


class GithubDependentsCrawlRun(PgBase):
    """
    One attempted GitHub dependents crawl for one tracked source repository.

    ``listing_complete`` gates reconciliation (may unseen observations be
    retired); ``counts_complete`` gates whether the reported header totals are
    presented as current. The two are independent: a run can enumerate the
    full public listing while one header count failed to parse.
    """

    __tablename__ = "github_dependents_crawl_runs"

    id: Mapped[intpk] = mapped_column(init=False)
    source_repo_id: Mapped[int] = mapped_column(ForeignKey("repos.id"))

    status: Mapped[GithubDependentsRunStatus] = mapped_column(
        Enum(GithubDependentsRunStatus, name="github_dependents_run_status", values_callable=enum_values),
        default=GithubDependentsRunStatus.running,
    )
    listing_complete: Mapped[bool] = mapped_column(default=False)
    counts_complete: Mapped[bool] = mapped_column(default=False)
    listing_incomplete_reason: Mapped[str | None] = mapped_column(String(64), default=None)
    counts_incomplete_reason: Mapped[str | None] = mapped_column(String(64), default=None)

    #: GitHub header totals as reported (may count entries that are not
    #: publicly enumerable); ``public_repos_observed`` is the unique public
    #: entries this run actually parsed.
    repos_total_reported: Mapped[int | None] = mapped_column(default=None)
    packages_total_reported: Mapped[int | None] = mapped_column(default=None)
    public_repos_observed: Mapped[int] = mapped_column(default=0)
    packages_scanned: Mapped[int | None] = mapped_column(default=None)
    pages_fetched: Mapped[int] = mapped_column(default=0)
    request_count: Mapped[int] = mapped_column(default=0)

    parser_version: Mapped[str | None] = mapped_column(String(32), default=None)
    app_version: Mapped[str | None] = mapped_column(String(64), default=None)
    snapshot_fingerprint: Mapped[str | None] = mapped_column(HexBinary(length=32), default=None)
    error_detail: Mapped[str | None] = mapped_column(String(4096), default=None)

    started_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        init=False,
    )
    finished_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        init=False,
    )


class GithubDependentObservation(PgBase):
    """
    Current observation state for one dependent GitHub identity of one source.

    ``dependent_key`` is the lowercased ``owner/repo`` form — GitHub identity
    is case-insensitive — and is the uniqueness and self-skip key.
    ``dependent_canonical_id`` is the stable normalized
    ``pkg:github/{dependent_key}``; the observed display casing lives in
    ``observed_owner``/``observed_repo_name``. ``resolved_repo_id`` links a
    dependent that is itself a tracked repository; untracked dependents stay
    vertex-free by design.
    """

    __tablename__ = "github_dependent_observations"
    __table_args__ = (UniqueConstraint("source_repo_id", "dependent_key"),)

    id: Mapped[intpk] = mapped_column(init=False)
    source_repo_id: Mapped[int] = mapped_column(ForeignKey("repos.id"))

    dependent_key: Mapped[str] = mapped_column(String(512))
    dependent_canonical_id: Mapped[str] = mapped_column(String(512))
    observed_owner: Mapped[str] = mapped_column(String(256))
    observed_repo_name: Mapped[str] = mapped_column(String(256))
    dependent_repo_url: Mapped[str] = mapped_column(String(1024))
    resolved_repo_id: Mapped[int | None] = mapped_column(
        ForeignKey("repos.id", ondelete="SET NULL"),
        default=None,
    )

    #: GitHub package ids through which this dependent was observed. Complete
    #: listings replace the set; partial listings union into it and never
    #: remove unseen memberships.
    package_ids: Mapped[list[str] | None] = mapped_column(JSONB(none_as_null=True), default=None)

    last_seen_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("github_dependents_crawl_runs.id"),
        default=None,
    )
    retired_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
    )
    retired_by_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("github_dependents_crawl_runs.id"),
        default=None,
    )

    first_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        init=False,
    )
    last_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        init=False,
    )


# Matches the query patterns: run lookups filter source_repo_id and order or
# aggregate by id; the active-observation reads filter source_repo_id and
# paginate ordered by dependent_key.
idx_github_dependents_runs_source_id = Index(
    "ix_github_dependents_crawl_runs_source_id",
    GithubDependentsCrawlRun.source_repo_id,
    GithubDependentsCrawlRun.id,
)
idx_github_dependent_observations_source_active = Index(
    "ix_github_dependent_observations_source_active",
    GithubDependentObservation.source_repo_id,
    GithubDependentObservation.dependent_key,
    postgresql_where=GithubDependentObservation.retired_at.is_(None),
)
