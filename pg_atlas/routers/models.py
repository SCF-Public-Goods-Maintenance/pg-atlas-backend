"""
Shared Pydantic response models for the PG Atlas public REST API.

All models that appear in more than one router live here, as do the generic
pagination wrapper and the ``ProjectMetadata`` validator for JSONB normalisation.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from pg_atlas.db_models.base import (
    ActivityStatus,
    EdgeConfidence,
    ProjectType,
    RepoVertexType,
    SubmissionStatus,
    Visibility,
)

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Pagination wrapper
# ---------------------------------------------------------------------------


class PaginatedResponse(BaseModel, Generic[T]):
    """
    Generic paginated response envelope.

    Every list endpoint returns this shape so that the frontend can drive
    infinite-scroll or page-based navigation uniformly.
    """

    items: list[T]
    total: int
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# Project metadata (JSONB normalisation)
# ---------------------------------------------------------------------------


class ScfSubmission(BaseModel):
    """A single SCF funding round submission."""

    round: str
    title: str


class ProjectMetadata(BaseModel):
    """
    Validates and normalises the ``project_metadata`` JSONB column on ``Project``.

    The single write path (``_build_project_metadata`` in the OpenGrants crawler)
    guarantees consistent key names and types.  ``extra = "allow"`` ensures that
    any future keys added by the crawler pass through without breaking the API.
    Future keys must still be added in this model for clarity.
    """

    model_config = {"extra": "allow"}

    scf_submissions: list[ScfSubmission] = []
    description: str | None = None
    technical_architecture: str | None = None
    scf_tranche_completion: str | None = None
    website: str | None = None
    x_profile: str | None = None
    total_awarded_usd: int | float | None = None
    total_paid_usd: int | float | None = None
    awarded_submissions_count: int | None = None
    open_source: bool | None = None
    socials: list[dict[str, Any]] | None = None
    analytics: Any | None = None
    regions_of_operation: Any | None = None


# ---------------------------------------------------------------------------
# Project response models
# ---------------------------------------------------------------------------


class ProjectSummary(BaseModel):
    """
    Compact project representation used in list endpoints and as an embedded
    reference in repo detail responses.
    """

    model_config = {"from_attributes": True}

    canonical_id: str
    display_name: str
    project_type: ProjectType
    activity_status: ActivityStatus
    category: str | None
    git_owner_url: str | None
    pony_factor: int | None
    criticality_score: int | None
    adoption_score: float | None
    updated_at: dt.datetime


class ProjectDetailResponse(ProjectSummary):
    """
    Full project detail including contributor stats and metadata.

    ``metadata`` is the normalised form of the ``project_metadata`` JSONB column.
    """

    project_id: int
    active_contributors_30d: int
    active_contributors_90d: int
    metadata: ProjectMetadata


# ---------------------------------------------------------------------------
# Repo response models
# ---------------------------------------------------------------------------


class RepoSummary(BaseModel):
    """
    Compact repo representation used in list endpoints and as an embedded
    reference in project detail responses.
    """

    model_config = {"from_attributes": True}

    canonical_id: str
    display_name: str
    visibility: Visibility
    latest_version: str
    latest_commit_date: dt.datetime | None
    repo_url: str | None
    project_id: int | None
    pony_factor: int | None
    criticality_score: int | None
    adoption_downloads: int | None
    adoption_stars: int | None
    adoption_forks: int | None
    updated_at: dt.datetime


class DepCounts(BaseModel):
    """
    Counts of dependency edges grouped by target vertex type.

    Provided as part of the repo detail response so the frontend can show
    "depends on N repos + M external" without fetching the full edge list.
    """

    repos: int
    external_repos: int


class ContributorSummary(BaseModel):
    """Compact contributor reference embedded in repo detail responses."""

    model_config = {"from_attributes": True}

    id: int
    name: str
    email_hash: str


class RepoContributorSummary(ContributorSummary):
    """Contributor summary with commit counts for one repo."""

    number_of_commits: int
    first_commit_date: dt.datetime
    last_commit_date: dt.datetime


class ProjectContributorSummary(ContributorSummary):
    """Contributor summary aggregated across all repos in a project."""

    total_commits_in_project: int


class RepoDetailResponse(RepoSummary):
    """
    Full repo detail with parent project, contributors, releases, dependency counts,
    and active contributor stats.
    """

    releases: list[dict[str, Any]] | None
    parent_project: ProjectSummary | None
    contributors: list[ContributorSummary]
    outgoing_dep_counts: DepCounts
    incoming_dep_counts: DepCounts
    active_contributors_30d: int
    active_contributors_90d: int


# ---------------------------------------------------------------------------
# Dependency edge response models
# ---------------------------------------------------------------------------


class RepoDependency(BaseModel):
    """
    A single dependency or reverse-dependency edge from a repo.

    ``vertex_type`` tells the frontend whether the target is an in-ecosystem
    ``Repo`` or an ``ExternalRepo``.
    """

    canonical_id: str
    display_name: str
    vertex_type: RepoVertexType
    version_range: str | None
    confidence: EdgeConfidence


class ProjectDependency(BaseModel):
    """
    A collapsed project-level dependency: aggregates repo-level edges
    between two projects into a single summary.
    """

    project: ProjectSummary
    edge_count: int


# ---------------------------------------------------------------------------
# Contributor response models
# ---------------------------------------------------------------------------


class ContributionEntry(BaseModel):
    """A single repo that a contributor has committed to."""

    repo_canonical_id: str
    repo_display_name: str
    project_canonical_id: str | None
    number_of_commits: int
    first_commit_date: dt.datetime
    last_commit_date: dt.datetime


class ContributorDetailResponse(BaseModel):
    """
    Full contributor detail with aggregated statistics and per-repo activity.
    """

    id: int
    name: str
    email_hash: str
    total_repos: int
    total_commits: int
    first_contribution: dt.datetime | None
    last_contribution: dt.datetime | None
    repos: list[ContributionEntry]


# ---------------------------------------------------------------------------
# Metadata response model
# ---------------------------------------------------------------------------


class MetadataResponse(BaseModel):
    """
    Ecosystem-wide summary statistics returned by ``GET /metadata``.
    """

    total_projects: int
    active_projects: int
    total_repos: int
    active_repos_90d: int
    total_external_repos: int
    total_dependency_edges: int
    total_contributor_edges: int
    active_contributors_30d: int
    active_contributors_90d: int
    last_updated: dt.datetime | None


class GitLogArtifactSummary(BaseModel):
    """Compact gitlog artifact audit record for list endpoints."""

    id: int
    repo_id: int
    repo_canonical_id: str
    repo_display_name: str
    artifact_path: str | None
    status: SubmissionStatus
    error_detail: str | None
    since_months: int
    submitted_at: dt.datetime
    processed_at: dt.datetime | None


class GitLogArtifactDetailResponse(GitLogArtifactSummary):
    """Full gitlog artifact audit record including raw artifact content."""

    raw_artifact: str | None = None
