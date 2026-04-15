"""
DB-backed tests for A9 criticality materialization.

These tests create a disconnected mini-graph inside a rollback-only
transaction so they can verify exact persisted scores without mutating the
shared development dataset.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models import DependsOn, ExternalRepo, Project, Repo
from pg_atlas.db_models.base import ActivityStatus, EdgeConfidence, ProjectType, Visibility
from pg_atlas.metrics.materialize import CriticalityMaterializationStats, materialize_criticality_scores


@dataclass(frozen=True)
class SeededCriticalityFixture:
    """
    Hold the canonical IDs for one disconnected test component.
    """

    leaf_project_id: int
    core_project_id: int
    zero_project_id: int
    dormant_project_id: int
    empty_project_id: int
    leaf_repo_id: int
    core_repo_id: int
    zero_repo_id: int
    dormant_repo_id: int
    external_repo_id: int


@pytest.fixture
async def rollback_db_session(db_session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    """
    Run each materialization test inside a transaction that is rolled back.
    """

    transaction = await db_session.begin()
    try:
        yield db_session
    finally:
        if transaction.is_active:
            await transaction.rollback()


async def _seed_disconnected_component(session: AsyncSession) -> SeededCriticalityFixture:
    """
    Insert a disconnected graph component with deterministic A9 scores.

    The seeded shape is:

        leaf_repo (live) -> core_repo (discontinued) -> external_repo
        zero_repo (live, isolated)
        dormant_repo (discontinued, isolated)

    Expected dep-layer criticality after A6 -> A9:
        leaf_repo     = 0
        core_repo     = 1
        zero_repo     = 0
        dormant_repo  = 0
        external_repo = 2
    """

    suffix = uuid4().hex[:8]

    leaf_project = Project(
        canonical_id=f"daoip-5:stellar:project:leaf-{suffix}",
        display_name=f"Leaf Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    core_project = Project(
        canonical_id=f"daoip-5:stellar:project:core-{suffix}",
        display_name=f"Core Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.discontinued,
    )
    zero_project = Project(
        canonical_id=f"daoip-5:stellar:project:zero-{suffix}",
        display_name=f"Zero Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    dormant_project = Project(
        canonical_id=f"daoip-5:stellar:project:dormant-{suffix}",
        display_name=f"Dormant Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.discontinued,
    )
    empty_project = Project(
        canonical_id=f"daoip-5:stellar:project:empty-{suffix}",
        display_name=f"Empty Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    session.add_all([leaf_project, core_project, zero_project, dormant_project, empty_project])
    await session.flush()

    leaf_repo = Repo(
        canonical_id=f"pkg:github/test/leaf-{suffix}",
        display_name=f"leaf-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=leaf_project.id,
    )
    core_repo = Repo(
        canonical_id=f"pkg:github/test/core-{suffix}",
        display_name=f"core-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=core_project.id,
    )
    zero_repo = Repo(
        canonical_id=f"pkg:github/test/zero-{suffix}",
        display_name=f"zero-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=zero_project.id,
    )
    dormant_repo = Repo(
        canonical_id=f"pkg:github/test/dormant-{suffix}",
        display_name=f"dormant-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=dormant_project.id,
    )
    external_repo = ExternalRepo(
        canonical_id=f"pkg:npm/ext-{suffix}",
        display_name=f"ext-{suffix}",
        latest_version="1.0.0",
    )
    session.add_all([leaf_repo, core_repo, zero_repo, dormant_repo, external_repo])
    await session.flush()

    session.add_all(
        [
            DependsOn(
                in_vertex_id=leaf_repo.id,
                out_vertex_id=core_repo.id,
                confidence=EdgeConfidence.verified_sbom,
            ),
            DependsOn(
                in_vertex_id=core_repo.id,
                out_vertex_id=external_repo.id,
                confidence=EdgeConfidence.verified_sbom,
            ),
        ]
    )
    await session.flush()

    return SeededCriticalityFixture(
        leaf_project_id=leaf_project.id,
        core_project_id=core_project.id,
        zero_project_id=zero_project.id,
        dormant_project_id=dormant_project.id,
        empty_project_id=empty_project.id,
        leaf_repo_id=leaf_repo.id,
        core_repo_id=core_repo.id,
        zero_repo_id=zero_repo.id,
        dormant_repo_id=dormant_repo.id,
        external_repo_id=external_repo.id,
    )


async def _get_repo(session: AsyncSession, repo_id: int) -> Repo:
    """
    Load one Repo row and assert it exists.
    """

    repo = await session.get(Repo, repo_id)
    assert repo is not None

    return repo


async def _get_external_repo(session: AsyncSession, repo_id: int) -> ExternalRepo:
    """
    Load one ExternalRepo row and assert it exists.
    """

    external_repo = await session.get(ExternalRepo, repo_id)
    assert external_repo is not None

    return external_repo


async def _get_project(session: AsyncSession, project_id: int) -> Project:
    """
    Load one Project row and assert it exists.
    """

    project = await session.get(Project, project_id)
    assert project is not None

    return project


async def test_materialize_criticality_scores_persists_repo_external_and_project_scores(
    rollback_db_session: AsyncSession,
) -> None:
    """
    A9 materialization must persist dep-layer scores and project aggregates.
    """

    seeded = await _seed_disconnected_component(rollback_db_session)

    stats = await materialize_criticality_scores(rollback_db_session)

    assert isinstance(stats, CriticalityMaterializationStats)
    assert stats.repo_rows_updated >= 4
    assert stats.external_repo_rows_updated >= 1
    assert stats.project_rows_updated >= 5

    leaf_repo = await rollback_db_session.get(Repo, seeded.leaf_repo_id)
    core_repo = await rollback_db_session.get(Repo, seeded.core_repo_id)
    zero_repo = await rollback_db_session.get(Repo, seeded.zero_repo_id)
    dormant_repo = await rollback_db_session.get(Repo, seeded.dormant_repo_id)
    external_repo = await rollback_db_session.get(ExternalRepo, seeded.external_repo_id)

    assert leaf_repo is not None
    assert core_repo is not None
    assert zero_repo is not None
    assert dormant_repo is not None
    assert external_repo is not None

    assert leaf_repo.criticality_score == 0
    assert core_repo.criticality_score == 1
    assert zero_repo.criticality_score == 0
    assert dormant_repo.criticality_score == 0
    assert external_repo.criticality_score == 2

    leaf_project = await rollback_db_session.get(Project, seeded.leaf_project_id)
    core_project = await rollback_db_session.get(Project, seeded.core_project_id)
    zero_project = await rollback_db_session.get(Project, seeded.zero_project_id)
    dormant_project = await rollback_db_session.get(Project, seeded.dormant_project_id)
    empty_project = await rollback_db_session.get(Project, seeded.empty_project_id)

    assert leaf_project is not None
    assert core_project is not None
    assert zero_project is not None
    assert dormant_project is not None
    assert empty_project is not None

    assert leaf_project.criticality_score == 0
    assert core_project.criticality_score == 1
    assert zero_project.criticality_score == 0
    assert dormant_project.criticality_score == 0
    assert empty_project.criticality_score is None


async def test_materialize_criticality_scores_is_idempotent(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Re-running A9 materialization must leave persisted scores unchanged.
    """

    seeded = await _seed_disconnected_component(rollback_db_session)

    await materialize_criticality_scores(rollback_db_session)
    leaf_repo = await _get_repo(rollback_db_session, seeded.leaf_repo_id)
    core_repo = await _get_repo(rollback_db_session, seeded.core_repo_id)
    zero_repo = await _get_repo(rollback_db_session, seeded.zero_repo_id)
    dormant_repo = await _get_repo(rollback_db_session, seeded.dormant_repo_id)
    external_repo = await _get_external_repo(rollback_db_session, seeded.external_repo_id)
    leaf_project = await _get_project(rollback_db_session, seeded.leaf_project_id)
    core_project = await _get_project(rollback_db_session, seeded.core_project_id)
    zero_project = await _get_project(rollback_db_session, seeded.zero_project_id)
    dormant_project = await _get_project(rollback_db_session, seeded.dormant_project_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    first_scores = (
        leaf_repo.criticality_score,
        core_repo.criticality_score,
        zero_repo.criticality_score,
        dormant_repo.criticality_score,
        external_repo.criticality_score,
        leaf_project.criticality_score,
        core_project.criticality_score,
        zero_project.criticality_score,
        dormant_project.criticality_score,
        empty_project.criticality_score,
    )

    await materialize_criticality_scores(rollback_db_session)
    leaf_repo = await _get_repo(rollback_db_session, seeded.leaf_repo_id)
    core_repo = await _get_repo(rollback_db_session, seeded.core_repo_id)
    zero_repo = await _get_repo(rollback_db_session, seeded.zero_repo_id)
    dormant_repo = await _get_repo(rollback_db_session, seeded.dormant_repo_id)
    external_repo = await _get_external_repo(rollback_db_session, seeded.external_repo_id)
    leaf_project = await _get_project(rollback_db_session, seeded.leaf_project_id)
    core_project = await _get_project(rollback_db_session, seeded.core_project_id)
    zero_project = await _get_project(rollback_db_session, seeded.zero_project_id)
    dormant_project = await _get_project(rollback_db_session, seeded.dormant_project_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    second_scores = (
        leaf_repo.criticality_score,
        core_repo.criticality_score,
        zero_repo.criticality_score,
        dormant_repo.criticality_score,
        external_repo.criticality_score,
        leaf_project.criticality_score,
        core_project.criticality_score,
        zero_project.criticality_score,
        dormant_project.criticality_score,
        empty_project.criticality_score,
    )

    assert first_scores == second_scores
