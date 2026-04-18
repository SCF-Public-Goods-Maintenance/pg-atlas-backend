"""
DB-backed tests for project adoption score materialization.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models import Project, Repo
from pg_atlas.db_models.base import ActivityStatus, ProjectType, Visibility
from pg_atlas.metrics.materialize_adoption import AdoptionMaterializationStats, materialize_adoption_scores


@dataclass(frozen=True)
class SeededAdoptionFixture:
    """
    Hold seeded project IDs for one deterministic adoption test component.
    """

    project_a_id: int
    project_b_id: int
    project_c_id: int
    empty_project_id: int


@dataclass(frozen=True)
class SeededMonorepoDownloadsFixture:
    """
    Hold seeded IDs for monorepo download aggregation tests.
    """

    project_id: int
    repo_id: int


@pytest.fixture
async def rollback_db_session(db_session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    """
    Run each adoption materialization test inside a rolled-back transaction.
    """

    transaction = await db_session.begin()
    try:
        yield db_session
    finally:
        if transaction.is_active:
            await transaction.rollback()


async def _seed_adoption_fixture(session: AsyncSession) -> SeededAdoptionFixture:
    """
    Insert deterministic repos and projects for adoption-score materialization.
    """

    suffix = uuid4().hex[:8]

    # Neutralize existing repo adoption signals inside the rollback-only
    # transaction so the percentile pools are deterministic for this test.
    # Materialization now rebuilds downloads from repo_metadata maps, so we
    # also clear repo_metadata to avoid background fixtures re-entering the
    # ranking pools.
    await session.execute(
        update(Repo).values(
            adoption_stars=None,
            adoption_forks=None,
            adoption_downloads=None,
            repo_metadata=None,
        )
    )
    await session.flush()

    project_a = Project(
        canonical_id=f"daoip-5:stellar:project:adoption-a-{suffix}",
        display_name=f"Adoption A {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    project_b = Project(
        canonical_id=f"daoip-5:stellar:project:adoption-b-{suffix}",
        display_name=f"Adoption B {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    project_c = Project(
        canonical_id=f"daoip-5:stellar:project:adoption-c-{suffix}",
        display_name=f"Adoption C {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        adoption_score=Decimal("99.00"),
    )
    empty_project = Project(
        canonical_id=f"daoip-5:stellar:project:adoption-empty-{suffix}",
        display_name=f"Adoption Empty {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        adoption_score=Decimal("88.00"),
    )
    session.add_all([project_a, project_b, project_c, empty_project])
    await session.flush()

    session.add_all(
        [
            Repo(
                canonical_id=f"pkg:github/test/adoption-a1-{suffix}",
                display_name=f"adoption-a1-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
                project_id=project_a.id,
                adoption_stars=10,
                adoption_forks=2,
            ),
            Repo(
                canonical_id=f"pkg:github/test/adoption-a2-{suffix}",
                display_name=f"adoption-a2-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
                project_id=project_a.id,
                adoption_stars=20,
                adoption_downloads=100,
                repo_metadata={
                    "adoption_downloads_by_purl": {
                        f"pkg:pypi/adoption-a2-{suffix}": 100,
                    }
                },
            ),
            Repo(
                canonical_id=f"pkg:github/test/adoption-b1-{suffix}",
                display_name=f"adoption-b1-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
                project_id=project_b.id,
                adoption_forks=6,
                adoption_downloads=300,
                repo_metadata={
                    "adoption_downloads_by_purl": {
                        f"pkg:pypi/adoption-b1-{suffix}": 300,
                    }
                },
            ),
            Repo(
                canonical_id=f"pkg:github/test/adoption-c1-{suffix}",
                display_name=f"adoption-c1-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
                project_id=project_c.id,
            ),
            Repo(
                canonical_id=f"pkg:github/test/adoption-orphan-{suffix}",
                display_name=f"adoption-orphan-{suffix}",
                visibility=Visibility.public,
                latest_version="1.0.0",
                project_id=None,
            ),
        ]
    )
    await session.flush()

    return SeededAdoptionFixture(
        project_a_id=project_a.id,
        project_b_id=project_b.id,
        project_c_id=project_c.id,
        empty_project_id=empty_project.id,
    )


async def _get_project(session: AsyncSession, project_id: int) -> Project:
    """
    Load one Project row and assert it exists.
    """

    project = await session.get(Project, project_id)
    assert project is not None

    return project


async def _seed_monorepo_download_fixture(session: AsyncSession) -> SeededMonorepoDownloadsFixture:
    """
    Insert one repo with per-PURL download metadata for aggregation tests.
    """

    suffix = uuid4().hex[:8]

    project = Project(
        canonical_id=f"daoip-5:stellar:project:adoption-monorepo-{suffix}",
        display_name=f"Adoption Monorepo {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
    )
    session.add(project)
    await session.flush()

    repo = Repo(
        canonical_id=f"pkg:github/test/adoption-monorepo-{suffix}",
        display_name=f"adoption-monorepo-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=project.id,
        adoption_downloads=5,
        repo_metadata={
            "adoption_downloads_by_purl": {
                "pkg:pub/foo": 100,
                "pkg:pub/bar": 200,
            }
        },
    )
    session.add(repo)
    await session.flush()

    return SeededMonorepoDownloadsFixture(project_id=project.id, repo_id=repo.id)


async def test_materialize_adoption_scores_persists_project_scores(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Adoption materialization should persist deterministic project aggregates.
    """

    seeded = await _seed_adoption_fixture(rollback_db_session)

    stats = await materialize_adoption_scores(rollback_db_session)

    assert isinstance(stats, AdoptionMaterializationStats)
    assert stats.repos_seen >= 5
    assert stats.repo_composites_computed >= 3
    assert stats.projects_seen >= 4
    assert stats.projects_scored >= 2

    project_a = await _get_project(rollback_db_session, seeded.project_a_id)
    project_b = await _get_project(rollback_db_session, seeded.project_b_id)
    project_c = await _get_project(rollback_db_session, seeded.project_c_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    assert project_a.adoption_score == Decimal("12.50")
    assert project_b.adoption_score == Decimal("50.00")
    assert project_c.adoption_score is None
    assert empty_project.adoption_score is None


async def test_materialize_adoption_scores_is_idempotent(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Re-running adoption materialization should preserve the same scores.
    """

    seeded = await _seed_adoption_fixture(rollback_db_session)

    await materialize_adoption_scores(rollback_db_session)
    project_a = await _get_project(rollback_db_session, seeded.project_a_id)
    project_b = await _get_project(rollback_db_session, seeded.project_b_id)
    project_c = await _get_project(rollback_db_session, seeded.project_c_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    assert project_a.adoption_score == Decimal("12.50")
    assert project_b.adoption_score == Decimal("50.00")
    assert project_c.adoption_score is None
    assert empty_project.adoption_score is None

    await materialize_adoption_scores(rollback_db_session)
    project_a = await _get_project(rollback_db_session, seeded.project_a_id)
    project_b = await _get_project(rollback_db_session, seeded.project_b_id)
    project_c = await _get_project(rollback_db_session, seeded.project_c_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    assert project_a.adoption_score == Decimal("12.50")
    assert project_b.adoption_score == Decimal("50.00")
    assert project_c.adoption_score is None
    assert empty_project.adoption_score is None


async def test_materialize_adoption_scores_updates_repo_downloads_from_metadata(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Materialization should persist summed per-PURL downloads onto Repo rows.
    """

    seeded = await _seed_monorepo_download_fixture(rollback_db_session)

    await materialize_adoption_scores(rollback_db_session)
    repo = await rollback_db_session.get(Repo, seeded.repo_id)
    assert repo is not None
    assert repo.adoption_downloads == 300

    await materialize_adoption_scores(rollback_db_session)
    repo = await rollback_db_session.get(Repo, seeded.repo_id)
    assert repo is not None
    assert repo.adoption_downloads == 300
