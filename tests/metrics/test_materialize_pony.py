"""
DB-backed tests for pony-factor materialization.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models import ContributedTo, Contributor, GitLogArtifact, Project, Repo
from pg_atlas.db_models.base import ActivityStatus, ProjectType, SubmissionStatus, Visibility
from pg_atlas.gitlog.parser import hash_email
from pg_atlas.metrics.materialize_pony import PonyFactorMaterializationStats, materialize_pony_factor_scores


@dataclass(frozen=True)
class SeededPonyFixture:
    """
    Hold the row ids for one deterministic pony-factor component.
    """

    alpha_project_id: int
    beta_project_id: int
    gamma_project_id: int
    empty_project_id: int
    repo_a1_id: int
    repo_a2_id: int
    repo_b1_id: int
    repo_g1_id: int
    seed_run_ordinal_lower: int
    seed_run_ordinal_upper: int


@pytest.fixture
async def rollback_db_session(db_session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    """
    Run each pony-factor test inside a transaction that is rolled back.
    """

    transaction = await db_session.begin()
    try:
        yield db_session
    finally:
        if transaction.is_active:
            await transaction.rollback()


async def _seed_pony_component(session: AsyncSession) -> SeededPonyFixture:
    """
    Insert repos, contributors, and gitlog artifacts with deterministic outcomes.

    Expected full recompute results:
    - repo_a1 = 1  (shared=3, bob=2)
    - repo_a2 = 1  (carol=5, shared=2)
    - repo_b1 = 1  (dave=4, erin=4)
    - repo_g1 = 0  (no contributor edges)
    - project alpha = 2  (shared=5, carol=5, bob=2)
    - project beta = 1
    - project gamma = 0
    - empty project = NULL
    """

    suffix = uuid4().hex[:8]

    # Choose ordinals above any existing data so MAX() resolution is predictable.
    current_max: int = int(await session.scalar(select(func.max(GitLogArtifact.seed_run_ordinal))) or 0)
    ordinal_lower = current_max + 1
    ordinal_upper = current_max + 2

    # Guard: verify that neither ordinal is already in use.
    existing = (
        await session.execute(
            select(GitLogArtifact.seed_run_ordinal)
            .where(GitLogArtifact.seed_run_ordinal.in_([ordinal_lower, ordinal_upper]))
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        raise RuntimeError(
            f"seed_run_ordinal collision: ordinal {existing} is already in use. "
            "Cannot seed test data without causing downstream assertion failures."
        )

    alpha_project = Project(
        canonical_id=f"daoip-5:stellar:project:alpha-{suffix}",
        display_name=f"Alpha Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        pony_factor=99,
    )
    beta_project = Project(
        canonical_id=f"daoip-5:stellar:project:beta-{suffix}",
        display_name=f"Beta Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        pony_factor=99,
    )
    gamma_project = Project(
        canonical_id=f"daoip-5:stellar:project:gamma-{suffix}",
        display_name=f"Gamma Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        pony_factor=99,
    )
    empty_project = Project(
        canonical_id=f"daoip-5:stellar:project:empty-{suffix}",
        display_name=f"Empty Project {suffix}",
        project_type=ProjectType.scf_project,
        activity_status=ActivityStatus.live,
        pony_factor=99,
    )
    session.add_all([alpha_project, beta_project, gamma_project, empty_project])
    await session.flush()

    repo_a1 = Repo(
        canonical_id=f"pkg:github/test/alpha-a1-{suffix}",
        display_name=f"alpha-a1-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=alpha_project.id,
        pony_factor=99,
    )
    repo_a2 = Repo(
        canonical_id=f"pkg:github/test/alpha-a2-{suffix}",
        display_name=f"alpha-a2-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=alpha_project.id,
        pony_factor=99,
    )
    repo_b1 = Repo(
        canonical_id=f"pkg:github/test/beta-b1-{suffix}",
        display_name=f"beta-b1-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=beta_project.id,
        pony_factor=99,
    )
    repo_g1 = Repo(
        canonical_id=f"pkg:github/test/gamma-g1-{suffix}",
        display_name=f"gamma-g1-{suffix}",
        visibility=Visibility.public,
        latest_version="1.0.0",
        project_id=gamma_project.id,
        pony_factor=99,
    )
    session.add_all([repo_a1, repo_a2, repo_b1, repo_g1])
    await session.flush()

    contributors = [
        Contributor(email_hash=hash_email("shared@example.com"), name="Shared Dev"),
        Contributor(email_hash=hash_email("bob@example.com"), name="Bob"),
        Contributor(email_hash=hash_email("carol@example.com"), name="Carol"),
        Contributor(email_hash=hash_email("dave@example.com"), name="Dave"),
        Contributor(email_hash=hash_email("erin@example.com"), name="Erin"),
    ]
    session.add_all(contributors)
    await session.flush()

    shared, bob, carol, dave, erin = contributors
    session.add_all(
        [
            ContributedTo(
                contributor_id=shared.id,
                repo_id=repo_a1.id,
                number_of_commits=3,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
            ContributedTo(
                contributor_id=bob.id,
                repo_id=repo_a1.id,
                number_of_commits=2,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
            ContributedTo(
                contributor_id=carol.id,
                repo_id=repo_a2.id,
                number_of_commits=5,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
            ContributedTo(
                contributor_id=shared.id,
                repo_id=repo_a2.id,
                number_of_commits=2,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
            ContributedTo(
                contributor_id=dave.id,
                repo_id=repo_b1.id,
                number_of_commits=4,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
            ContributedTo(
                contributor_id=erin.id,
                repo_id=repo_b1.id,
                number_of_commits=4,
                first_commit_date=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                last_commit_date=dt.datetime(2025, 6, 1, tzinfo=dt.UTC),
            ),
        ]
    )

    session.add_all(
        [
            GitLogArtifact(
                repo_id=repo_a1.id,
                since_months=12,
                seed_run_ordinal=ordinal_lower,
                status=SubmissionStatus.processed,
            ),
            GitLogArtifact(
                repo_id=repo_g1.id,
                since_months=12,
                seed_run_ordinal=ordinal_lower,
                status=SubmissionStatus.processed,
            ),
            GitLogArtifact(
                repo_id=repo_b1.id,
                since_months=12,
                seed_run_ordinal=ordinal_upper,
                status=SubmissionStatus.processed,
            ),
        ]
    )
    await session.flush()

    return SeededPonyFixture(
        alpha_project_id=alpha_project.id,
        beta_project_id=beta_project.id,
        gamma_project_id=gamma_project.id,
        empty_project_id=empty_project.id,
        repo_a1_id=repo_a1.id,
        repo_a2_id=repo_a2.id,
        repo_b1_id=repo_b1.id,
        repo_g1_id=repo_g1.id,
        seed_run_ordinal_lower=ordinal_lower,
        seed_run_ordinal_upper=ordinal_upper,
    )


async def _get_repo(session: AsyncSession, repo_id: int) -> Repo:
    """
    Load one Repo row and assert it exists.
    """

    repo = await session.get(Repo, repo_id)
    assert repo is not None

    return repo


async def _get_project(session: AsyncSession, project_id: int) -> Project:
    """
    Load one Project row and assert it exists.
    """

    project = await session.get(Project, project_id)
    assert project is not None

    return project


async def test_materialize_pony_factor_scores_persists_repo_and_project_scores(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Full recompute must persist repo and project pony factors.
    """

    seeded = await _seed_pony_component(rollback_db_session)

    stats = await materialize_pony_factor_scores(rollback_db_session)

    assert isinstance(stats, PonyFactorMaterializationStats)
    assert stats.resolved_seed_run_ordinal is None
    assert stats.repo_rows_updated >= 4
    assert stats.project_rows_updated >= 4

    repo_a1 = await _get_repo(rollback_db_session, seeded.repo_a1_id)
    repo_a2 = await _get_repo(rollback_db_session, seeded.repo_a2_id)
    repo_b1 = await _get_repo(rollback_db_session, seeded.repo_b1_id)
    repo_g1 = await _get_repo(rollback_db_session, seeded.repo_g1_id)

    assert repo_a1.pony_factor == 1
    assert repo_a2.pony_factor == 1
    assert repo_b1.pony_factor == 1
    assert repo_g1.pony_factor == 0

    alpha_project = await _get_project(rollback_db_session, seeded.alpha_project_id)
    beta_project = await _get_project(rollback_db_session, seeded.beta_project_id)
    gamma_project = await _get_project(rollback_db_session, seeded.gamma_project_id)
    empty_project = await _get_project(rollback_db_session, seeded.empty_project_id)

    assert alpha_project.pony_factor == 2
    assert beta_project.pony_factor == 1
    assert gamma_project.pony_factor == 0
    assert empty_project.pony_factor is None


async def test_materialize_pony_factor_scores_is_idempotent(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Re-running the same full recompute must preserve the same values.
    """

    seeded = await _seed_pony_component(rollback_db_session)

    first_stats = await materialize_pony_factor_scores(rollback_db_session)
    second_stats = await materialize_pony_factor_scores(rollback_db_session)

    assert first_stats.repo_rows_updated == second_stats.repo_rows_updated
    assert first_stats.project_rows_updated == second_stats.project_rows_updated

    alpha_project = await _get_project(rollback_db_session, seeded.alpha_project_id)
    gamma_project = await _get_project(rollback_db_session, seeded.gamma_project_id)
    assert alpha_project.pony_factor == 2
    assert gamma_project.pony_factor == 0


async def test_materialize_pony_factor_scores_latest_seed_run_limits_updates(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Latest-seed-run recompute must touch only repos in the latest seed run and their projects.
    """

    seeded = await _seed_pony_component(rollback_db_session)

    stats = await materialize_pony_factor_scores(rollback_db_session, latest_seed_run=True)

    assert stats.resolved_seed_run_ordinal == seeded.seed_run_ordinal_upper
    assert stats.repo_rows_updated == 1
    assert stats.project_rows_updated == 1

    repo_a1 = await _get_repo(rollback_db_session, seeded.repo_a1_id)
    repo_a2 = await _get_repo(rollback_db_session, seeded.repo_a2_id)
    repo_b1 = await _get_repo(rollback_db_session, seeded.repo_b1_id)
    repo_g1 = await _get_repo(rollback_db_session, seeded.repo_g1_id)
    alpha_project = await _get_project(rollback_db_session, seeded.alpha_project_id)
    beta_project = await _get_project(rollback_db_session, seeded.beta_project_id)
    gamma_project = await _get_project(rollback_db_session, seeded.gamma_project_id)

    assert repo_a1.pony_factor == 99
    assert repo_a2.pony_factor == 99
    assert repo_b1.pony_factor == 1
    assert repo_g1.pony_factor == 99
    assert alpha_project.pony_factor == 99
    assert beta_project.pony_factor == 1
    assert gamma_project.pony_factor == 99


async def test_materialize_pony_factor_scores_explicit_seed_run_recomputes_affected_project(
    rollback_db_session: AsyncSession,
) -> None:
    """
    Explicit seed-run recompute must update affected projects from contributor edges, not repo rows.
    """

    seeded = await _seed_pony_component(rollback_db_session)

    stats = await materialize_pony_factor_scores(rollback_db_session, seed_run_ordinal=seeded.seed_run_ordinal_lower)

    assert stats.resolved_seed_run_ordinal == seeded.seed_run_ordinal_lower
    assert stats.repo_rows_updated == 2
    assert stats.project_rows_updated == 2

    repo_a1 = await _get_repo(rollback_db_session, seeded.repo_a1_id)
    repo_a2 = await _get_repo(rollback_db_session, seeded.repo_a2_id)
    repo_b1 = await _get_repo(rollback_db_session, seeded.repo_b1_id)
    repo_g1 = await _get_repo(rollback_db_session, seeded.repo_g1_id)
    alpha_project = await _get_project(rollback_db_session, seeded.alpha_project_id)
    beta_project = await _get_project(rollback_db_session, seeded.beta_project_id)
    gamma_project = await _get_project(rollback_db_session, seeded.gamma_project_id)

    assert repo_a1.pony_factor == 1
    assert repo_a2.pony_factor == 99
    assert repo_b1.pony_factor == 99
    assert repo_g1.pony_factor == 0
    assert alpha_project.pony_factor == 2
    assert beta_project.pony_factor == 99
    assert gamma_project.pony_factor == 0
