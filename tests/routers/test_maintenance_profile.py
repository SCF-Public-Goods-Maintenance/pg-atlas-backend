"""
Tests for the /repos/{canonical_id}/maintenance-profile endpoint over ASGI.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.metrics.maintenance import MAINTENANCE_PROFILE_KEY
from tests.conftest import get_test_database_url


async def test_maintenance_profile_db_unavailable_returns_503(no_db_client: AsyncClient) -> None:
    """The endpoint returns 503 when no database is configured."""

    resp = await no_db_client.get("/repos/pkg:github/test/repo/maintenance-profile")
    assert resp.status_code == 503


def _profile_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "as_of": "2026-09-15T12:00:00+00:00",
        "window_days": 180,
        "response_interval_days": 7,
        "eligible_population": 9,
        "signals": {
            "release_cadence": {
                "source": "depsdev",
                "shipping_events": 10,
                "release_ships_code": True,
                "median_gap_days": {"state": "ok", "value": 23.0, "percentile": 0.875, "pool_size": 8},
                "days_since_last_release": {"state": "ok", "value": 22.0, "percentile": 0.75, "pool_size": 8},
            },
            "issue_responsiveness": {
                "window_days": 180,
                "interval_days": 7,
                "cohort_size": 4,
                "eligible_size": 4,
                "responded_within_interval": 4,
                "median_response_days_context": 0.08,
                "unknown_attribution_count": 0,
                "maintainer_authored_count": 2,
                "response_fraction": {"state": "ok", "value": 1.0, "percentile": 1.0, "pool_size": 5},
            },
            "issue_backlog": {
                "open_count": {"state": "ok", "value": 0, "percentile": 1.0, "pool_size": 9},
                "median_open_age_days": {"state": "not-applicable"},
            },
            "activity_recency": {
                "days_since_push": {"state": "unavailable"},
                "commit_count_window": {"state": "incomplete", "reason": "stale-artifact"},
            },
        },
    }


async def _store_profile(canonical_id: str, document: dict[str, Any]) -> None:
    """Write a profile document into the repo's metadata directly."""

    database_url = get_test_database_url()
    assert database_url is not None
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as session:
            repo_id = (await session.execute(select(Repo.id).where(Repo.canonical_id == canonical_id))).scalar_one()
            await session.execute(
                update(Repo)
                .where(Repo.id == repo_id)
                .values(repo_metadata={MAINTENANCE_PROFILE_KEY: document, "other_key": "kept"})
                .execution_options(synchronize_session=False)
            )
            await session.commit()
    finally:
        await engine.dispose()


async def test_unknown_repo_returns_404(seeded_client: tuple[AsyncClient, dict[str, Any]]) -> None:
    client, _ = seeded_client

    resp = await client.get("/repos/pkg:github/nobody/nothing/maintenance-profile")
    assert resp.status_code == 404


async def test_repo_without_profile_returns_404(seeded_client: tuple[AsyncClient, dict[str, Any]]) -> None:
    """A tracked repo with no materialized profile is a 404, not an empty document."""

    client, seed = seeded_client
    canonical_id = seed["repo_a1"].canonical_id

    resp = await client.get(f"/repos/{canonical_id}/maintenance-profile")
    assert resp.status_code == 404
    assert "profile" in resp.json()["detail"].lower()


async def test_profile_key_with_non_document_value_returns_404(
    seeded_client: tuple[AsyncClient, dict[str, Any]],
) -> None:
    """A corrupted profile value under the key is a 404, the same as absence."""

    client, seed = seeded_client
    canonical_id = seed["repo_a2"].canonical_id
    database_url = get_test_database_url()
    assert database_url is not None
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as session:
            repo_id = (await session.execute(select(Repo.id).where(Repo.canonical_id == canonical_id))).scalar_one()
            await session.execute(
                update(Repo)
                .where(Repo.id == repo_id)
                .values(repo_metadata={MAINTENANCE_PROFILE_KEY: "corrupted"})
                .execution_options(synchronize_session=False)
            )
            await session.commit()
    finally:
        await engine.dispose()

    resp = await client.get(f"/repos/{canonical_id}/maintenance-profile")
    assert resp.status_code == 404


async def test_stored_profile_is_served_verbatim(seeded_client: tuple[AsyncClient, dict[str, Any]]) -> None:
    """The endpoint serves the materialized document: states, ranks, pools, as-of."""

    client, seed = seeded_client
    canonical_id = seed["repo_a1"].canonical_id
    document = _profile_document()
    await _store_profile(canonical_id, document)

    resp = await client.get(f"/repos/{canonical_id}/maintenance-profile")
    assert resp.status_code == 200

    body = resp.json()
    assert body["schema_version"] == 1
    assert body["as_of"] == document["as_of"]
    assert body["window_days"] == 180
    assert body["response_interval_days"] == 7
    assert body["eligible_population"] == 9
    assert body["signals"] == document["signals"]

    cadence = body["signals"]["release_cadence"]["median_gap_days"]
    assert cadence == {"state": "ok", "value": 23.0, "percentile": 0.875, "pool_size": 8}
    assert body["signals"]["release_cadence"]["release_ships_code"] is True
    assert body["signals"]["issue_backlog"]["median_open_age_days"] == {"state": "not-applicable"}
    assert body["signals"]["activity_recency"]["days_since_push"] == {"state": "unavailable"}
    assert body["signals"]["activity_recency"]["commit_count_window"]["reason"] == "stale-artifact"


async def test_undetermined_release_ships_code_is_served_as_null(seeded_client: tuple[AsyncClient, dict[str, Any]]) -> None:
    """An undetermined ``release_ships_code`` stays an explicit null, distinct from false."""

    client, seed = seeded_client
    canonical_id = seed["repo_a1"].canonical_id
    document = _profile_document()
    document["signals"]["release_cadence"]["release_ships_code"] = None
    await _store_profile(canonical_id, document)

    resp = await client.get(f"/repos/{canonical_id}/maintenance-profile")
    assert resp.status_code == 200

    cadence = resp.json()["signals"]["release_cadence"]
    assert "release_ships_code" in cadence
    assert cadence["release_ships_code"] is None
