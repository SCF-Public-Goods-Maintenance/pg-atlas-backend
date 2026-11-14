"""
Go SBOM repository resolution without changing external package identity.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import Select
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.base import Visibility
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo
from pg_atlas.ingestion.persist import _golang_github_candidate, _plan_sbom_edges, _upsert_sbom_vertices
from pg_atlas.ingestion.spdx import parse_and_validate_spdx

FIXTURE = Path(__file__).parents[1] / "sbom" / "data_fixtures" / "golang_dependencies.spdx.json"


def _repo(canonical_id: str, vertex_id: int) -> Repo:
    """
    Construct a tracked repository without opening a database connection.
    """
    repo = Repo(
        canonical_id=canonical_id,
        display_name=canonical_id.rsplit("/", 1)[-1],
        visibility=Visibility.public,
        latest_version="main",
    )
    repo.id = vertex_id

    return repo


@pytest.mark.parametrize("stored_owner", ["withObsrvr", "withobsrvr", "WITHOBSRVR"])
async def test_golang_links_tracked_repo_and_preserves_external_control(
    mocker: MockerFixture,
    stored_owner: str,
) -> None:
    """
    A Go module links regardless of GitHub casing; untracked UUID stays external.
    """
    sbom = parse_and_validate_spdx(FIXTURE.read_bytes())
    root = _repo("pkg:github/pgm-testing/sbom-demo-go", 11)
    sdk = _repo(f"pkg:github/{stored_owner}/nebu", 22)
    session = AsyncMock(spec=AsyncSession)
    query_result = Mock()
    query_result.all.return_value = [sdk]

    async def select_repos(statement: Select[tuple[Repo]]) -> Mock:
        """
        Require a case-insensitive SQL lookup, not merely a case-insensitive Python map.
        """
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        assert "lower(repo_vertices.canonical_id)" in sql
        assert "pkg:github/withobsrvr/nebu" in sql

        return query_result

    session.scalars.side_effect = select_repos
    mocker.patch("pg_atlas.ingestion.persist.find_repos_by_release_purls", return_value={})
    external = ExternalRepo(
        canonical_id="pkg:golang/github.com/google/uuid",
        display_name="github.com/google/uuid",
        latest_version="v1.6.0",
        repo_url=None,
    )
    external.id = 33
    upsert = mocker.patch("pg_atlas.ingestion.persist.upsert_external_repo", return_value=external)

    mapping, versions = await _upsert_sbom_vertices(session, sbom, root.canonical_id, root)

    assert mapping == {"SPDXRef-root": 11, "SPDXRef-sdk": 22, "SPDXRef-uuid": 33}
    assert versions == {22: "v0.7.2", 33: "v1.6.0"}
    upsert.assert_awaited_once_with(
        session,
        canonical_id="pkg:golang/github.com/google/uuid",
        display_name="github.com/google/uuid",
        latest_version="v1.6.0",
        repo_url=None,
    )
    session.add.assert_not_called()
    root_edges, nested = _plan_sbom_edges(sbom, root.id, mapping, versions)
    assert root_edges == {22: "v0.7.2", 33: "v1.6.0"}
    assert nested == []


@pytest.mark.parametrize("ambiguous", [False, True], ids=["untracked", "ambiguous-casing"])
async def test_golang_unresolved_repository_retains_package_vertices(
    mocker: MockerFixture,
    ambiguous: bool,
) -> None:
    """
    Untracked or ambiguous GitHub candidates remain Go ExternalRepos without guessing.
    """
    sbom = parse_and_validate_spdx(FIXTURE.read_bytes())
    root = _repo("pkg:github/pgm-testing/sbom-demo-go", 11)
    session = AsyncMock(spec=AsyncSession)
    query_result = Mock()
    query_result.all.return_value = (
        [_repo("pkg:github/withObsrvr/nebu", 22), _repo("pkg:github/withobsrvr/nebu", 23)] if ambiguous else []
    )
    session.scalars.return_value = query_result
    mocker.patch("pg_atlas.ingestion.persist.find_repos_by_release_purls", return_value={})

    async def external_repo(
        _session: AsyncSession,
        *,
        canonical_id: str,
        display_name: str,
        latest_version: str,
        repo_url: str | None,
    ) -> ExternalRepo:
        """
        Preserve the package identity and return an isolated external vertex.
        """
        assert canonical_id.startswith("pkg:golang/")
        repo = ExternalRepo(
            canonical_id=canonical_id, display_name=display_name, latest_version=latest_version, repo_url=repo_url
        )
        repo.id = 33 if "uuid" in canonical_id else 44

        return repo

    upsert = mocker.patch("pg_atlas.ingestion.persist.upsert_external_repo", side_effect=external_repo)
    mapping, versions = await _upsert_sbom_vertices(session, sbom, root.canonical_id, root)

    assert mapping == {"SPDXRef-root": 11, "SPDXRef-sdk": 44, "SPDXRef-uuid": 33}
    assert versions == {44: "v0.7.2", 33: "v1.6.0"}
    assert upsert.await_count == 2
    session.add.assert_not_called()


@pytest.mark.parametrize("suffix", ["/v2", "/submodule", "/submodule/v2", "?goos=linux", "#subpath", "/..", "/"])
def test_go_non_root_paths_remain_external(suffix: str) -> None:
    """
    Defer submodule and major-version inference to verified release metadata.
    """

    assert _golang_github_candidate("pkg:golang/github.com/withobsrvr/nebu" + suffix) is None


@pytest.mark.parametrize(
    "package_id", ["pkg:golang/gitlab.com/team/repo", "pkg:npm/github.com/team/repo", "pkg:golang/github.com//repo"]
)
def test_go_other_hosts_and_invalid_paths(package_id: str) -> None:
    """
    Do not infer repositories for other ecosystems or ambiguous paths.
    """

    assert _golang_github_candidate(package_id) is None
