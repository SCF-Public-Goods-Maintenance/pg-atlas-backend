"""
Swift SBOM repository resolution without changing external package identity.

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
from pg_atlas.ingestion.persist import _plan_sbom_edges, _swift_github_candidate, _upsert_sbom_vertices
from pg_atlas.ingestion.spdx import parse_and_validate_spdx

FIXTURE = Path(__file__).parents[1] / "sbom" / "data_fixtures" / "swift_dependencies.spdx.json"


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


@pytest.mark.parametrize("stored_owner", ["Soneso", "soneso", "SONESO"])
async def test_swift_links_tracked_repo_and_preserves_external_control(
    mocker: MockerFixture,
    stored_owner: str,
) -> None:
    """
    A Swift SDK links regardless of GitHub casing; untracked Collections stays external.
    """
    sbom = parse_and_validate_spdx(FIXTURE.read_bytes())
    root = _repo("pkg:github/pgm-testing/sbom-demo-swift", 11)
    sdk = _repo(f"pkg:github/{stored_owner}/stellar-ios-mac-sdk", 22)
    session = AsyncMock(spec=AsyncSession)
    query_result = Mock()
    query_result.all.return_value = [sdk]

    async def select_repos(statement: Select[tuple[Repo]]) -> Mock:
        """
        Require a case-insensitive SQL lookup, not merely a case-insensitive Python map.
        """
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        assert "lower(repo_vertices.canonical_id)" in sql
        assert "pkg:github/soneso/stellar-ios-mac-sdk" in sql

        return query_result

    session.scalars.side_effect = select_repos
    mocker.patch("pg_atlas.ingestion.persist.find_repos_by_release_purls", return_value={})
    external = ExternalRepo(
        canonical_id="pkg:swift/github.com/apple/swift-collections",
        display_name="github.com/apple/swift-collections",
        latest_version="1.1.4",
        repo_url=None,
    )
    external.id = 33
    upsert = mocker.patch("pg_atlas.ingestion.persist.upsert_external_repo", return_value=external)

    mapping, versions = await _upsert_sbom_vertices(session, sbom, root.canonical_id, root)

    assert mapping == {"SPDXRef-root": 11, "SPDXRef-sdk": 22, "SPDXRef-collections": 33}
    assert versions == {22: "3.14.0", 33: "1.1.4"}
    upsert.assert_awaited_once_with(
        session,
        canonical_id="pkg:swift/github.com/apple/swift-collections",
        display_name="github.com/apple/swift-collections",
        latest_version="1.1.4",
        repo_url=None,
    )
    session.add.assert_not_called()
    root_edges, nested = _plan_sbom_edges(sbom, root.id, mapping, versions)
    assert root_edges == {22: "3.14.0", 33: "1.1.4"}
    assert nested == []


@pytest.mark.parametrize("ambiguous", [False, True], ids=["untracked", "ambiguous-casing"])
async def test_swift_unresolved_repository_retains_package_vertices(
    mocker: MockerFixture,
    ambiguous: bool,
) -> None:
    """
    Untracked or ambiguous GitHub candidates remain Swift ExternalRepos without guessing.
    """
    sbom = parse_and_validate_spdx(FIXTURE.read_bytes())
    root = _repo("pkg:github/pgm-testing/sbom-demo-swift", 11)
    session = AsyncMock(spec=AsyncSession)
    query_result = Mock()
    query_result.all.return_value = (
        [_repo("pkg:github/Soneso/stellar-ios-mac-sdk", 22), _repo("pkg:github/soneso/stellar-ios-mac-sdk", 23)]
        if ambiguous
        else []
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
        assert canonical_id.startswith("pkg:swift/")
        repo = ExternalRepo(
            canonical_id=canonical_id, display_name=display_name, latest_version=latest_version, repo_url=repo_url
        )
        repo.id = 33 if "collections" in canonical_id else 44

        return repo

    upsert = mocker.patch("pg_atlas.ingestion.persist.upsert_external_repo", side_effect=external_repo)
    mapping, versions = await _upsert_sbom_vertices(session, sbom, root.canonical_id, root)

    assert mapping == {"SPDXRef-root": 11, "SPDXRef-sdk": 44, "SPDXRef-collections": 33}
    assert versions == {44: "3.14.0", 33: "1.1.4"}
    assert upsert.await_count == 2
    session.add.assert_not_called()


@pytest.mark.parametrize(
    ("package_id", "expected"),
    [
        ("pkg:swift/GitHub.com/Soneso/stellar-ios-mac-sdk", "pkg:github/soneso/stellar-ios-mac-sdk"),
        ("pkg:swift/gitlab.com/team/library", None),
        ("pkg:npm/github.com/team/library", None),
        ("pkg:swift/github.com/team/library/subpath", None),
        ("pkg:swift/github.com//library", None),
        ("pkg:swift/github.com/team/..", None),
        ("pkg:swift/github.com/team/library?download_url=other", None),
        ("pkg:swift/github.com/team/library#subpath", None),
    ],
)
def test_swift_github_candidate_scope(package_id: str, expected: str | None) -> None:
    """
    Limit GitHub inference to unambiguous Swift repository paths.
    """

    assert _swift_github_candidate(package_id) == expected
