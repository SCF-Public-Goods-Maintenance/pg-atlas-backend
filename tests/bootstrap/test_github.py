"""
Unit tests for ``pg_atlas.procrastinate.github`` helper functions.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

from pg_atlas.procrastinate import github as gh


def _graphql_response(
    *,
    nodes: list[gh._ManifestGraphNode],
    has_next_page: bool = False,
    end_cursor: str | None = None,
) -> gh._GraphQLResponse:
    """Build a typed GraphQL response object for manifest discovery tests."""

    page_info = gh._PageInfo(hasNextPage=has_next_page, endCursor=end_cursor)
    manifests = gh._DependencyGraphManifestConnection(nodes=nodes, pageInfo=page_info)
    repository = gh._RepositoryGraphPayload(dependencyGraphManifests=manifests)

    return gh._GraphQLResponse(data=gh._GraphQLData(repository=repository), errors=None)


def test_manifest_paths_from_graphql_filters_non_parseable_and_ignored_paths(monkeypatch: Any) -> None:
    """Manifest listing should keep only parseable, non-ignored manifest files."""

    response = _graphql_response(
        nodes=[
            gh._ManifestGraphNode(filename="apps/sdk/package.json", parseable=True, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/example/package.json", parseable=True, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/invalid/composer.json", parseable=False, exceedsMaxSize=False),
            gh._ManifestGraphNode(filename="apps/too-big/pubspec.yaml", parseable=True, exceedsMaxSize=True),
        ]
    )

    def _mock_graph_query(owner: str, repo_name: str, after: str | None) -> gh._GraphQLResponse:
        del owner, repo_name, after
        return response

    monkeypatch.setattr(gh, "_run_dependency_graph_query", _mock_graph_query)

    paths = gh._manifest_paths_from_graphql("test-org", "test-repo")

    assert paths == ["apps/sdk/package.json"]


def test_detect_packages_from_repo_uses_graphql_manifest_paths(monkeypatch: Any) -> None:
    """Package detection should read manifests listed by GraphQL and parse package names."""

    class _FakeGitHubClient:
        def get_repo(self, repo_path: str) -> SimpleNamespace:
            return SimpleNamespace(repo_path=repo_path)

    monkeypatch.setattr(gh, "get_github_client", lambda: _FakeGitHubClient())

    def _mock_manifest_paths(owner: str, repo_name: str) -> list[str]:
        del owner, repo_name
        return ["package.json"]

    def _mock_read_manifest_text(repo: Any, path: str) -> str:
        del repo, path
        return '{"name":"pkg-name","version":"1.0.0"}'

    monkeypatch.setattr(gh, "_manifest_paths_from_graphql", _mock_manifest_paths)
    monkeypatch.setattr(gh, "_read_manifest_text", _mock_read_manifest_text)

    package_refs = gh.detect_packages_from_repo("test-org", "test-repo")

    assert len(package_refs) == 1
    assert package_refs[0].system == "NPM"
    assert package_refs[0].name == "pkg-name"


def test_extract_package_name_npm_private_fast_path_skips_even_if_json_is_invalid() -> None:
    """NPM manifest should be skipped quickly when private=true appears in raw JSON text."""

    manifest_text = '{"name": "private-pkg", "private": true, "version": '

    assert gh._extract_package_name("NPM", "package.json", manifest_text, "org", "repo") is None


def test_extract_package_name_npm_requires_version_field() -> None:
    """NPM package should be treated as non-published when version is absent."""

    manifest_text = '{"name": "example-no-version"}'

    assert gh._extract_package_name("NPM", "package.json", manifest_text, "org", "repo") is None


def test_extract_package_name_go_handles_quotes_and_comments() -> None:
    """Go module parser should ignore trailing comments and optional quotes."""

    quoted = 'module "github.com/org/repo" // comment\n'
    bare = "module github.com/org/repo-bare // comment\n"

    assert gh._extract_package_name("GO", "go.mod", quoted, "org", "fallback") == "github.com/org/repo"
    assert gh._extract_package_name("GO", "go.mod", bare, "org", "fallback") == "github.com/org/repo-bare"


def test_extract_package_name_cargo_requires_package_section() -> None:
    """Cargo parser should skip manifests missing a [package] section."""

    manifest_text = "[workspace]\nmembers = ['a']\n"

    assert gh._extract_package_name("CARGO", "Cargo.toml", manifest_text, "org", "repo") is None


def test_system_from_manifest_path() -> None:
    """Manifest system resolution should classify known manifest names."""

    assert gh._system_from_manifest_path("a/b/Cargo.toml") == "CARGO"
    assert gh._system_from_manifest_path("a/b/package.json") == "NPM"
    assert gh._system_from_manifest_path("a/b/pubspec.yaml") == "DART"
    assert gh._system_from_manifest_path("a/b/unknown.txt") is None


def _fake_github_repo(full_name: str) -> SimpleNamespace:
    owner, name = full_name.split("/")
    del owner

    return SimpleNamespace(
        name=name,
        full_name=full_name,
        description=None,
        default_branch="main",
        stargazers_count=3,
        forks_count=1,
        pushed_at=dt.datetime(2026, 9, 20, 8, 0, tzinfo=dt.UTC),
        language=None,
        topics=[],
    )


def test_list_org_repos_stamps_observation_time_before_the_call_and_keeps_it_when_cached(monkeypatch: Any) -> None:
    """Every listed repo carries the time taken just before the listing call; cache hits keep it."""

    calls: list[dt.datetime] = []

    class _FakeUser:
        def get_repos(self, type: str) -> list[SimpleNamespace]:
            del type
            calls.append(dt.datetime.now(dt.UTC))

            return [_fake_github_repo("test-org/a"), _fake_github_repo("test-org/b")]

    class _FakeClient:
        def get_user(self, owner: str) -> _FakeUser:
            del owner

            return _FakeUser()

    monkeypatch.setattr(gh, "get_github_client", lambda: _FakeClient())
    monkeypatch.setattr(gh, "_gh_org_repos_cache", {})

    before = dt.datetime.now(dt.UTC)
    repos = gh.list_org_repos("test-org")

    assert len(calls) == 1
    assert [repo.observed_at for repo in repos] == [repos[0].observed_at] * 2
    observed_at = repos[0].observed_at
    assert observed_at is not None
    assert observed_at.utcoffset() == dt.timedelta(0)
    assert before <= observed_at <= calls[0]

    cached = gh.list_org_repos("test-org")

    assert len(calls) == 1
    assert [repo.observed_at for repo in cached] == [observed_at] * 2


def test_fetch_repo_list_stamps_each_repo_with_its_own_call_time(monkeypatch: Any) -> None:
    calls: dict[str, dt.datetime] = {}

    class _FakeClient:
        def get_repo(self, full_name: str) -> SimpleNamespace:
            calls[full_name] = dt.datetime.now(dt.UTC)

            return _fake_github_repo(full_name)

    monkeypatch.setattr(gh, "get_github_client", lambda: _FakeClient())

    before = dt.datetime.now(dt.UTC)
    repos = gh.fetch_repo_list(["https://github.com/test-org/a", "https://github.com/test-org/b/"])

    assert [repo.full_name for repo in repos] == ["test-org/a", "test-org/b"]
    first, second = repos[0].observed_at, repos[1].observed_at
    assert first is not None and second is not None
    assert before <= first <= calls["test-org/a"] <= second <= calls["test-org/b"]
