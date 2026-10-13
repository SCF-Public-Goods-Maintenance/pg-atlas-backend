"""
Unit tests for ``pg_atlas.procrastinate.tasks``.

These tests avoid network/database I/O via pytest-native mocking
(``mocker`` and ``monkeypatch``).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any

import pytest
from procrastinate.exceptions import AlreadyEnqueued
from pytest_mock import MockerFixture

from pg_atlas.db_models.base import ActivityStatus, ProjectType, RedirectPredicate, Visibility
from pg_atlas.db_models.release import Release
from pg_atlas.db_models.repo_vertex import ExternalRepo, RedirectVertex, Repo
from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevRequirement,
    ProjectPackage,
)
from pg_atlas.procrastinate.opengrants import ScfProject

try:
    from pg_atlas.procrastinate.tasks import (
        GitHubRepoMetadata,
        PackageReference,
        _load_project_overrides,
        _purl_type_for_system,
        crawl_github_repo,
        crawl_package_deps,
        crawl_package_registry,
        defer_with_lock,
        demote_fork_repo_to_redirect,
        process_gitlog_batch,
        process_project,
        sync_opengrants,
    )
except ValueError:
    pytest.skip("PG_ATLAS_DATABASE_URL intentionally not set for CI tests", allow_module_level=True)


class _FakeConfiguredTask:
    def __init__(self, mocker: Any) -> None:
        self.defer_async = mocker.AsyncMock()


def _depsdev_package_info(
    default_version: str,
    versions: list[SimpleNamespace],
    *,
    system: str = "PYPI",
    name: str = "stellar-sdk",
    purl: str = "pkg:pypi/stellar-sdk",
) -> SimpleNamespace:
    """Build a minimal deps.dev package info stub for task tests."""

    return SimpleNamespace(
        system=system,
        name=name,
        purl=purl,
        default_version=default_version,
        versions=versions,
    )


def test_purl_type_for_system() -> None:
    assert _purl_type_for_system("PYPI") == "pypi"
    assert _purl_type_for_system("GO") == "golang"
    assert _purl_type_for_system("unknown") is None


def test_load_project_overrides() -> None:
    mapping = _load_project_overrides()
    assert "daoip-5:scf:project:stellar_sdf" in mapping
    assert "daoip-5:scf:project:kalepail" in mapping


async def test_defer_with_lock_handles_already_enqueued(mocker: Any) -> None:
    task = mocker.Mock()
    configured = _FakeConfiguredTask(mocker)
    configured.defer_async.side_effect = AlreadyEnqueued("Job cannot be enqueued")
    task.configure.return_value = configured

    ok = await defer_with_lock(task, queueing_lock="PYPI:foo", system="PYPI", package_name="foo")

    assert ok is False


async def test_sync_opengrants_defers_each_project_with_overrides(mocker: Any) -> None:
    projects = [
        ScfProject(
            canonical_id="proj:no-code",
            display_name="No Code",
            activity_status=ActivityStatus.live,
            git_owner_url=None,
            git_repo_urls=[],
            category="Developer Tooling",
        ),
        ScfProject(
            canonical_id="proj:with-code",
            display_name="With Code",
            activity_status=ActivityStatus.in_dev,
            git_owner_url="https://github.com/a",
            git_repo_urls=["https://github.com/a/b"],
            category="Smart Contracts",
        ),
    ]

    mocker.patch("pg_atlas.procrastinate.tasks.fetch_scf_projects", new=mocker.AsyncMock(return_value=projects))
    mocker.patch(
        "pg_atlas.procrastinate.tasks._load_project_overrides",
        return_value={
            "proj:to-be-inserted": {
                "display_name": "To be inserted",
                "activity_status": "non-responsive",
                "git_owner_url": "https://github.com/sloth",
            },
            "proj:no-code": {
                "category": "Visibility",
                "git_owner_url": "https://github.com/enriched",
                "git_repo_urls": [
                    "https://github.com/enriched/repo",
                    "https://github.com/upstream/opera",
                ],
            },
            "proj:with-code": {
                "activity_status": "live",
                "metadata": {"description": "Contracts launched on mainnet!"},
            },
        },
    )
    defer_mock = mocker.patch.object(process_project, "batch_defer_async", new=mocker.AsyncMock())

    await sync_opengrants()

    defer_mock.assert_awaited_once()
    defer_data: tuple[dict[str, Any], ...] = defer_mock.call_args_list[0].args
    assert len(defer_data) == 3
    assert defer_data[0]["git_owner_url"] == "https://github.com/enriched"
    assert defer_data[0]["git_repo_urls"] == ["https://github.com/enriched/repo", "https://github.com/upstream/opera"]
    assert defer_data[0]["category"] == "Visibility"
    assert defer_data[1]["activity_status"] == ActivityStatus.live
    assert defer_data[1]["git_repo_urls"] == ["https://github.com/a/b"]
    assert defer_data[1]["category"] == "Smart Contracts"
    assert defer_data[1]["project_metadata"]["description"] == "Contracts launched on mainnet!"
    assert defer_data[2]["canonical_id"] == "proj:to-be-inserted"
    assert defer_data[2]["display_name"] == "To be inserted"
    assert defer_data[2]["activity_status"] == "non-responsive"
    assert defer_data[2]["git_owner_url"] == "https://github.com/sloth"
    assert defer_data[2]["git_repo_urls"] == []


async def test_sync_opengrants_filters_projects_by_canonical_id(mocker: Any) -> None:
    projects = [
        ScfProject(
            canonical_id="daoip-5:scf:project:python_stellar_sdk",
            display_name="Python SDK",
            activity_status=ActivityStatus.live,
            git_owner_url="https://github.com/stellar",
            git_repo_urls=["https://github.com/stellar/python-stellar-sdk"],
            category="Developer Tooling",
        ),
        ScfProject(
            canonical_id="daoip-5:scf:project:stellarchain.io",
            display_name="StellarChain",
            activity_status=ActivityStatus.live,
            git_owner_url="https://github.com/stellarchain",
            git_repo_urls=["https://github.com/stellarchain/web"],
            category="Developer Tooling",
        ),
    ]

    mocker.patch("pg_atlas.procrastinate.tasks.fetch_scf_projects", new=mocker.AsyncMock(return_value=projects))
    mocker.patch("pg_atlas.procrastinate.tasks._load_project_overrides", return_value={})
    defer_mock = mocker.patch.object(process_project, "batch_defer_async", new=mocker.AsyncMock())

    await sync_opengrants(canonical_ids=["daoip-5:scf:project:python_stellar_sdk"])

    defer_mock.assert_awaited_once()
    defer_data: tuple[dict[str, Any], ...] = defer_mock.call_args_list[0].args
    assert len(defer_data) == 1
    assert defer_data[0]["canonical_id"] == "daoip-5:scf:project:python_stellar_sdk"


async def test_sync_opengrants_raises_for_unknown_canonical_ids(mocker: Any) -> None:
    projects = [
        ScfProject(
            canonical_id="daoip-5:scf:project:python_stellar_sdk",
            display_name="Python SDK",
            activity_status=ActivityStatus.live,
            git_owner_url="https://github.com/stellar",
            git_repo_urls=["https://github.com/stellar/python-stellar-sdk"],
            category="Developer Tooling",
        )
    ]
    overrides = {"daoip-5:scf:project:need_to_insert": dict[str, str]()}

    mocker.patch("pg_atlas.procrastinate.tasks.fetch_scf_projects", new=mocker.AsyncMock(return_value=projects))
    mocker.patch("pg_atlas.procrastinate.tasks._load_project_overrides", return_value=overrides)
    defer_mock = mocker.patch.object(process_project, "defer_async", new=mocker.AsyncMock())

    with pytest.raises(ValueError, match="not found") as exc_info:
        await sync_opengrants(
            canonical_ids=[
                "daoip-5:scf:project:python_stellar_sdk",
                "daoip-5:scf:project:missing_one",
                "daoip-5:scf:project:missing_two",
                "daoip-5:scf:project:need_to_insert",
            ]
        )

    assert "daoip-5:scf:project:missing_one" in str(exc_info.value)
    assert "daoip-5:scf:project:missing_two" in str(exc_info.value)
    assert "daoip-5:scf:project:python_stellar_sdk" not in str(exc_info.value)
    assert "daoip-5:scf:project:need_to_insert" not in str(exc_info.value)
    defer_mock.assert_not_awaited()


async def test_process_gitlog_batch_calls_runtime(mocker: Any) -> None:
    runtime_mock = mocker.patch("pg_atlas.procrastinate.tasks.process_gitlog_repo_batch", new=mocker.AsyncMock())

    await process_gitlog_batch([11, 12, 13])

    runtime_mock.assert_awaited_once_with([11, 12, 13], seed_run_ordinal=0)


async def test_process_project_enriches_packages_from_depsdev(mocker: Any) -> None:
    depsdev_info = SimpleNamespace(
        project_id="github.com/org/repo",
        stars_count=10,
        forks_count=3,
        license="Apache-2.0",
        description="repo",
        packages=[ProjectPackage(system="PYPI", name="stellar-sdk", purl="pkg:pypi/stellar-sdk")],
    )
    depsdev_info.populate_packages = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.tasks.list_org_repos",
        return_value=[
            GitHubRepoMetadata(
                name="repo",
                full_name="org/repo",
                description="",
                default_branch="main",
                stars=1,
                forks=1,
                pushed_at=dt.datetime(2015, 9, 30, 16, 46, 54, tzinfo=dt.UTC),
                language="",
                topics=[],
            )
        ],
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_project_batch",
        new=mocker.AsyncMock(
            return_value={
                "github.com/org/repo": depsdev_info,
            }
        ),
    )
    upsert_project_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())

    await process_project(
        canonical_id="proj:1",
        display_name="Project 1",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=[],
        project_metadata={"k": "v"},
        category="Developer Tooling",
    )

    assert upsert_project_mock.call_args.kwargs["project_type"] == ProjectType.public_good
    crawl_defer_mock.assert_awaited_once()
    defer_data: tuple[dict[str, Any], ...] = crawl_defer_mock.call_args_list[0].args
    assert len(defer_data) == 1
    assert defer_data[0]["packages"] == [
        {
            "system": "PYPI",
            "name": "stellar-sdk",
            "purl": "pkg:pypi/stellar-sdk",
        }
    ]


async def test_process_project_defers_forks_and_crawls_only_normal_repos(mocker: Any) -> None:
    """Forks bypass deps.dev and normal crawling and are queued for demotion."""
    fork = GitHubRepoMetadata(
        name="fork",
        full_name="org/fork",
        description="",
        default_branch="main",
        stars=1,
        forks=0,
        pushed_at=None,
        language="",
        topics=[],
        is_fork=True,
        parent_full_name="upstream/fork",
        source_full_name="upstream/fork",
    )
    normal = GitHubRepoMetadata(
        name="normal",
        full_name="org/normal",
        description="",
        default_branch="main",
        stars=2,
        forks=0,
        pushed_at=None,
        language="Python",
        topics=[],
    )
    mocker.patch("pg_atlas.procrastinate.tasks.list_org_repos", return_value=[fork, normal])
    get_project_batch_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.get_project_batch",
        new=mocker.AsyncMock(return_value={}),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())
    demote_task_mock = mocker.Mock()
    demote_task_mock.batch_defer_async = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_fork_repo_to_redirect",
        new=demote_task_mock,
        create=True,
    )

    await process_project(
        canonical_id="proj:fork-partition",
        display_name="Fork Partition",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=[],
        project_metadata=None,
    )

    requested_project_ids = get_project_batch_mock.await_args.args[0]
    assert requested_project_ids == ["github.com/org/normal"]
    crawl_data: tuple[dict[str, Any], ...] = crawl_defer_mock.call_args.args
    assert [item["repo"] for item in crawl_data] == ["normal"]
    demote_task_mock.batch_defer_async.assert_awaited_once_with({"github_repo_url": "https://github.com/org/fork"})


async def test_process_project_exempts_explicit_fork_urls_from_demotion(mocker: Any) -> None:
    """Explicit repository URLs remain crawl candidates even when GitHub marks them as forks."""
    explicit_fork = GitHubRepoMetadata(
        name="explicit-fork",
        full_name="org/explicit-fork",
        description="",
        default_branch="main",
        stars=1,
        forks=0,
        pushed_at=None,
        language="",
        topics=[],
        is_fork=True,
        parent_full_name="upstream/explicit-fork",
        source_full_name="upstream/explicit-fork",
    )
    unlisted_fork = GitHubRepoMetadata(
        name="unlisted-fork",
        full_name="org/unlisted-fork",
        description="",
        default_branch="main",
        stars=1,
        forks=0,
        pushed_at=None,
        language="",
        topics=[],
        is_fork=True,
        parent_full_name="upstream/unlisted-fork",
        source_full_name="upstream/unlisted-fork",
    )
    normal = GitHubRepoMetadata(
        name="normal",
        full_name="org/normal",
        description="",
        default_branch="main",
        stars=2,
        forks=0,
        pushed_at=None,
        language="Python",
        topics=[],
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.list_org_repos",
        return_value=[explicit_fork, unlisted_fork, normal],
    )
    get_project_batch_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.get_project_batch",
        new=mocker.AsyncMock(return_value={}),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())
    demote_task_mock = mocker.Mock()
    demote_task_mock.batch_defer_async = mocker.AsyncMock()
    mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_fork_repo_to_redirect",
        new=demote_task_mock,
        create=True,
    )

    await process_project(
        canonical_id="proj:explicit-fork-exemption",
        display_name="Explicit Fork Exemption",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=["https://github.com/org/explicit-fork"],
        project_metadata=None,
        extended_universe=True,
    )

    requested_project_ids = get_project_batch_mock.await_args.args[0]
    assert requested_project_ids == ["github.com/org/explicit-fork", "github.com/org/normal"]
    crawl_data: tuple[dict[str, Any], ...] = crawl_defer_mock.call_args.args
    assert [item["repo"] for item in crawl_data] == ["explicit-fork", "normal"]
    demote_task_mock.batch_defer_async.assert_awaited_once_with({"github_repo_url": "https://github.com/org/unlisted-fork"})


async def test_demote_fork_repo_to_redirect_prefers_source_repo(mocker: Any) -> None:
    """The root source Repo takes priority over a parent Repo or package vertex."""
    session = mocker.AsyncMock()
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)

    fork = Repo(
        canonical_id="pkg:github/org/fork",
        display_name="fork",
        visibility=Visibility.public,
        latest_version="1.0.0",
        releases=[Release(purl="pkg:pypi/fork-package", version="1.0.0")],
    )
    fork.id = 11
    source = Repo(
        canonical_id="pkg:github/root/source",
        display_name="source",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    source.id = 12
    get_vertex_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.get_vertex",
        new=mocker.AsyncMock(side_effect=[fork, source]),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_repo_fork_details",
        return_value=("org/parent", "root/source"),
    )
    upsert_external_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.upsert_external_repo",
        new=mocker.AsyncMock(),
    )
    demote_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_repo_to_redirect",
        new=mocker.AsyncMock(),
    )

    await demote_fork_repo_to_redirect("https://github.com/org/fork")

    assert [call.args[1] for call in get_vertex_mock.await_args_list] == [
        "pkg:github/org/fork",
        "pkg:github/root/source",
    ]
    upsert_external_mock.assert_not_awaited()
    demote_mock.assert_awaited_once_with(
        session,
        repo_id=11,
        target_vertex_id=12,
        predicate=RedirectPredicate.fork_of,
    )
    session.commit.assert_awaited_once()
    session.close.assert_awaited_once()


async def test_demote_fork_repo_to_redirect_falls_back_to_parent_repo(mocker: Any) -> None:
    """An existing parent Repo is used when no source Repo exists."""
    session = mocker.AsyncMock()
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)
    fork = Repo(
        canonical_id="pkg:github/org/fork",
        display_name="fork",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    fork.id = 21
    parent = Repo(
        canonical_id="pkg:github/org/parent",
        display_name="parent",
        visibility=Visibility.public,
        latest_version="1.0.0",
    )
    parent.id = 22
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_vertex",
        new=mocker.AsyncMock(side_effect=[fork, None, parent]),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_repo_fork_details",
        return_value=("org/parent", "root/source"),
    )
    demote_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_repo_to_redirect",
        new=mocker.AsyncMock(),
    )

    await demote_fork_repo_to_redirect("https://github.com/org/fork")

    demote_mock.assert_awaited_once_with(
        session,
        repo_id=21,
        target_vertex_id=22,
        predicate=RedirectPredicate.fork_of,
    )


@pytest.mark.parametrize("package_exists", [True, False], ids=["reuse-package", "create-package"])
async def test_demote_fork_repo_to_redirect_uses_release_package_fallback(mocker: Any, package_exists: bool) -> None:
    """A release package PURL reuses an ExternalRepo or creates one when absent."""
    session = mocker.AsyncMock()
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)
    fork = Repo(
        canonical_id="pkg:github/org/fork",
        display_name="fork",
        visibility=Visibility.public,
        latest_version="2.3.4",
        releases=[Release(purl="pkg:pypi/fork-package", version="2.3.4")],
    )
    fork.id = 31
    external_package = ExternalRepo(
        canonical_id="pkg:pypi/fork-package",
        display_name="fork package",
        latest_version="2.3.4",
    )
    external_package.id = 32
    if package_exists:
        get_vertex_results: list[Repo | ExternalRepo | None] = [fork, external_package]
    else:
        get_vertex_results = [fork, None]
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_vertex",
        new=mocker.AsyncMock(side_effect=get_vertex_results),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_repo_fork_details", return_value=(None, None))
    upsert_external_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.upsert_external_repo",
        new=mocker.AsyncMock(return_value=33),
    )
    demote_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_repo_to_redirect",
        new=mocker.AsyncMock(),
    )

    await demote_fork_repo_to_redirect("https://github.com/org/fork")

    if package_exists:
        upsert_external_mock.assert_not_awaited()
        target_vertex_id = 32
    else:
        upsert_external_mock.assert_awaited_once_with(
            session,
            canonical_id="pkg:pypi/fork-package",
            display_name="fork",
            latest_version="2.3.4",
        )
        target_vertex_id = 33
    demote_mock.assert_awaited_once_with(
        session,
        repo_id=31,
        target_vertex_id=target_vertex_id,
        predicate=RedirectPredicate.fork_of,
    )


@pytest.mark.parametrize("release_purl", [None, "pkg:github/upstream/repo"], ids=["no-package", "reserved-github-purl"])
async def test_demote_fork_repo_to_redirect_raises_without_a_valid_target(mocker: Any, release_purl: str | None) -> None:
    """Missing targets fail instead of creating an invalid GitHub ExternalRepo."""
    session = mocker.AsyncMock()
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)
    releases = [Release(purl=release_purl, version="1.0.0")] if release_purl is not None else []
    fork = Repo(
        canonical_id="pkg:github/org/fork",
        display_name="fork",
        visibility=Visibility.public,
        latest_version="1.0.0",
        releases=releases,
    )
    fork.id = 41
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_vertex",
        new=mocker.AsyncMock(return_value=fork),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_repo_fork_details", return_value=(None, None))
    upsert_external_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.upsert_external_repo",
        new=mocker.AsyncMock(),
    )
    demote_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_repo_to_redirect",
        new=mocker.AsyncMock(),
    )

    with pytest.raises(ValueError, match="Cannot resolve redirect target"):
        await demote_fork_repo_to_redirect("https://github.com/org/fork")

    upsert_external_mock.assert_not_awaited()
    demote_mock.assert_not_awaited()
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()


@pytest.mark.parametrize("redirected", [False, True], ids=["missing", "already-redirect"])
async def test_demote_fork_repo_to_redirect_returns_for_missing_or_redirect_vertex(mocker: Any, redirected: bool) -> None:
    """Missing GitHub vertices and vertices already redirected are harmless no-ops."""
    session = mocker.AsyncMock()
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)
    vertex = None
    if redirected:
        vertex = RedirectVertex(
            canonical_id="pkg:github/org/fork",
            target_vertex_id=99,
            predicate=RedirectPredicate.fork_of,
        )
    get_vertex_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.get_vertex",
        new=mocker.AsyncMock(return_value=vertex),
    )
    details_mock = mocker.patch("pg_atlas.procrastinate.tasks.get_repo_fork_details")
    demote_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.demote_repo_to_redirect",
        new=mocker.AsyncMock(),
    )

    await demote_fork_repo_to_redirect("https://github.com/org/fork")

    get_vertex_mock.assert_awaited_once()
    details_mock.assert_not_called()
    demote_mock.assert_not_awaited()
    session.commit.assert_not_awaited()
    session.close.assert_awaited_once()


async def test_process_project_associates_unavailable_repos(mocker: Any) -> None:
    """Known repos unavailable on GitHub should still be linked to the project."""

    mocker.patch(
        "pg_atlas.procrastinate.tasks.fetch_repo_list",
        return_value=([], ["org/unavailable"]),
    )
    upsert_project_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    associate_mock = mocker.patch("pg_atlas.procrastinate.tasks.associate_repos_with_project", new=mocker.AsyncMock())
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())

    await process_project(
        canonical_id="proj:unavailable-repo",
        display_name="Unavailable Repo Project",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=["https://github.com/org/unavailable"],
        project_metadata=None,
    )

    upsert_project_mock.assert_awaited_once()

    associate_mock.assert_awaited_once_with(["pkg:github/org/unavailable"], 101)
    crawl_defer_mock.assert_not_awaited()


async def test_process_project_cleans_stale_repos_in_restricted_crawl(mocker: Any) -> None:
    """Restricted crawls retain configured repos even when GitHub omits them."""

    mocker.patch("pg_atlas.procrastinate.tasks.fetch_repo_list", return_value=([], []))
    upsert_project_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    clear_mock = mocker.patch("pg_atlas.procrastinate.tasks.clear_project_repo_associations", new=mocker.AsyncMock())
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())

    await process_project(
        canonical_id="proj:restricted",
        display_name="Restricted Project",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=[
            "https://github.com/org/available",
            "https://github.com/org/unavailable",
            "https://github.com/org/transient-error",
        ],
        project_metadata=None,
    )

    upsert_project_mock.assert_awaited_once()
    clear_mock.assert_awaited_once_with(
        101,
        {
            "pkg:github/org/available",
            "pkg:github/org/unavailable",
            "pkg:github/org/transient-error",
        },
    )
    crawl_defer_mock.assert_not_awaited()


@pytest.mark.parametrize(
    ("extended_universe", "git_repo_urls"),
    [(True, ["https://github.com/org/one"]), (False, [])],
)
async def test_process_project_skips_cleanup_for_broad_crawls(
    mocker: Any,
    extended_universe: bool,
    git_repo_urls: list[str],
) -> None:
    """Full-owner crawls must not unlink repos outside a targeted URL list."""

    mocker.patch("pg_atlas.procrastinate.tasks.list_org_repos", return_value=[])
    fetch_mock = mocker.patch("pg_atlas.procrastinate.tasks.fetch_repo_list", return_value=([], []))
    upsert_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    clear_mock = mocker.patch("pg_atlas.procrastinate.tasks.clear_project_repo_associations", new=mocker.AsyncMock())
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "batch_defer_async", new=mocker.AsyncMock())

    await process_project(
        canonical_id="proj:broad",
        display_name="Broad Project",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_urls=git_repo_urls,
        project_metadata=None,
        extended_universe=extended_universe,
    )

    upsert_mock.assert_awaited_once()
    fetch_mock.assert_not_called()
    clear_mock.assert_not_awaited()
    crawl_defer_mock.assert_not_awaited()


async def test_crawl_github_repo_defers_package_deps(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=_depsdev_package_info(
                "11.1.0",
                versions=[
                    SimpleNamespace(
                        version="11.1.0",
                        purl="pkg:pypi/stellar-sdk@11.1.0",
                        published_at=None,
                    )
                ],
            )
        ),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="StellarCN",
        repo="py-stellar-base",
        project_id=1,
        packages=[{"system": "PYPI", "name": "stellar-sdk"}],
        adoption_stars=123,
        adoption_forks=44,
    )

    deps_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_deps]
    registry_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_registry]

    assert len(deps_defer_calls) == 1
    assert len(registry_defer_calls) == 1


async def test_crawl_github_repo_processes_all_depsdev_package_refs(mocker: Any) -> None:
    mocker.patch("pg_atlas.procrastinate.tasks.latest_version_from_repo", return_value="")
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(return_value=_depsdev_package_info("11.1.0", versions=[], purl="pkg:pypi/stellar-sdk")),
    )
    upsert_repo_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    absorb_mock = mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="StellarCN",
        repo="py-stellar-base",
        project_id=1,
        packages=[
            {"system": "PYPI", "name": "stellar-sdk", "purl": "pkg:pypi/stellar-sdk"},
            {"system": "PYPI", "name": "stellar-sdk", "purl": "pkg:pypi/stellar-sdk"},
        ],
        adoption_stars=123,
        adoption_forks=44,
    )

    assert upsert_repo_mock.call_count == 1
    assert [c.args[0] for c in absorb_mock.await_args_list] == ["pkg:pypi/stellar-sdk"]
    deps_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_deps]
    registry_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_registry]

    assert len(deps_defer_calls) == 2
    assert len(registry_defer_calls) == 1


@pytest.mark.parametrize(
    ("system", "name", "canonical_name", "purl", "expected_ids"),
    [
        (
            "CARGO",
            "stellar_xdr",
            "stellar-xdr",
            "pkg:cargo/stellar-xdr",
            ["pkg:cargo/stellar_xdr", "pkg:cargo/stellar-xdr"],
        ),
        (
            "NPM",
            "@stellar/stellar-sdk",
            "@stellar/stellar-sdk",
            "pkg:npm/%40stellar/stellar-sdk",
            ["pkg:npm/@stellar/stellar-sdk", "pkg:npm/%40stellar/stellar-sdk"],
        ),
        (
            "MAVEN",
            "network.lightsail:stellar-sdk",
            "network.lightsail:stellar-sdk",
            "pkg:maven/network.lightsail/stellar-sdk",
            ["pkg:maven/network.lightsail:stellar-sdk", "pkg:maven/network.lightsail/stellar-sdk"],
        ),
        ("PYPI", "stellar-sdk", "stellar-sdk", "pkg:pypi/stellar-sdk", ["pkg:pypi/stellar-sdk"]),
    ],
    ids=["cargo", "scoped-npm", "maven", "identical-pypi"],
)
async def test_crawl_github_repo_absorbs_name_and_depsdev_purl(
    mocker: MockerFixture,
    system: str,
    name: str,
    canonical_name: str,
    purl: str,
    expected_ids: list[str],
) -> None:
    """Absorb dependency-crawler and SBOM ids in order, deduplicating all package refs."""

    mocker.patch("pg_atlas.procrastinate.tasks.latest_version_from_repo", return_value="")
    get_package_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=_depsdev_package_info("1.0.0", versions=[], system=system, name=canonical_name, purl=purl)
        ),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    absorb_mock = mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="test-org",
        repo="test-repo",
        project_id=1,
        packages=[{"system": system, "name": name}, {"system": system, "name": name}],
        adoption_stars=0,
        adoption_forks=0,
    )

    assert [c.args[:2] for c in get_package_mock.await_args_list] == [(system, name), (system, name)]
    assert [c.args[0] for c in absorb_mock.await_args_list] == expected_ids
    assert all(c.args[1] == 10 for c in absorb_mock.await_args_list)


@pytest.mark.parametrize("package_error", [False, True], ids=["empty-purl", "depsdev-error"])
async def test_crawl_github_repo_absorbs_name_without_depsdev_purl(mocker: MockerFixture, package_error: bool) -> None:
    """Missing deps.dev purls leave the name-based absorption path available."""

    mocker.patch("pg_atlas.procrastinate.tasks.latest_version_from_repo", return_value="")
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=_depsdev_package_info("1.0.0", versions=[], system="CARGO", name="stellar-xdr", purl=""),
            side_effect=DepsDevError("not found") if package_error else None,
        ),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    absorb_mock = mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="test-org",
        repo="test-repo",
        project_id=1,
        packages=[{"system": "CARGO", "name": "stellar_xdr"}],
        adoption_stars=0,
        adoption_forks=0,
    )

    absorb_mock.assert_awaited_once_with("pkg:cargo/stellar_xdr", 10)


async def test_crawl_package_deps_uses_source_repo_canonical_id(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(return_value=_depsdev_package_info("1.0.0", versions=[])),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_requirements",
        new=mocker.AsyncMock(return_value=[DepsDevRequirement(system="PYPI", name="requests", version_constraint=">=2")]),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.find_repo_by_release_purl", new=mocker.AsyncMock(return_value=None))
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_external_repo", new=mocker.AsyncMock(return_value=200))
    edge_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_depends_on", new=mocker.AsyncMock())

    session = mocker.AsyncMock()
    execute_result = mocker.Mock()
    execute_result.one_or_none.return_value = (111,)
    session.execute = mocker.AsyncMock(return_value=execute_result)
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)

    await crawl_package_deps(
        system="PYPI",
        package_name="stellar-sdk",
        source_repo_canonical_id="pkg:github/StellarCN/py-stellar-base",
    )

    edge_kwargs = edge_mock.call_args.kwargs
    assert edge_kwargs["in_vertex_id"] == 111


async def test_crawl_package_deps_skips_not_found(mocker: Any) -> None:
    mocker.patch("pg_atlas.procrastinate.tasks.get_package", new=mocker.AsyncMock(side_effect=DepsDevError("not found")))

    await crawl_package_deps(
        system="PYPI",
        package_name="missing",
        source_repo_canonical_id="pkg:github/x/y",
    )


async def test_crawl_package_deps_skips_self_recursive_dep(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=SimpleNamespace(
                system="PYPI",
                name="py-evm",
                purl="pkg:pypi/py-evm",
                default_version="0.1.0",
                versions=[],
            )
        ),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_requirements",
        new=mocker.AsyncMock(return_value=[DepsDevRequirement(system="PYPI", name="py-evm", version_constraint=">=0")]),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.find_repo_by_release_purl",
        new=mocker.AsyncMock(return_value=(200, "pkg:github/ethereum/py-evm", 42)),
    )
    upsert_ext_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_external_repo", new=mocker.AsyncMock(return_value=200))
    edge_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_depends_on", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    session = mocker.AsyncMock()
    execute_result = mocker.Mock()
    execute_result.one_or_none.return_value = (111,)
    session.execute = mocker.AsyncMock(return_value=execute_result)
    session_factory = mocker.Mock(return_value=session)
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=session_factory)

    await crawl_package_deps(
        system="PYPI",
        package_name="py-evm",
        source_repo_canonical_id="pkg:pypi/py-evm",
    )

    upsert_ext_mock.assert_not_called()
    edge_mock.assert_not_called()
    defer_mock.assert_not_called()


async def test_process_project_edu_community_skips_crawl(mocker: Any) -> None:
    upsert_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=42))
    list_repos_mock = mocker.patch("pg_atlas.procrastinate.tasks.list_org_repos")
    crawl_mock = mocker.patch.object(crawl_github_repo, "defer_async", new=mocker.AsyncMock())

    await process_project(
        canonical_id="proj:edu",
        display_name="Stellar Academy",
        activity_status="live",
        git_owner_url="https://github.com/stellar-academy",
        git_repo_urls=[],
        project_metadata={"k": "v"},
        category="Education & Community",
    )

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args.kwargs["category"] == "Education & Community"
    list_repos_mock.assert_not_called()
    crawl_mock.assert_not_called()


async def test_crawl_github_repo_defers_and_runs_registry_crawl_for_flutter(mocker: Any) -> None:
    """Ensure Flutter package detection defers and executes DART registry crawl."""

    mocker.patch(
        "pg_atlas.procrastinate.tasks.detect_packages_from_repo",
        return_value=[PackageReference(system="DART", name="stellar_flutter_sdk")],
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_package", new=mocker.AsyncMock(side_effect=DepsDevError("missing")))
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=mocker.Mock())

    fake_result = mocker.Mock(packages_processed=1, errors=[])
    fake_crawler = mocker.Mock()
    fake_crawler.crawl_and_persist = mocker.AsyncMock(return_value=fake_result)
    build_crawler_mock = mocker.patch("pg_atlas.procrastinate.tasks.build_registry_crawler", return_value=fake_crawler)

    async def _defer_side_effect(task: Any, queueing_lock: str, **kwargs: Any) -> bool:
        if task is crawl_package_registry:
            await crawl_package_registry(**kwargs)

        return True

    defer_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.defer_with_lock",
        new=mocker.AsyncMock(side_effect=_defer_side_effect),
    )

    await crawl_github_repo(
        owner="Soneso",
        repo="stellar_flutter_sdk",
        project_id=1,
        packages=[],
        adoption_stars=123,
        adoption_forks=44,
    )

    registry_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_registry]
    assert len(registry_defer_calls) == 1
    assert registry_defer_calls[0].kwargs["system"] == "DART"
    assert registry_defer_calls[0].kwargs["package_names"] == ["stellar_flutter_sdk"]

    build_crawler_mock.assert_called_once()
    fake_crawler.crawl_and_persist.assert_awaited_once_with(package_names=["stellar_flutter_sdk"])


async def test_crawl_github_repo_defers_and_runs_registry_crawl_for_php(mocker: Any) -> None:
    """Ensure Composer package detection defers and executes COMPOSER registry crawl."""

    mocker.patch(
        "pg_atlas.procrastinate.tasks.detect_packages_from_repo",
        return_value=[PackageReference(system="COMPOSER", name="soneso/stellar-php-sdk")],
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_package", new=mocker.AsyncMock(side_effect=DepsDevError("missing")))
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    mocker.patch("pg_atlas.procrastinate.tasks.get_session_factory", return_value=mocker.Mock())

    fake_result = mocker.Mock(packages_processed=1, errors=[])
    fake_crawler = mocker.Mock()
    fake_crawler.crawl_and_persist = mocker.AsyncMock(return_value=fake_result)
    build_crawler_mock = mocker.patch("pg_atlas.procrastinate.tasks.build_registry_crawler", return_value=fake_crawler)

    async def _defer_side_effect(task: Any, queueing_lock: str, **kwargs: Any) -> bool:
        if task is crawl_package_registry:
            await crawl_package_registry(**kwargs)

        return True

    defer_mock = mocker.patch(
        "pg_atlas.procrastinate.tasks.defer_with_lock",
        new=mocker.AsyncMock(side_effect=_defer_side_effect),
    )

    await crawl_github_repo(
        owner="Soneso",
        repo="stellar-php-sdk",
        project_id=1,
        packages=[],
        adoption_stars=123,
        adoption_forks=44,
    )

    registry_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_registry]
    assert len(registry_defer_calls) == 1
    assert registry_defer_calls[0].kwargs["system"] == "COMPOSER"
    assert registry_defer_calls[0].kwargs["package_names"] == ["soneso/stellar-php-sdk"]

    build_crawler_mock.assert_called_once()
    fake_crawler.crawl_and_persist.assert_awaited_once_with(package_names=["soneso/stellar-php-sdk"])


@pytest.mark.parametrize(
    ("system", "package_name"),
    [
        ("NPM", "lodash"),
        ("CARGO", "serde"),
        ("PYPI", "requests"),
    ],
)
async def test_crawl_github_repo_supports_new_registry_systems_without_warning(
    mocker: Any,
    caplog: pytest.LogCaptureFixture,
    system: str,
    package_name: str,
) -> None:
    """Supported direct-registry systems should enqueue registry crawls without unsupported warnings."""

    mocker.patch(
        "pg_atlas.procrastinate.tasks.detect_packages_from_repo",
        return_value=[PackageReference(system=system, name=package_name)],
    )
    mocker.patch("pg_atlas.procrastinate.tasks.get_package", new=mocker.AsyncMock(side_effect=DepsDevError("missing")))
    mocker.patch("pg_atlas.procrastinate.tasks.latest_version_from_repo", return_value="")
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks.defer_with_lock", new=mocker.AsyncMock(return_value=True))

    with caplog.at_level("WARNING"):
        await crawl_github_repo(
            owner="test-org",
            repo="test-repo",
            project_id=1,
            packages=[],
            adoption_stars=123,
            adoption_forks=44,
        )

    assert "registry-crawl unsupported ecosystem" not in caplog.text

    registry_defer_calls = [call for call in defer_mock.call_args_list if call.args[0] is crawl_package_registry]
    assert len(registry_defer_calls) == 1
    assert registry_defer_calls[0].kwargs["system"] == system
    assert registry_defer_calls[0].kwargs["package_names"] == [package_name]


# ---------------------------------------------------------------------------
# Serialization regression — Procrastinate deferred payload shapes
# ---------------------------------------------------------------------------


class TestDeferredPayloadJsonSerializable:
    """
    Regression guard for Procrastinate queue kwargs serialization.

    Procrastinate serializes task kwargs to JSON via psycopg.  Any
    non-JSON-serializable value (e.g. an ``enum.Enum`` that does not
    inherit from ``str``) will raise a ``TypeError`` at enqueue time and
    silently drop the job.  These tests catch that class of bug without
    requiring a live database connection.
    """

    def _assert_json_round_trips(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Serialize *payload* to JSON and parse it back.

        Raises ``TypeError`` / ``ValueError`` on the first non-serializable
        value, which pytest surfaces as a test failure with a clear message.
        """
        import json

        serialized = json.dumps(payload)

        return dict(json.loads(serialized))  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # Shape 1: process_project  (regression — ActivityStatus was broken)
    # ------------------------------------------------------------------

    def test_process_project_payload_is_json_serializable(self) -> None:
        """
        Regression: ``ActivityStatus`` in ``asdict(ScfProject)`` must be a
        plain ``str`` so that Procrastinate can serialize it to JSON.

        Before ``ActivityStatus(str, enum.Enum)`` was introduced, this raised
        ``TypeError: Object of type ActivityStatus is not JSON serializable``.
        """
        from dataclasses import asdict

        from pg_atlas.db_models.base import ActivityStatus
        from pg_atlas.procrastinate.opengrants import ScfProject

        proj = ScfProject(
            canonical_id="scf:project:test-123",
            display_name="Test Project",
            activity_status=ActivityStatus.live,
            git_owner_url="https://github.com/test-org",
            git_repo_urls=["https://github.com/test-org/test-repo"],
            category="Infrastructure",
            project_metadata={"round": 25, "tags": ["defi"]},
        )
        payload = {**asdict(proj), "extended_universe": False}
        data = self._assert_json_round_trips(payload)

        # The value must survive the round-trip as a plain string.
        assert isinstance(
            data["activity_status"], str
        ), "activity_status must be a str after JSON round-trip; check that ActivityStatus inherits from str"
        assert data["activity_status"] == "live"

    def test_process_project_payload_all_activity_status_values(self) -> None:
        """Every ``ActivityStatus`` member must be JSON serializable."""
        from dataclasses import asdict

        from pg_atlas.db_models.base import ActivityStatus
        from pg_atlas.procrastinate.opengrants import ScfProject

        for status in ActivityStatus:
            proj = ScfProject(
                canonical_id=f"scf:project:{status.value}",
                display_name=f"Project {status.value}",
                activity_status=status,
                git_owner_url=None,
                git_repo_urls=[],
            )
            payload = {**asdict(proj), "extended_universe": False}
            data = self._assert_json_round_trips(payload)
            assert data["activity_status"] == status.value

    # ------------------------------------------------------------------
    # Shape 2: crawl_github_repo  (via build_repo_defer_data)
    # ------------------------------------------------------------------

    def test_build_repo_defer_data_payload_is_json_serializable(self) -> None:
        """
        ``build_repo_defer_data`` converts ``pushed_at`` datetime → isoformat
        string and ``ProjectPackage`` instances → plain dicts; the resulting
        payload must be JSON-serializable.
        """
        import datetime as dt

        from pg_atlas.procrastinate.depsdev import ProjectPackage
        from pg_atlas.procrastinate.github import GitHubRepoMetadata
        from pg_atlas.procrastinate.tasks import build_repo_defer_data

        pkg = ProjectPackage(system="PYPI", name="stellar-sdk", purl="pkg:pypi/stellar-sdk")
        repo_info = GitHubRepoMetadata(
            name="py-stellar-base",
            full_name="StellarCN/py-stellar-base",
            description="",
            default_branch="main",
            stars=999,
            forks=42,
            pushed_at=dt.datetime(2025, 6, 1, 12, 0, 0, tzinfo=dt.UTC),
            language="Python",
            topics=[],
        )
        # Minimal DepsDevProjectInfo stub — only the fields build_repo_defer_data reads.
        from types import SimpleNamespace

        depsdev_info = SimpleNamespace(packages=[pkg], stars_count=1000, forks_count=50)
        payload = build_repo_defer_data(
            repo_info,
            project_id=7,
            projects_info={"github.com/stellarcn/py-stellar-base": depsdev_info},  # pyright: ignore[reportArgumentType]
        )

        data = self._assert_json_round_trips(payload)
        assert data["owner"] == "StellarCN"
        assert data["repo"] == "py-stellar-base"
        assert isinstance(data["pushed_at_isodt"], str)
        assert data["packages"] == [{"system": "PYPI", "name": "stellar-sdk", "purl": "pkg:pypi/stellar-sdk"}]

    def test_build_repo_defer_data_payload_none_pushed_at(self) -> None:
        """``pushed_at=None`` must produce a JSON-serializable ``None`` value."""
        from pg_atlas.procrastinate.github import GitHubRepoMetadata
        from pg_atlas.procrastinate.tasks import build_repo_defer_data

        repo_info = GitHubRepoMetadata(
            name="repo",
            full_name="org/repo",
            description="",
            default_branch="main",
            stars=0,
            forks=0,
            pushed_at=None,
            language="",
            topics=[],
        )
        payload = build_repo_defer_data(repo_info, project_id=1, projects_info={})
        data = self._assert_json_round_trips(payload)
        assert data["pushed_at_isodt"] is None

    # ------------------------------------------------------------------
    # Shape 3: crawl_package_deps
    # ------------------------------------------------------------------

    def test_crawl_package_deps_payload_is_json_serializable(self) -> None:
        """Kwargs for ``crawl_package_deps`` are plain strings — confirm no regression."""
        payload: dict[str, Any] = {
            "system": "PYPI",
            "package_name": "stellar-sdk",
            "source_repo_canonical_id": "pkg:github/StellarCN/py-stellar-base",
        }
        data = self._assert_json_round_trips(payload)
        assert data["system"] == "PYPI"

    # ------------------------------------------------------------------
    # Shape 4: crawl_package_registry
    # ------------------------------------------------------------------

    def test_crawl_package_registry_payload_is_json_serializable(self) -> None:
        """``package_names`` is a sorted list of plain strings."""
        payload: dict[str, Any] = {
            "system": "NPM",
            "package_names": ["@stellar/stellar-sdk", "bignumber.js"],
        }
        data = self._assert_json_round_trips(payload)
        assert data["package_names"] == ["@stellar/stellar-sdk", "bignumber.js"]

    # ------------------------------------------------------------------
    # Shape 5: process_sbom_submission
    # ------------------------------------------------------------------

    def test_process_sbom_submission_payload_is_json_serializable(self) -> None:
        """
        ``expected_status`` is stored as ``SubmissionStatus.value`` (a plain
        string) before being passed to ``defer_with_lock`` — confirm this
        serializes correctly.
        """
        from pg_atlas.db_models.base import SubmissionStatus

        for status in SubmissionStatus:
            payload: dict[str, Any] = {
                "submission_id": 42,
                "expected_status": status.value,
            }
            data = self._assert_json_round_trips(payload)
            assert isinstance(data["expected_status"], str)
            assert data["expected_status"] == status.value
