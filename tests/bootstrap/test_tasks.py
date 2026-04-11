"""

Unit tests for ``pg_atlas.procrastinate.tasks``.

These tests avoid network/database I/O via pytest-native mocking
(``mocker`` and ``monkeypatch``).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from procrastinate.exceptions import AlreadyEnqueued

from pg_atlas.db_models.base import ActivityStatus, ProjectType
from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevPackageInfo,
    DepsDevProjectInfo,
    DepsDevRequirement,
    DepsDevVersionInfo,
    ProjectPackageVersion,
)
from pg_atlas.procrastinate.opengrants import ScfProject

try:
    from pg_atlas.procrastinate.tasks import (
        GitHubRepoMetadata,
        _defer_with_lock,
        _load_git_mapping,
        _purl_type_for_system,
        crawl_github_repo,
        crawl_package_deps,
        process_gitlog_batch,
        process_project,
        sync_opengrants,
    )
except ValueError:
    pytest.skip("PG_ATLAS_DATABASE_URL intentionally not set for CI tests", allow_module_level=True)


class _FakeConfiguredTask:
    def __init__(self, mocker: Any) -> None:
        self.defer_async = mocker.AsyncMock()


def test_purl_type_for_system() -> None:
    assert _purl_type_for_system("PYPI") == "pypi"
    assert _purl_type_for_system("GO") == "golang"
    assert _purl_type_for_system("unknown") is None


def test_load_git_mapping() -> None:
    mapping = _load_git_mapping()
    assert "daoip-5:scf:project:python_stellar_sdk" in mapping


async def test_defer_with_lock_handles_already_enqueued(mocker: Any) -> None:
    task = mocker.Mock()
    configured = _FakeConfiguredTask(mocker)
    configured.defer_async.side_effect = AlreadyEnqueued("Job cannot be enqueued")
    task.configure.return_value = configured

    ok = await _defer_with_lock(task, queueing_lock="PYPI:foo", system="PYPI", package_name="foo")

    assert ok is False


async def test_sync_opengrants_defers_each_project_with_mapping(mocker: Any) -> None:
    projects = [
        ScfProject(
            canonical_id="proj:no-code",
            display_name="No Code",
            activity_status=ActivityStatus.live,
            git_owner_url=None,
            git_repo_url=None,
            category="Developer Tooling",
        ),
        ScfProject(
            canonical_id="proj:with-code",
            display_name="With Code",
            activity_status=ActivityStatus.in_dev,
            git_owner_url="https://github.com/a",
            git_repo_url="https://github.com/a/b",
            category="Smart Contracts",
        ),
    ]

    mocker.patch("pg_atlas.procrastinate.tasks.fetch_scf_projects", new=mocker.AsyncMock(return_value=projects))
    mocker.patch(
        "pg_atlas.procrastinate.tasks._load_git_mapping",
        return_value={
            "proj:no-code": {
                "git_owner_url": "https://github.com/enriched",
                "git_repo_url": "https://github.com/enriched/repo",
            }
        },
    )
    defer_mock = mocker.patch.object(process_project, "defer_async", new=mocker.AsyncMock())

    await sync_opengrants()

    assert defer_mock.call_count == 2
    first_call: dict[str, Any] = defer_mock.call_args_list[0].kwargs
    assert first_call["git_owner_url"] == "https://github.com/enriched"
    assert first_call["category"] == "Developer Tooling"


async def test_process_gitlog_batch_calls_runtime(mocker: Any) -> None:
    runtime_mock = mocker.patch("pg_atlas.procrastinate.tasks.process_gitlog_repo_batch", new=mocker.AsyncMock())

    await process_gitlog_batch([11, 12, 13])

    runtime_mock.assert_awaited_once_with([11, 12, 13])


async def test_process_project_enriches_packages_from_depsdev(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks._list_org_repos",
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
                "github.com/org/repo": DepsDevProjectInfo(
                    project_id="github.com/org/repo",
                    stars_count=10,
                    forks_count=3,
                    license="Apache-2.0",
                    description="repo",
                    package_versions=[],
                )
            }
        ),
    )
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_project_package_versions",
        new=mocker.AsyncMock(
            return_value=[
                ProjectPackageVersion(
                    system="PYPI",
                    name="stellar-sdk",
                    version="11.1.0",
                    purl="pkg:pypi/stellar-sdk@11.1.0",
                )
            ]
        ),
    )
    upsert_project_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_project", new=mocker.AsyncMock(return_value=101))
    crawl_defer_mock = mocker.patch.object(crawl_github_repo, "defer_async", new=mocker.AsyncMock())

    await process_project(
        project_canonical_id="proj:1",
        display_name="Project 1",
        activity_status="live",
        git_owner_url="https://github.com/org",
        git_repo_url=None,
        project_metadata={"k": "v"},
        category="Developer Tooling",
    )

    assert upsert_project_mock.call_args.kwargs["project_type"] == ProjectType.public_good
    call_kwargs: dict[str, Any] = crawl_defer_mock.call_args.kwargs
    assert call_kwargs["packages"] == [
        {
            "system": "PYPI",
            "name": "stellar-sdk",
            "version": "11.1.0",
            "purl": "pkg:pypi/stellar-sdk@11.1.0",
        }
    ]


async def test_crawl_github_repo_defers_package_deps(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=DepsDevPackageInfo(
                system="PYPI",
                name="stellar-sdk",
                purl="pkg:pypi/stellar-sdk",
                default_version="11.1.0",
                versions=[
                    DepsDevVersionInfo(
                        version="11.1.0",
                        purl="pkg:pypi/stellar-sdk@11.1.0",
                        published_at=None,
                        is_default=True,
                    )
                ],
            )
        ),
    )
    mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks._defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="StellarCN",
        repo="py-stellar-base",
        project_id=1,
        packages=[{"system": "PYPI", "name": "stellar-sdk"}],
        adoption_stars=123,
        adoption_forks=44,
    )

    assert defer_mock.call_count == 1


async def test_crawl_github_repo_deduplicates_same_package(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=DepsDevPackageInfo(
                system="PYPI",
                name="stellar-sdk",
                purl="pkg:pypi/stellar-sdk",
                default_version="11.1.0",
                versions=[],
            )
        ),
    )
    upsert_repo_mock = mocker.patch("pg_atlas.procrastinate.tasks.upsert_repo", new=mocker.AsyncMock(return_value=10))
    absorb_mock = mocker.patch("pg_atlas.procrastinate.tasks.absorb_external_repo", new=mocker.AsyncMock(return_value=False))
    mocker.patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new=mocker.AsyncMock())
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks._defer_with_lock", new=mocker.AsyncMock(return_value=True))

    await crawl_github_repo(
        owner="StellarCN",
        repo="py-stellar-base",
        project_id=1,
        packages=[
            {"system": "PYPI", "name": "stellar-sdk", "version": "1.0.0"},
            {"system": "PYPI", "name": "stellar-sdk", "version": "1.1.0"},
        ],
        adoption_stars=123,
        adoption_forks=44,
    )

    # 1 upsert for github repo; per-package loop calls absorb, not upsert_repo.
    assert upsert_repo_mock.call_count == 1
    assert absorb_mock.call_count == 1
    assert defer_mock.call_count == 1


async def test_crawl_package_deps_uses_source_repo_canonical_id(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.tasks.get_package",
        new=mocker.AsyncMock(
            return_value=DepsDevPackageInfo(
                system="PYPI",
                name="stellar-sdk",
                purl="pkg:pypi/stellar-sdk",
                default_version="1.0.0",
                versions=[],
            )
        ),
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
            return_value=DepsDevPackageInfo(
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
    defer_mock = mocker.patch("pg_atlas.procrastinate.tasks._defer_with_lock", new=mocker.AsyncMock(return_value=True))

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
    list_repos_mock = mocker.patch("pg_atlas.procrastinate.tasks._list_org_repos")
    crawl_mock = mocker.patch.object(crawl_github_repo, "defer_async", new=mocker.AsyncMock())

    await process_project(
        project_canonical_id="proj:edu",
        display_name="Stellar Academy",
        activity_status="live",
        git_owner_url="https://github.com/stellar-academy",
        git_repo_url=None,
        project_metadata={"k": "v"},
        category="Education & Community",
    )

    upsert_mock.assert_called_once()
    assert upsert_mock.call_args.kwargs["category"] == "Education & Community"
    list_repos_mock.assert_not_called()
    crawl_mock.assert_not_called()
