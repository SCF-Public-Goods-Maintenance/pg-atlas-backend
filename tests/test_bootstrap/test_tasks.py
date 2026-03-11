"""
Unit tests for ``pg_atlas.procrastinate.tasks``.

All external I/O (databases, HTTP, GitHub API, deps.dev gRPC) is mocked so
these tests run without network or database access.  The tests verify:
- Task fan-out logic (correct child tasks deferred)
- Git-mapping enrichment
- Package detection heuristics
- PURL type mapping
- Correct arguments passed to upserts

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from pg_atlas.db_models.base import ActivityStatus, ProjectType
from pg_atlas.procrastinate.depsdev import DepsDevError, DepsDevPackageInfo, DepsDevProjectInfo, DepsDevRequirement
from pg_atlas.procrastinate.opengrants import ScfProject
from pg_atlas.procrastinate.tasks import (
    _load_git_mapping,
    _purl_type_for_system,
    crawl_github_repo,
    crawl_package_deps,
    process_project,
    sync_opengrants,
)

# ===================================================================
# _purl_type_for_system
# ===================================================================


class TestPurlTypeForSystem:
    """Tests for ``_purl_type_for_system``."""

    def test_pypi(self) -> None:
        assert _purl_type_for_system("PYPI") == "pypi"

    def test_npm(self) -> None:
        assert _purl_type_for_system("NPM") == "npm"

    def test_cargo(self) -> None:
        assert _purl_type_for_system("CARGO") == "cargo"

    def test_go(self) -> None:
        assert _purl_type_for_system("GO") == "golang"

    def test_case_insensitive(self) -> None:
        assert _purl_type_for_system("pypi") == "pypi"

    def test_unknown_returns_none(self) -> None:
        assert _purl_type_for_system("UNKNOWN") is None


# ===================================================================
# _load_git_mapping
# ===================================================================


class TestLoadGitMapping:
    """Tests for ``_load_git_mapping``."""

    def test_loads_mapping_file(self) -> None:
        mapping = _load_git_mapping()

        assert "daoip-5:scf:project:python_stellar_sdk" in mapping
        entry = mapping["daoip-5:scf:project:python_stellar_sdk"]
        assert entry["git_org_url"] == "https://github.com/StellarCN"
        assert entry["git_repo_url"] == "https://github.com/StellarCN/py-stellar-base"


# ===================================================================
# sync_opengrants
# ===================================================================


class TestSyncOpengrants:
    """Tests for the ``sync_opengrants`` task."""

    @patch("pg_atlas.procrastinate.tasks.fetch_scf_projects")
    @patch("pg_atlas.procrastinate.tasks.process_project")
    @patch("pg_atlas.procrastinate.tasks._load_git_mapping")
    async def test_defers_process_project_per_project(
        self,
        mock_mapping: MagicMock,
        mock_process_project: MagicMock,
        mock_fetch: AsyncMock,
    ) -> None:
        """One ``process_project`` task is deferred per unique project."""
        mock_mapping.return_value = {}
        mock_fetch.return_value = [
            ScfProject(
                canonical_id="proj:a",
                display_name="Project A",
                activity_status=ActivityStatus.live,
                git_org_url="https://github.com/org-a",
                git_repo_url="https://github.com/org-a/repo-a",
            ),
            ScfProject(
                canonical_id="proj:b",
                display_name="Project B",
                activity_status=ActivityStatus.in_dev,
                git_org_url=None,
                git_repo_url=None,
            ),
        ]

        mock_process_project.defer_async = AsyncMock()

        await sync_opengrants()

        assert mock_process_project.defer_async.call_count == 2

    @patch("pg_atlas.procrastinate.tasks.fetch_scf_projects")
    @patch("pg_atlas.procrastinate.tasks.process_project")
    @patch("pg_atlas.procrastinate.tasks._load_git_mapping")
    async def test_enriches_from_git_mapping(
        self,
        mock_mapping: MagicMock,
        mock_process_project: MagicMock,
        mock_fetch: AsyncMock,
    ) -> None:
        """Projects missing io.scf.code are enriched from the mapping file."""
        mock_mapping.return_value = {
            "proj:no-code": {
                "git_org_url": "https://github.com/enriched-org",
                "git_repo_url": "https://github.com/enriched-org/repo",
            }
        }
        mock_fetch.return_value = [
            ScfProject(
                canonical_id="proj:no-code",
                display_name="No Code Project",
                activity_status=ActivityStatus.live,
                git_org_url=None,
                git_repo_url=None,
            ),
        ]

        mock_process_project.defer_async = AsyncMock()

        await sync_opengrants()

        call_kwargs = mock_process_project.defer_async.call_args[1]
        assert call_kwargs["git_org_url"] == "https://github.com/enriched-org"
        assert call_kwargs["git_repo_url"] == "https://github.com/enriched-org/repo"

    @patch("pg_atlas.procrastinate.tasks.fetch_scf_projects")
    @patch("pg_atlas.procrastinate.tasks.process_project")
    @patch("pg_atlas.procrastinate.tasks._load_git_mapping")
    async def test_no_enrichment_when_code_present(
        self,
        mock_mapping: MagicMock,
        mock_process_project: MagicMock,
        mock_fetch: AsyncMock,
    ) -> None:
        """Projects that already have git_org_url are not overridden by mapping."""
        mock_mapping.return_value = {
            "proj:has-code": {
                "git_org_url": "https://github.com/mapping-org",
                "git_repo_url": "https://github.com/mapping-org/repo",
            }
        }
        mock_fetch.return_value = [
            ScfProject(
                canonical_id="proj:has-code",
                display_name="Has Code",
                activity_status=ActivityStatus.live,
                git_org_url="https://github.com/original-org",
                git_repo_url="https://github.com/original-org/repo",
            ),
        ]

        mock_process_project.defer_async = AsyncMock()

        await sync_opengrants()

        call_kwargs = mock_process_project.defer_async.call_args[1]
        assert call_kwargs["git_org_url"] == "https://github.com/original-org"


# ===================================================================
# process_project
# ===================================================================


class TestProcessProject:
    """Tests for the ``process_project`` task."""

    @patch("pg_atlas.procrastinate.tasks.crawl_github_repo")
    @patch("pg_atlas.procrastinate.tasks.upsert_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_project_batch", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._list_org_repos")
    async def test_defers_crawl_for_each_org_repo(
        self,
        mock_list_repos: MagicMock,
        mock_get_batch: AsyncMock,
        mock_upsert: AsyncMock,
        mock_crawl: MagicMock,
    ) -> None:
        """Defers one crawl_github_repo per org repo."""
        mock_list_repos.return_value = [
            {"name": "repo-a", "full_name": "org/repo-a", "stars": 10, "forks": 2},
            {"name": "repo-b", "full_name": "org/repo-b", "stars": 5, "forks": 1},
        ]
        mock_get_batch.return_value = {}
        mock_upsert.return_value = 42
        mock_crawl.defer_async = AsyncMock()

        await process_project(
            project_canonical_id="proj:test",
            display_name="Test Project",
            activity_status="live",
            git_org_url="https://github.com/org",
            git_repo_url=None,
            project_metadata={"scf_category": "Developer Tooling"},
        )

        assert mock_crawl.defer_async.call_count == 2
        mock_upsert.assert_called_once()

    @patch("pg_atlas.procrastinate.tasks.crawl_github_repo")
    @patch("pg_atlas.procrastinate.tasks.upsert_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_project_batch", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._list_org_repos")
    async def test_single_repo_url_without_org_listing(
        self,
        mock_list_repos: MagicMock,
        mock_get_batch: AsyncMock,
        mock_upsert: AsyncMock,
        mock_crawl: MagicMock,
    ) -> None:
        """Falls back to single repo when org listing is empty."""
        mock_list_repos.return_value = []
        mock_get_batch.return_value = {}
        mock_upsert.return_value = 42
        mock_crawl.defer_async = AsyncMock()

        await process_project(
            project_canonical_id="proj:single",
            display_name="Single Repo",
            activity_status="live",
            git_org_url="https://github.com/owner",
            git_repo_url="https://github.com/owner/my-repo",
            project_metadata=None,
        )

        assert mock_crawl.defer_async.call_count == 1
        call_kwargs = mock_crawl.defer_async.call_args[1]
        assert call_kwargs["owner"] == "owner"
        assert call_kwargs["repo"] == "my-repo"

    @patch("pg_atlas.procrastinate.tasks.crawl_github_repo")
    @patch("pg_atlas.procrastinate.tasks.upsert_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_project_batch", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._list_org_repos")
    async def test_no_github_url_skips_crawl(
        self,
        mock_list_repos: MagicMock,
        mock_get_batch: AsyncMock,
        mock_upsert: AsyncMock,
        mock_crawl: MagicMock,
    ) -> None:
        """No GitHub URL means no crawl tasks are deferred."""
        mock_upsert.return_value = 42
        mock_crawl.defer_async = AsyncMock()

        await process_project(
            project_canonical_id="proj:no-git",
            display_name="No Git",
            activity_status="live",
            git_org_url=None,
            git_repo_url=None,
            project_metadata=None,
        )

        mock_crawl.defer_async.assert_not_called()
        mock_list_repos.assert_not_called()

    @patch("pg_atlas.procrastinate.tasks.crawl_github_repo")
    @patch("pg_atlas.procrastinate.tasks.upsert_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_project_batch", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._list_org_repos")
    async def test_depsdev_info_merged_into_crawl(
        self,
        mock_list_repos: MagicMock,
        mock_get_batch: AsyncMock,
        mock_upsert: AsyncMock,
        mock_crawl: MagicMock,
    ) -> None:
        """deps.dev project info (stars, forks, packages) is passed to crawl."""
        mock_list_repos.return_value = [
            {"name": "repo", "full_name": "org/repo", "stars": 10, "forks": 2},
        ]
        mock_get_batch.return_value = {
            "github.com/org/repo": DepsDevProjectInfo(
                project_id="github.com/org/repo",
                stars_count=500,
                forks_count=100,
                license="Apache-2.0",
                description="A project",
                package_versions=[{"system": "PYPI", "name": "my-pkg"}],
            ),
        }
        mock_upsert.return_value = 42
        mock_crawl.defer_async = AsyncMock()

        await process_project(
            project_canonical_id="proj:enriched",
            display_name="Enriched",
            activity_status="live",
            git_org_url="https://github.com/org",
            git_repo_url=None,
            project_metadata=None,
        )

        call_kwargs = mock_crawl.defer_async.call_args[1]
        assert call_kwargs["adoption_stars"] == 500
        assert call_kwargs["adoption_forks"] == 100
        assert call_kwargs["packages"] == [{"system": "PYPI", "name": "my-pkg"}]

    @patch("pg_atlas.procrastinate.tasks.crawl_github_repo")
    @patch("pg_atlas.procrastinate.tasks.upsert_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_project_batch", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._list_org_repos")
    async def test_project_type_public_good_when_packages_found(
        self,
        mock_list_repos: MagicMock,
        mock_get_batch: AsyncMock,
        mock_upsert: AsyncMock,
        mock_crawl: MagicMock,
    ) -> None:
        """Project type is ``public_good`` when deps.dev finds published packages."""
        mock_list_repos.return_value = [
            {"name": "r", "full_name": "o/r", "stars": 1, "forks": 0},
        ]
        mock_get_batch.return_value = {
            "github.com/o/r": DepsDevProjectInfo(
                project_id="github.com/o/r",
                stars_count=1,
                forks_count=0,
                license="",
                description="",
                package_versions=[{"system": "PYPI", "name": "pkg"}],
            ),
        }
        mock_upsert.return_value = 1
        mock_crawl.defer_async = AsyncMock()

        await process_project(
            project_canonical_id="proj:pg",
            display_name="PG",
            activity_status="live",
            git_org_url="https://github.com/o",
            git_repo_url=None,
            project_metadata=None,
        )

        _, kwargs = mock_upsert.call_args
        assert kwargs["project_type"] == ProjectType.public_good


# ===================================================================
# crawl_github_repo
# ===================================================================


class TestCrawlGithubRepo:
    """Tests for the ``crawl_github_repo`` task."""

    @patch("pg_atlas.procrastinate.tasks.crawl_package_deps")
    @patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_defers_crawl_package_deps_per_package(
        self,
        mock_get_pkg: AsyncMock,
        mock_upsert_repo: AsyncMock,
        mock_associate: AsyncMock,
        mock_crawl_deps: MagicMock,
    ) -> None:
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="PYPI",
            name="stellar-sdk",
            purl="pkg:pypi/stellar-sdk",
            default_version="11.1.0",
            versions=[{"version": "11.1.0", "purl": "pkg:pypi/stellar-sdk@11.1.0", "is_default": True}],
        )
        mock_upsert_repo.return_value = 101
        mock_crawl_deps_configure = MagicMock()
        mock_crawl_deps_configure.defer_async = AsyncMock()
        mock_crawl_deps.configure = MagicMock(return_value=mock_crawl_deps_configure)

        await crawl_github_repo(
            owner="StellarCN",
            repo="py-stellar-base",
            project_id=42,
            packages=[{"system": "PYPI", "name": "stellar-sdk"}],
            adoption_stars=498,
            adoption_forks=178,
        )

        # Should upsert repo twice: once for pkg:github/…, once for pkg:pypi/stellar-sdk
        assert mock_upsert_repo.call_count == 2
        mock_crawl_deps.configure.assert_called_once()
        mock_crawl_deps_configure.defer_async.assert_called_once()

    @patch("pg_atlas.procrastinate.tasks.crawl_package_deps")
    @patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks._detect_packages_from_repo")
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_detects_packages_when_empty(
        self,
        mock_get_pkg: AsyncMock,
        mock_detect: MagicMock,
        mock_upsert_repo: AsyncMock,
        mock_associate: AsyncMock,
        mock_crawl_deps: MagicMock,
    ) -> None:
        """When packages is empty, _detect_packages_from_repo is called."""
        mock_detect.return_value = [{"system": "CARGO", "name": "soroban-sdk"}]
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="CARGO",
            name="soroban-sdk",
            purl="pkg:cargo/soroban-sdk",
            default_version="1.0.0",
            versions=[],
        )
        mock_upsert_repo.return_value = 200
        mock_crawl_deps_configure = MagicMock()
        mock_crawl_deps_configure.defer_async = AsyncMock()
        mock_crawl_deps.configure = MagicMock(return_value=mock_crawl_deps_configure)

        await crawl_github_repo(
            owner="stellar",
            repo="soroban-sdk",
            project_id=10,
            packages=[],
            adoption_stars=50,
            adoption_forks=10,
        )

        mock_detect.assert_called_once_with("stellar", "soroban-sdk")

    @patch("pg_atlas.procrastinate.tasks.crawl_package_deps")
    @patch("pg_atlas.procrastinate.tasks.associate_repo_with_project", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_depsdev_not_found_falls_back_to_git_version(
        self,
        mock_get_pkg: AsyncMock,
        mock_upsert_repo: AsyncMock,
        mock_associate: AsyncMock,
        mock_crawl_deps: MagicMock,
    ) -> None:
        """When deps.dev doesn't know the package, fall back to git tag version."""
        mock_get_pkg.side_effect = DepsDevError("not found")
        mock_upsert_repo.return_value = 300
        mock_crawl_deps_configure = MagicMock()
        mock_crawl_deps_configure.defer_async = AsyncMock()
        mock_crawl_deps.configure = MagicMock(return_value=mock_crawl_deps_configure)

        with patch("pg_atlas.procrastinate.tasks._latest_version_from_repo", return_value="v3.0.1"):
            await crawl_github_repo(
                owner="org",
                repo="niche-lib",
                project_id=5,
                packages=[{"system": "PYPI", "name": "niche-lib"}],
                adoption_stars=1,
                adoption_forks=0,
            )

        # upsert_repo called with version from git
        first_call_kwargs = mock_upsert_repo.call_args_list[0][1]
        assert first_call_kwargs["latest_version"] == "v3.0.1"


# ===================================================================
# crawl_package_deps
# ===================================================================


class TestCrawlPackageDeps:
    """Tests for the ``crawl_package_deps`` task."""

    @patch("pg_atlas.db_models.session.get_session_factory")
    @patch("pg_atlas.procrastinate.tasks.upsert_depends_on", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_external_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.is_project_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_requirements", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_creates_external_repos_for_deps(
        self,
        mock_get_pkg: AsyncMock,
        mock_get_reqs: AsyncMock,
        mock_is_project: AsyncMock,
        mock_upsert_repo: AsyncMock,
        mock_upsert_ext: AsyncMock,
        mock_upsert_edge: AsyncMock,
        mock_session_factory: MagicMock,
    ) -> None:
        """Non-project dependencies become ExternalRepo vertices."""
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="PYPI",
            name="stellar-sdk",
            purl="pkg:pypi/stellar-sdk",
            default_version="11.1.0",
            versions=[],
        )
        mock_get_reqs.return_value = [
            DepsDevRequirement(system="PYPI", name="requests", version_constraint=">=2.25.0"),
            DepsDevRequirement(system="PYPI", name="pynacl", version_constraint=">=1.4.0"),
        ]
        mock_is_project.return_value = False
        mock_upsert_ext.return_value = 500

        # Mock session to return source vertex ID.
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.one_or_none.return_value = (100,)
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_factory = MagicMock(return_value=mock_session)
        mock_session_factory.return_value = mock_factory

        await crawl_package_deps(
            system="PYPI",
            package_name="stellar-sdk",
            source_repo_canonical_id="pkg:github/StellarCN/py-stellar-base",
        )

        assert mock_upsert_ext.call_count == 2
        assert mock_upsert_edge.call_count == 2
        mock_upsert_repo.assert_not_called()

    @patch("pg_atlas.db_models.session.get_session_factory")
    @patch("pg_atlas.procrastinate.tasks.upsert_depends_on", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_external_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.upsert_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.is_project_repo", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_requirements", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_recurses_for_project_repos(
        self,
        mock_get_pkg: AsyncMock,
        mock_get_reqs: AsyncMock,
        mock_is_project: AsyncMock,
        mock_upsert_repo: AsyncMock,
        mock_upsert_ext: AsyncMock,
        mock_upsert_edge: AsyncMock,
        mock_session_factory: MagicMock,
    ) -> None:
        """Dependencies that are project repos trigger recursion via defer_async."""
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="PYPI",
            name="stellar-sdk",
            purl="pkg:pypi/stellar-sdk",
            default_version="11.1.0",
            versions=[],
        )
        mock_get_reqs.return_value = [
            DepsDevRequirement(system="PYPI", name="stellar-sdk-xdr", version_constraint=">=0.2.0"),
        ]
        mock_is_project.return_value = True
        mock_upsert_repo.return_value = 200

        # Mock session to return source vertex ID.
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.one_or_none.return_value = (100,)
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_factory = MagicMock(return_value=mock_session)
        mock_session_factory.return_value = mock_factory

        # Mock the recursive defer call on the task object.
        mock_configure = MagicMock()
        mock_configure.defer_async = AsyncMock()
        with patch.object(crawl_package_deps, "configure", return_value=mock_configure):
            await crawl_package_deps(
                system="PYPI",
                package_name="stellar-sdk",
                source_repo_canonical_id="pkg:github/StellarCN/py-stellar-base",
            )

        mock_upsert_repo.assert_called_once()
        mock_configure.defer_async.assert_called_once()
        mock_upsert_ext.assert_not_called()

    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_skips_when_package_not_found(
        self,
        mock_get_pkg: AsyncMock,
    ) -> None:
        """When GetPackage returns NOT_FOUND, exits gracefully."""
        mock_get_pkg.side_effect = DepsDevError("not found")

        # Should not raise.
        await crawl_package_deps(
            system="PYPI",
            package_name="nonexistent",
            source_repo_canonical_id="pkg:github/x/y",
        )

    @patch("pg_atlas.procrastinate.tasks.get_requirements", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_skips_when_no_default_version(
        self,
        mock_get_pkg: AsyncMock,
        mock_get_reqs: AsyncMock,
    ) -> None:
        """When default_version is empty, exits without calling GetRequirements."""
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="PYPI",
            name="edge-case",
            purl="",
            default_version="",
            versions=[],
        )

        await crawl_package_deps(
            system="PYPI",
            package_name="edge-case",
            source_repo_canonical_id="pkg:github/x/y",
        )

        mock_get_reqs.assert_not_called()

    @patch("pg_atlas.db_models.session.get_session_factory")
    @patch("pg_atlas.procrastinate.tasks.get_requirements", new_callable=AsyncMock)
    @patch("pg_atlas.procrastinate.tasks.get_package", new_callable=AsyncMock)
    async def test_skips_when_source_vertex_not_found(
        self,
        mock_get_pkg: AsyncMock,
        mock_get_reqs: AsyncMock,
        mock_session_factory: MagicMock,
    ) -> None:
        """When the source vertex doesn't exist in DB, exits gracefully."""
        mock_get_pkg.return_value = DepsDevPackageInfo(
            system="PYPI",
            name="pkg",
            purl="",
            default_version="1.0",
            versions=[],
        )
        mock_get_reqs.return_value = [
            DepsDevRequirement(system="PYPI", name="dep", version_constraint="*"),
        ]

        # Mock session returning None for vertex lookup.
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_factory = MagicMock(return_value=mock_session)
        mock_session_factory.return_value = mock_factory

        await crawl_package_deps(
            system="PYPI",
            package_name="pkg",
            source_repo_canonical_id="pkg:github/missing/repo",
        )

        # Should have exited without attempting upserts.
