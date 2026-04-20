"""

Unit tests for ``pg_atlas.procrastinate.depsdev``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import grpc
import pytest
from grpclib.const import Status
from grpclib.exceptions import GRPCError

from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevPackageInfo,
    DepsDevProjectInfo,
    ProjectPackage,
    ProjectPackageVersion,
    _extract_requirements,
    _get_project_package_versions,
    get_package,
    get_project_batch,
    get_requirements,
    system_for_purl,
)


def _ns(**kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(**kwargs)


def test_system_for_purl() -> None:
    assert system_for_purl("pkg:pypi/stellar-sdk@1.0.0") == "PYPI"
    assert system_for_purl("pkg:cargo/serde") == "CARGO"
    assert system_for_purl("pkg:github/foo/bar") is None


def test_extract_requirements_runtime_only() -> None:
    reqs = _ns(
        pypi=None,
        npm=None,
        cargo=_ns(
            dependencies=[
                _ns(name="serde", requirement="^1.0", kind=""),
                _ns(name="tokio", requirement="^1.0", kind="dev"),
            ]
        ),
        go=None,
        maven=None,
        nuget=None,
        rubygems=None,
    )

    result = _extract_requirements("CARGO", reqs)  # pyright: ignore[reportArgumentType]

    assert len(result) == 1
    assert result[0].name == "serde"


async def test_get_package_success(mocker: Any) -> None:
    mock_pkg = _ns(
        package_key=_ns(name="stellar-sdk"),
        purl="pkg:pypi/stellar-sdk",
        versions=[
            _ns(
                version_key=_ns(version="11.1.0"),
                purl="pkg:pypi/stellar-sdk@11.1.0",
                published_at=None,
                is_default=True,
            )
        ],
    )
    mocker.patch("pg_atlas.procrastinate.depsdev._get_package_message", new=mocker.AsyncMock(return_value=mock_pkg))

    info = await get_package("PYPI", "stellar-sdk")

    assert isinstance(info, DepsDevPackageInfo)
    assert info.default_version == "11.1.0"


async def test_get_package_not_found(mocker: Any) -> None:
    class _NotFound(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.NOT_FOUND

    rpc_error = _NotFound()
    mocker.patch("pg_atlas.procrastinate.depsdev._get_package_message", new=mocker.AsyncMock(side_effect=rpc_error))

    with pytest.raises(DepsDevError, match="not found"):
        await get_package("PYPI", "missing")


async def test_get_requirements_success(mocker: Any) -> None:
    mock_reqs = _ns(
        pypi=_ns(dependencies=[_ns(project_name="requests", version_specifier=">=2.25")]),
        npm=None,
        cargo=None,
        go=None,
        maven=None,
        nuget=None,
        rubygems=None,
    )
    mocker.patch("pg_atlas.procrastinate.depsdev._get_requirements_message", new=mocker.AsyncMock(return_value=mock_reqs))

    reqs = await get_requirements("PYPI", "stellar-sdk", "11.1.0")

    assert len(reqs) == 1
    assert reqs[0].name == "requests"


async def test_get_project_batch_success(mocker: Any) -> None:
    resp = _ns(
        project=_ns(
            project_key=_ns(id="github.com/stellarcn/py-stellar-base"),
            stars_count=10,
            forks_count=2,
            license="Apache-2.0",
            description="sdk",
        )
    )
    mocker.patch("pg_atlas.procrastinate.depsdev._get_project_batch_page", new=mocker.AsyncMock(return_value=([resp], "")))

    result = await get_project_batch(["github.com/stellarcn/py-stellar-base"])

    info = result["github.com/stellarcn/py-stellar-base"]
    assert isinstance(info, DepsDevProjectInfo)


async def test_get_project_package_versions_success(mocker: Any) -> None:
    version_key = _ns(system=7, name="stellar-sdk", version="11.1.0")
    response = _ns(versions=[_ns(version_key=version_key)])
    mocker.patch("pg_atlas.procrastinate.depsdev._run_with_stub", new=mocker.AsyncMock(return_value=response))

    versions = await _get_project_package_versions("github.com/stellarcn/py-stellar-base", stub=None)

    assert isinstance(versions[0], ProjectPackageVersion)
    assert versions[0].system == "PYPI"
    assert versions[0].name == "stellar-sdk"


async def test_populate_packages_deduplicates_version_rows(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.depsdev._get_project_package_versions",
        new=mocker.AsyncMock(
            return_value=[
                ProjectPackageVersion(
                    system="PYPI",
                    name="stellar-sdk",
                    version="11.0.0",
                    purl="pkg:pypi/stellar-sdk@11.0.0",
                ),
                ProjectPackageVersion(
                    system="PYPI",
                    name="stellar-sdk",
                    version="11.1.0",
                    purl="pkg:pypi/stellar-sdk@11.1.0",
                ),
                ProjectPackageVersion(
                    system="NPM",
                    name="stellar-base",
                    version="2.0.0",
                    purl="pkg:npm/stellar-base@2.0.0",
                ),
            ]
        ),
    )

    info = DepsDevProjectInfo(
        project_id="github.com/org/repo",
        stars_count=10,
        forks_count=2,
        license="Apache-2.0",
        description="repo",
    )

    await info.populate_packages()

    assert info.packages == [
        ProjectPackage(system="NPM", name="stellar-base", purl="pkg:npm/stellar-base"),
        ProjectPackage(system="PYPI", name="stellar-sdk", purl="pkg:pypi/stellar-sdk"),
    ]


async def test_get_requirements_not_found_grpclib(mocker: Any) -> None:
    mocker.patch(
        "pg_atlas.procrastinate.depsdev._get_requirements_message",
        new=mocker.AsyncMock(side_effect=GRPCError(status=Status.NOT_FOUND, message="missing")),
    )

    reqs = await get_requirements("PYPI", "missing", "0")

    assert reqs == []
