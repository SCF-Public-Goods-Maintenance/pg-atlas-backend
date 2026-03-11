"""
Unit tests for ``pg_atlas.procrastinate.depsdev``.

Tests cover PURL → system mapping, the ``_extract_requirements`` parser for
every supported ecosystem, and the async wrapper functions (with mocked gRPC).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pg_atlas.procrastinate.depsdev import (
    DepsDevError,
    DepsDevPackageInfo,
    DepsDevProjectInfo,
    _extract_requirements,
    get_package,
    get_project_batch,
    get_requirements,
    system_for_purl,
)

# ===================================================================
# system_for_purl
# ===================================================================


class TestSystemForPurl:
    """Tests for ``system_for_purl``."""

    def test_pypi(self) -> None:
        assert system_for_purl("pkg:pypi/stellar-sdk@1.0.0") == "PYPI"

    def test_cargo(self) -> None:
        assert system_for_purl("pkg:cargo/serde") == "CARGO"

    def test_npm(self) -> None:
        assert system_for_purl("pkg:npm/%40scope/package") == "NPM"

    def test_maven(self) -> None:
        assert system_for_purl("pkg:maven/org.apache/commons") == "MAVEN"

    def test_golang(self) -> None:
        assert system_for_purl("pkg:golang/github.com/foo/bar") == "GO"

    def test_gem(self) -> None:
        assert system_for_purl("pkg:gem/rails") == "RUBYGEMS"

    def test_nuget(self) -> None:
        assert system_for_purl("pkg:nuget/Newtonsoft.Json") == "NUGET"

    def test_unsupported_returns_none(self) -> None:
        assert system_for_purl("pkg:github/foo/bar") is None

    def test_not_purl_returns_none(self) -> None:
        assert system_for_purl("not-a-purl") is None


# ===================================================================
# _extract_requirements — per ecosystem
# ===================================================================


def _ns(**kwargs: Any) -> SimpleNamespace:
    """Build a SimpleNamespace tree for fake protobuf messages."""

    return SimpleNamespace(**kwargs)


class TestExtractRequirementsPyPI:
    """PyPI requirement parsing."""

    def test_pypi_deps(self) -> None:
        reqs = _ns(
            pypi=_ns(
                dependencies=[
                    _ns(project_name="requests", version_specifier=">=2.25.0"),
                    _ns(project_name="pynacl", version_specifier=">=1.4.0"),
                ]
            ),
            npm=None,
            cargo=None,
            go=None,
            maven=None,
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("PYPI", reqs)

        assert len(result) == 2
        assert result[0].name == "requests"
        assert result[0].version_constraint == ">=2.25.0"
        assert result[0].system == "PYPI"


class TestExtractRequirementsNpm:
    """NPM requirement parsing."""

    def test_npm_deps(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=_ns(
                dependencies=_ns(
                    dependencies=[
                        _ns(name="lodash", requirement="^4.17.0"),
                        _ns(name="express", requirement="~4.18.0"),
                    ]
                )
            ),
            cargo=None,
            go=None,
            maven=None,
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("NPM", reqs)

        assert len(result) == 2
        assert result[0].name == "lodash"


class TestExtractRequirementsCargo:
    """Cargo requirement parsing (skips dev/build deps)."""

    def test_cargo_runtime_only(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=_ns(
                dependencies=[
                    _ns(name="serde", requirement="^1.0", kind=""),
                    _ns(name="tokio", requirement="^1.0", kind="dev"),
                    _ns(name="cc", requirement="^1.0", kind="build"),
                    _ns(name="log", requirement="^0.4", kind="normal"),
                ]
            ),
            go=None,
            maven=None,
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("CARGO", reqs)

        names = {r.name for r in result}
        assert "serde" in names
        assert "log" in names
        assert "tokio" not in names
        assert "cc" not in names


class TestExtractRequirementsGo:
    """Go requirement parsing."""

    def test_go_direct_deps(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=None,
            go=_ns(
                direct_dependencies=[
                    _ns(name="github.com/stellar/go", requirement="v2.0.0"),
                ]
            ),
            maven=None,
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("GO", reqs)

        assert len(result) == 1
        assert result[0].name == "github.com/stellar/go"


class TestExtractRequirementsMaven:
    """Maven requirement parsing (compile/runtime scope only)."""

    def test_maven_scope_filter(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=None,
            go=None,
            maven=_ns(
                dependencies=[
                    _ns(name="commons-lang3", version="3.14.0", scope="compile"),
                    _ns(name="junit", version="4.13", scope="test"),
                    _ns(name="guava", version="33.0", scope="runtime"),
                    _ns(name="slf4j-api", version="2.0", scope=""),
                ]
            ),
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("MAVEN", reqs)

        names = {r.name for r in result}
        assert "commons-lang3" in names
        assert "guava" in names
        assert "slf4j-api" in names
        assert "junit" not in names


class TestExtractRequirementsNuget:
    """NuGet requirement parsing (deduplication across groups)."""

    def test_nuget_deduplication(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=None,
            go=None,
            maven=None,
            nuget=_ns(
                dependency_groups=[
                    _ns(
                        dependencies=[
                            _ns(name="Newtonsoft.Json", requirement=">=13.0.0"),
                        ]
                    ),
                    _ns(
                        dependencies=[
                            _ns(name="Newtonsoft.Json", requirement=">=13.0.0"),
                            _ns(name="System.Text.Json", requirement=">=7.0"),
                        ]
                    ),
                ]
            ),
            rubygems=None,
        )

        result = _extract_requirements("NUGET", reqs)

        assert len(result) == 2
        names = [r.name for r in result]
        assert names.count("Newtonsoft.Json") == 1


class TestExtractRequirementsRubyGems:
    """RubyGems requirement parsing."""

    def test_rubygems_runtime(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=None,
            go=None,
            maven=None,
            nuget=None,
            rubygems=_ns(
                runtime_dependencies=[
                    _ns(name="activesupport", requirement=">= 5.0"),
                ]
            ),
        )

        result = _extract_requirements("RUBYGEMS", reqs)

        assert len(result) == 1
        assert result[0].name == "activesupport"


class TestExtractRequirementsUnknown:
    """Unknown system returns empty list."""

    def test_unknown_system(self) -> None:
        reqs = _ns(
            pypi=None,
            npm=None,
            cargo=None,
            go=None,
            maven=None,
            nuget=None,
            rubygems=None,
        )

        result = _extract_requirements("UNKNOWN", reqs)

        assert result == []


# ===================================================================
# Async wrappers (mocked gRPC)
# ===================================================================


class TestGetPackage:
    """Tests for ``get_package`` async wrapper."""

    async def test_success(self) -> None:
        """Basic successful GetPackage call."""
        mock_pkg = _ns(
            package_key=_ns(name="stellar-sdk"),
            purl="pkg:pypi/stellar-sdk",
            versions=[
                _ns(
                    version_key=_ns(version="11.1.0"),
                    purl="pkg:pypi/stellar-sdk@11.1.0",
                    published_at=None,
                    is_default=True,
                ),
            ],
        )

        with patch("pg_atlas.procrastinate.depsdev._sync_get_package", return_value=mock_pkg):
            info = await get_package("PYPI", "stellar-sdk")

        assert isinstance(info, DepsDevPackageInfo)
        assert info.system == "PYPI"
        assert info.name == "stellar-sdk"
        assert info.default_version == "11.1.0"
        assert len(info.versions) == 1

    async def test_not_found_raises(self) -> None:
        """NOT_FOUND status raises DepsDevError."""
        import grpc

        rpc_error = grpc.RpcError()
        rpc_error.code = MagicMock(return_value=grpc.StatusCode.NOT_FOUND)  # type: ignore[attr-defined]

        with patch("pg_atlas.procrastinate.depsdev._sync_get_package", side_effect=rpc_error):
            with pytest.raises(DepsDevError, match="not found"):
                await get_package("PYPI", "nonexistent")


class TestGetRequirements:
    """Tests for ``get_requirements`` async wrapper."""

    async def test_success(self) -> None:
        mock_reqs = _ns(
            pypi=_ns(
                dependencies=[
                    _ns(project_name="requests", version_specifier=">=2.25.0"),
                ]
            ),
            npm=None,
            cargo=None,
            go=None,
            maven=None,
            nuget=None,
            rubygems=None,
        )

        with patch("pg_atlas.procrastinate.depsdev._sync_get_requirements", return_value=mock_reqs):
            reqs = await get_requirements("PYPI", "stellar-sdk", "11.1.0")

        assert len(reqs) == 1
        assert reqs[0].name == "requests"

    async def test_not_found_returns_empty(self) -> None:
        """NOT_FOUND returns empty list instead of raising."""
        import grpc

        rpc_error = grpc.RpcError()
        rpc_error.code = MagicMock(return_value=grpc.StatusCode.NOT_FOUND)  # type: ignore[attr-defined]

        with patch("pg_atlas.procrastinate.depsdev._sync_get_requirements", side_effect=rpc_error):
            reqs = await get_requirements("PYPI", "stellar-sdk", "99.0.0")

        assert reqs == []


class TestGetProjectBatch:
    """Tests for ``get_project_batch`` async wrapper."""

    async def test_empty_input(self) -> None:
        result = await get_project_batch([])
        assert result == {}

    async def test_success(self) -> None:
        mock_resp = _ns(
            project=_ns(
                project_key=_ns(id="github.com/stellarcn/py-stellar-base"),
                stars_count=498,
                forks_count=178,
                license="Apache-2.0",
                description="Python SDK for Stellar",
            ),
        )
        batch_result = ([mock_resp], "")

        with patch("pg_atlas.procrastinate.depsdev._sync_get_project_batch", return_value=batch_result):
            result = await get_project_batch(["github.com/stellarcn/py-stellar-base"])

        assert "github.com/stellarcn/py-stellar-base" in result
        info = result["github.com/stellarcn/py-stellar-base"]
        assert isinstance(info, DepsDevProjectInfo)
        assert info.stars_count == 498
