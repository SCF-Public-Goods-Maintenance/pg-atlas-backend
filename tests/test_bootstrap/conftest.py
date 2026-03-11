"""
Shared pytest fixtures for A5 bootstrap pipeline tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).resolve().parent.parent / "data_fixtures"


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file by name."""

    return json.loads((FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# OpenGrants fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def opengrants_pools() -> list[dict[str, Any]]:
    """3 SCF grant pools."""

    return _load_fixture("opengrants_pools.json")["data"]


@pytest.fixture
def opengrants_round1_apps() -> list[dict[str, Any]]:
    """Round 1 applications (no io.scf.code)."""

    return _load_fixture("opengrants_applications_round1.json")["data"]


@pytest.fixture
def opengrants_round30_apps() -> list[dict[str, Any]]:
    """Round 30 applications (with io.scf.code)."""

    return _load_fixture("opengrants_applications_round30.json")["data"]


@pytest.fixture
def opengrants_empty_apps() -> list[dict[str, Any]]:
    """Empty round with no applications."""

    return _load_fixture("opengrants_applications_round39.json")["data"]


# ---------------------------------------------------------------------------
# deps.dev fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def depsdev_package_stellar_sdk() -> dict[str, Any]:
    """deps.dev GetPackage response for stellar-sdk."""

    return _load_fixture("depsdev_package_stellar_sdk.json")


@pytest.fixture
def depsdev_requirements_stellar_sdk() -> list[dict[str, Any]]:
    """deps.dev requirements for stellar-sdk."""

    return _load_fixture("depsdev_requirements_stellar_sdk.json")["requirements"]


@pytest.fixture
def depsdev_project_batch() -> dict[str, Any]:
    """deps.dev GetProjectBatch response for StellarCN repos."""

    return _load_fixture("depsdev_project_batch_stellarcn.json")
