"""
Shared pytest fixtures for registry crawler tests.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

FIXTURES = Path(__file__).parent / "data_fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    """Load a JSON fixture file by name."""
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def pubdev_package_data() -> dict[str, Any]:
    """pub.dev package API response fixture."""
    return _load_fixture("pubdev_package.json")


@pytest.fixture
def pubdev_metrics_data() -> dict[str, Any]:
    """pub.dev metrics API response fixture."""
    return _load_fixture("pubdev_metrics.json")


@pytest.fixture
def pubdev_search_data() -> dict[str, Any]:
    """pub.dev search API response fixture."""
    return _load_fixture("pubdev_search.json")


@pytest.fixture
def pubdev_search_empty_data() -> dict[str, Any]:
    """pub.dev empty search API response fixture."""
    return _load_fixture("pubdev_search_empty.json")


@pytest.fixture
def pubdev_package_minimal_data() -> dict[str, Any]:
    """pub.dev minimal package (no homepage, no deps) response fixture."""
    return _load_fixture("pubdev_package_minimal.json")


@pytest.fixture
def packagist_package_data() -> dict[str, Any]:
    """Packagist package API response fixture."""
    return _load_fixture("packagist_package.json")


@pytest.fixture
def packagist_downloads_data() -> dict[str, Any]:
    """Packagist downloads API response fixture."""
    return _load_fixture("packagist_downloads.json")


@pytest.fixture
def packagist_dependents_data() -> dict[str, Any]:
    """Packagist dependents API response fixture."""
    return _load_fixture("packagist_dependents.json")


@pytest.fixture
def packagist_package_dev_only_data() -> dict[str, Any]:
    """Packagist package with only dev branches fixture."""
    return _load_fixture("packagist_package_dev_only.json")


@pytest.fixture
def packagist_dependents_empty_data() -> dict[str, Any]:
    """Packagist empty dependents API response fixture."""
    return _load_fixture("packagist_dependents_empty.json")


@pytest.fixture
def mock_http_client() -> AsyncMock:
    """Mock httpx.AsyncClient for unit tests."""
    return AsyncMock(spec=httpx.AsyncClient)
