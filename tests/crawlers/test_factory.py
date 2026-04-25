"""
Tests for registry crawler factory helpers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from pg_atlas.crawlers.cargo import CargoCrawler
from pg_atlas.crawlers.factory import build_registry_crawler, normalize_registry_system
from pg_atlas.crawlers.npm import NpmCrawler
from pg_atlas.crawlers.pypi import PyPICrawler


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("npm", "NPM"),
        ("node", "NPM"),
        ("nodejs", "NPM"),
        ("cargo", "CARGO"),
        ("crates", "CARGO"),
        ("cratesio", "CARGO"),
        ("pypi", "PYPI"),
        ("pip", "PYPI"),
    ],
)
def test_normalize_registry_system_supports_new_aliases(alias: str, canonical: str) -> None:
    """New direct-registry ecosystems normalize to canonical system tokens."""

    assert normalize_registry_system(alias) == canonical


@pytest.mark.parametrize(
    ("system", "crawler_type"),
    [
        ("NPM", NpmCrawler),
        ("CARGO", CargoCrawler),
        ("PYPI", PyPICrawler),
    ],
)
def test_build_registry_crawler_supports_new_systems(system: str, crawler_type: type[object]) -> None:
    """Factory builds the expected crawler subclass for each new system."""

    client = AsyncMock()
    session_factory = AsyncMock()
    crawler = build_registry_crawler(
        system,
        client=client,
        session_factory=session_factory,
        rate_limit=0.0,
        max_retries=3,
    )

    assert isinstance(crawler, crawler_type)
