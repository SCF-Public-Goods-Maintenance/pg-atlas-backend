"""
Unit tests for ``pg_atlas.db_models.release``.


SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from pg_atlas.db_models.release import Release, merge_releases, preferred_latest_version


def test_preferred_latest_version_semver_precedence() -> None:
    selected = preferred_latest_version(
        [
            # first semver candidate should short-circuit
            # regardless of hash/arbitrary values further down
            Release(version="v3.1.2", release_date="", purl="pkg:npm/a"),
            Release(version="abc1234", release_date="", purl="pkg:npm/a"),
            Release(version="dev-main", release_date="", purl="pkg:npm/a"),
        ]
    )

    assert selected == "v3.1.2"


def test_preferred_latest_version_hash_fallback() -> None:
    selected = preferred_latest_version(
        [
            Release(version="feature-branch", release_date="", purl="pkg:npm/a"),
            Release(version="e57ab3dd9bbf", release_date="", purl="pkg:npm/a"),
            Release(version="something-else", release_date="", purl="pkg:npm/a"),
        ]
    )

    assert selected == "e57ab3dd9bbf"


def test_merge_releases_accepts_legacy_null_release_date() -> None:
    merged = merge_releases(existing=[{"purl": "pkg:pub/a", "version": "1.0.0", "release_date": None}], incoming=[])

    assert merged is not None
    assert merged[0].release_date == ""
