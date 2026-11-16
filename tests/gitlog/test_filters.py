"""
Unit tests for bot detection in pg_atlas.gitlog.filters.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import re

from pg_atlas.gitlog.filters import DEFAULT_BOT_EMAIL_PATTERNS, DEFAULT_BOT_NAME_PATTERNS, is_bot

# ---------------------------------------------------------------------------
# Bot detection — True cases
# ---------------------------------------------------------------------------


def test_bot_github_app_name() -> None:
    assert is_bot("dependabot[bot]", "49699333+dependabot[bot]@users.noreply.github.com") is True


def test_bot_github_actions_name() -> None:
    assert is_bot("github-actions[bot]", "41898282+github-actions[bot]@users.noreply.github.com") is True


def test_bot_renovate_name() -> None:
    assert is_bot("renovate[bot]", "29139614+renovate[bot]@users.noreply.github.com") is True


def test_bot_scaffold_name() -> None:
    assert is_bot("scaffold[bot]", "scaffold@example.com") is True


def test_bot_pre_commit_ci_name() -> None:
    assert is_bot("pre-commit-ci[bot]", "66853113+pre-commit-ci[bot]@users.noreply.github.com") is True


def test_bot_case_insensitive() -> None:
    assert is_bot("Dependabot[BOT]", "someone@example.com") is True


def test_bot_dependabot_without_suffix() -> None:
    assert is_bot("dependabot", "dependabot@example.com") is True


def test_bot_greenkeeper() -> None:
    assert is_bot("greenkeeper", "greenkeeper@example.com") is True


def test_bot_detected_by_email_fallback() -> None:
    """Name does NOT match any name pattern, but email matches bot noreply format."""
    assert is_bot("SomeBot", "12345+somebot[bot]@users.noreply.github.com") is True


def test_added_bot_names() -> None:
    """Recognize the requested bot names and the Stellar Light namespace."""
    for name in (
        "directory-sync-bot",
        "Claude",
        "Copilot",
        "trezor-ci",
        "stellarlight-bot",
        "stellarlight-task-lane",
        "stellarlight-repair-lane",
        "stellarlight-quality-bot",
    ):
        assert is_bot(name, "person@example.com") is True


def test_stellarlight_bot_emails() -> None:
    """Recognize Stellar Light bot and task-lane email addresses only."""
    for email in ("bot@stellarlight.xyz", "task-lane@stellarlight.xyz", "repair-lane@stellarlight.xyz"):
        assert is_bot("Human", email) is True

    assert is_bot("Human", "person@stellarlight.xyz") is False
    assert is_bot("Human", "bot@other.example") is False


# ---------------------------------------------------------------------------
# Bot detection — False cases (humans)
# ---------------------------------------------------------------------------


def test_human_regular_name() -> None:
    assert is_bot("John Doe", "john@example.com") is False


def test_human_noreply_email() -> None:
    """Human noreply emails do NOT contain [bot]."""
    assert is_bot("John", "12345+john@users.noreply.github.com") is False


def test_human_name_containing_bot_substring() -> None:
    """Patterns use anchors — substring 'bot' in name is not a match."""
    assert is_bot("robotics-team", "team@example.com") is False


def test_empty_name() -> None:
    """Empty name: not a bot (just misconfigured git)."""
    assert is_bot("", "bot@example.com") is False


def test_empty_email() -> None:
    """Empty email but bot name matches: still detected as bot."""
    assert is_bot("dependabot[bot]", "") is True


# ---------------------------------------------------------------------------
# Pattern compilation
# ---------------------------------------------------------------------------


def test_patterns_are_precompiled() -> None:
    """Verify patterns are re.Pattern instances (compiled at import time)."""
    for pattern in DEFAULT_BOT_NAME_PATTERNS:
        assert isinstance(pattern, re.Pattern)
    for pattern in DEFAULT_BOT_EMAIL_PATTERNS:
        assert isinstance(pattern, re.Pattern)
