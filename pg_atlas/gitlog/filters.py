"""
Bot detection for git log commit authors.

Pure functions with no I/O or database dependencies. Importable by
downstream metric computations (e.g. pony factor) for consistent
bot filtering.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import re

# Patterns matched against author_name (case-insensitive)
DEFAULT_BOT_NAME_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r".*\[bot\]$", re.IGNORECASE),  # All GitHub App bots
    re.compile(r"^dependabot$", re.IGNORECASE),  # dependabot without [bot]
    re.compile(r"^greenkeeper$", re.IGNORECASE),  # Greenkeeper (deprecated)
)

# Patterns matched against author_email (case-insensitive)
DEFAULT_BOT_EMAIL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"^\d+\+.+\[bot\]@users\.noreply\.github\.com$",
        re.IGNORECASE,
    ),  # GitHub App bot noreply format
)


def is_bot(author_name: str, author_email: str) -> bool:
    """
    Return True if the commit author is a bot.

    Checks author_name against name patterns first, then falls back to
    author_email against email patterns.
    """
    if author_name:
        for pattern in DEFAULT_BOT_NAME_PATTERNS:
            if pattern.search(author_name):
                return True

    if author_email:
        for pattern in DEFAULT_BOT_EMAIL_PATTERNS:
            if pattern.search(author_email):
                return True

    return False
