"""
Shared parsing for comma-separated ``owner/repo`` settings entries.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations


def parse_owner_repo_entries(raw: str) -> tuple[frozenset[str], tuple[str, ...]]:
    """
    Parse a comma-separated ``owner/repo`` list, lowercased.

    Returns the valid entries and, separately, the rejected raw items (an
    entry needs exactly two nonempty slash-separated components), so callers
    keep their own warning and caching policies.
    """

    valid: set[str] = set()
    rejected: list[str] = []
    for item in raw.split(","):
        entry = item.strip().lower()
        if not entry:
            continue

        parts = entry.split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            rejected.append(item.strip())
            continue

        valid.add(entry)

    return frozenset(valid), tuple(rejected)
