"""
Parse gitlog worker logs and emit GitHub Actions outputs.

Extracts:
- Per-queue status counts from worker output
- warning/error lines
- gitlog rate-limit counters from runtime summary logs
- terminal-failure private-marking list
- optional ``gh auth status`` output from a separate log file

Usage::

    python .github/scripts/parse-gitlog-log.py gitlog-worker.log [gh-auth-status.log]

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Add the scripts directory to sys.path to allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from worker_log_utils import emit_github_output, parse_base_log_line

_RATE_LIMIT_RE = re.compile(
    r"Gitlog rate-limit stats: first_rate_limit_hit_after_n_repos=(?P<first>\S+) " r"total_rate_limit_hits=(?P<total>\d+)"
)
_TERMINAL_PRIVATE_RE = re.compile(r"Gitlog terminal failures marked private: (?P<urls>.+)$")


def parse_worker_log(log_path: str) -> tuple[dict[str, dict[str, int]], list[str], list[str], str, str, str]:
    """Parse gitlog worker log and return summary fields."""

    status_counts: dict[str, dict[str, int]] = {}
    warnings: list[str] = []
    errors: list[str] = []
    first_rate_limit_hit_after_n_repos = "none"
    total_rate_limit_hits = "0"
    terminal_failures_marked_private = "none"

    if not Path(log_path).exists():
        return (
            status_counts,
            warnings,
            errors,
            first_rate_limit_hit_after_n_repos,
            total_rate_limit_hits,
            terminal_failures_marked_private,
        )

    with open(log_path) as file:
        for raw_line in file:
            line = raw_line.rstrip("\n")

            rate_match = _RATE_LIMIT_RE.search(line)
            if rate_match:
                first_rate_limit_hit_after_n_repos = rate_match.group("first")
                total_rate_limit_hits = rate_match.group("total")

            terminal_match = _TERMINAL_PRIVATE_RE.search(line)
            if terminal_match:
                terminal_failures_marked_private = terminal_match.group("urls")

            parsed = parse_base_log_line(line)
            if not parsed:
                continue

            tag, data = parsed
            if tag == "status":
                status_counts[data["queue"]] = data
            elif tag == "warning":
                warnings.append(data)
            elif tag == "error":
                errors.append(data)

    return (
        status_counts,
        warnings,
        errors,
        first_rate_limit_hit_after_n_repos,
        total_rate_limit_hits,
        terminal_failures_marked_private,
    )


def _read_auth_status(path: str | None) -> str:
    """Read ``gh auth status`` log content when available."""

    if path is None:
        return "Unavailable"

    p = Path(path)
    if not p.exists():
        return "Unavailable"

    content = p.read_text().strip()
    if not content:
        return "Unavailable"

    return content


def main() -> None:
    """Entry point."""

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <worker-log-file> [<gh-auth-status-log>]", file=sys.stderr)
        sys.exit(1)

    worker_log = sys.argv[1]
    auth_log = sys.argv[2] if len(sys.argv) > 2 else None

    (
        counts,
        warnings,
        errors,
        first_rate_limit_hit_after_n_repos,
        total_rate_limit_hits,
        terminal_failures_marked_private,
    ) = parse_worker_log(worker_log)

    extra_outputs = {
        "first_rate_limit_hit_after_n_repos": first_rate_limit_hit_after_n_repos,
        "total_rate_limit_hits": total_rate_limit_hits,
        "terminal_failures_marked_private": terminal_failures_marked_private,
        "gh_auth_status": _read_auth_status(auth_log),
    }

    emit_github_output(counts, warnings, errors, extra_outputs=extra_outputs)


if __name__ == "__main__":
    main()
