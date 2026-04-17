"""
Parse materialize_pony stdout/tee output and emit GitHub Actions outputs.

Reads captured output from ``uv run python -m pg_atlas.metrics.materialize_pony``
and extracts the summary fields from the final log line.

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-materialize-pony-log.py pony.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
import re
import sys

_SUMMARY_RE = re.compile(
    r"Pony-factor materialization finished: "
    r"repo_rows_updated=(?P<repo_rows>\d+) "
    r"project_rows_updated=(?P<project_rows>\d+) "
    r"resolved_seed_run_ordinal=(?P<ordinal>\S+) "
    r"duration_seconds=(?P<duration>[\d.]+)"
)


def parse_log(log_path: str) -> dict[str, str]:
    """
    Scan a tee'd materialize_pony log for the summary line.

    Returns a dict of output key→value pairs.
    """

    result: dict[str, str] = {}

    with open(log_path) as f:
        for line in f:
            m = _SUMMARY_RE.search(line)

            if m:
                result["pony_repo_rows_updated"] = m.group("repo_rows")
                result["pony_project_rows_updated"] = m.group("project_rows")
                result["pony_resolved_seed_run_ordinal"] = m.group("ordinal")
                result["pony_duration_seconds"] = m.group("duration")

    return result


def main() -> None:
    """Entry point."""

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <log-file>", file=sys.stderr)
        sys.exit(1)

    outputs = parse_log(sys.argv[1])

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            for key, value in outputs.items():
                f.write(f"{key}={value}\n")
    else:
        for key, value in outputs.items():
            print(f"{key}={value}")


if __name__ == "__main__":
    main()
