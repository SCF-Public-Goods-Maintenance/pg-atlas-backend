"""
Parse materialize_criticality stdout/tee output and emit GitHub Actions outputs.

Reads captured output from ``uv run python -m pg_atlas.metrics.materialize_criticality``
and extracts the summary fields from the final log line.

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-materialize-criticality-log.py criticality.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
import re
import sys

_SUMMARY_RE = re.compile(
    r"A9 criticality materialization finished: "
    r"dep_nodes_seen=(?P<dep_nodes>\d+) "
    r"active_dep_nodes_scored=(?P<active_nodes>\d+) "
    r"duration_seconds=(?P<duration>[\d.]+)"
)


def parse_log(log_path: str) -> dict[str, str]:
    """
    Scan a tee'd materialize_criticality log for the summary line.

    Returns a dict of output key→value pairs.
    """

    result: dict[str, str] = {}

    with open(log_path) as f:
        for line in f:
            m = _SUMMARY_RE.search(line)

            if m:
                result["criticality_dep_nodes_seen"] = m.group("dep_nodes")
                result["criticality_active_nodes_scored"] = m.group("active_nodes")
                result["criticality_duration_seconds"] = m.group("duration")

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
