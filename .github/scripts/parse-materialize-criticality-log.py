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

import re
import sys
from pathlib import Path

# Add the scripts directory to sys.path to allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from worker_log_utils import emit_github_output

_SUMMARY_RE = re.compile(
    r"A9 criticality materialization finished: "
    r"dep_nodes_seen=(?P<dep_nodes>\d+) "
    r"active_dep_nodes_scored=(?P<active_nodes>\d+) "
    r"duration_seconds=(?P<duration>[\d.]+)"
)


def parse_log(log_path: str) -> dict[str, str]:
    """
    Scan a tee'd materialize_criticality log for the summary line.

    Returns a dict of output key→value pairs, empty if the file is absent.
    """

    p = Path(log_path)
    if not p.exists():
        return {}

    result: dict[str, str] = {}

    with p.open() as f:
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
    emit_github_output({}, [], [], extra_outputs=outputs)


if __name__ == "__main__":
    main()
