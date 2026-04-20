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

import re
import sys
from pathlib import Path

# Add the scripts directory to sys.path to allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from worker_log_utils import emit_github_output

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
    emit_github_output({}, [], [], extra_outputs=outputs)


if __name__ == "__main__":
    main()
