"""
Parse materialize_adoption stdout/tee output and emit GitHub Actions outputs.

Reads captured output from ``uv run python -m pg_atlas.metrics.materialize_adoption``
and extracts the summary fields from the final log line.

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-materialize-adoption-log.py adoption.log

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
    r"project adoption materialization finished: "
    r"repos_seen=(?P<repos_seen>\d+) "
    r"repo_composites_computed=(?P<repo_composites>\d+) "
    r"projects_seen=(?P<projects_seen>\d+) "
    r"projects_scored=(?P<projects_scored>\d+) "
    r"duration_seconds=(?P<duration>[\d.]+)"
)


def parse_log(log_path: str) -> dict[str, str]:
    """
    Scan a tee'd materialize_adoption log for the summary line.

    Returns a dict of output key→value pairs, empty if the file is absent.
    """

    p = Path(log_path)
    if not p.exists():
        return {}

    result: dict[str, str] = {}

    with p.open() as f:
        for line in f:
            match = _SUMMARY_RE.search(line)

            if match:
                result["adoption_repos_seen"] = match.group("repos_seen")
                result["adoption_repo_composites_computed"] = match.group("repo_composites")
                result["adoption_projects_seen"] = match.group("projects_seen")
                result["adoption_projects_scored"] = match.group("projects_scored")
                result["adoption_duration_seconds"] = match.group("duration")

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
