"""
Parse Procrastinate worker log output and emit GitHub Actions outputs.

Reads captured worker output from a file and extracts:
- Per-queue status counts (succeeded, failed, todo, doing)
- Error and warning messages from task execution

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-bootstrap-log.py \
        opengrants-worker.log package-deps-worker.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add the scripts directory to sys.path to allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from worker_log_utils import emit_github_output, parse_base_log_line


def parse_log(log_path: str) -> tuple[dict[str, dict[str, int]], list[str], list[str], dict[str, set[str]]]:
    """
    Parse a single log file.

    Returns:
        A tuple of (status_counts_by_queue, warnings, errors, unsupported_purls).
        All collections are empty if the file does not exist.
    """
    status_counts: dict[str, dict[str, int]] = {}
    warnings: list[str] = []
    errors: list[str] = []
    unsupported_purls: dict[str, set[str]] = {}

    if not Path(log_path).exists():
        return status_counts, warnings, errors, unsupported_purls

    with open(log_path) as f:
        for line in f:
            line = line.rstrip("\n")
            res = parse_base_log_line(line)
            if not res:
                continue

            tag, data = res
            if tag == "status":
                status_counts[data["queue"]] = data
            elif tag == "warning":
                warnings.append(data)
            elif tag == "error":
                errors.append(data)
            elif tag == "unsupported":
                system = data["system"]
                purls = data["purls"]
                unsupported_purls.setdefault(system, set()).update(purls)

    return status_counts, warnings, errors, unsupported_purls


def _format_unsupported_ecosystems(unsupported_purls: dict[str, set[str]]) -> str:
    """
    Build one markdown-ready grouped summary for unsupported ecosystems.
    """

    if not unsupported_purls:
        return "None"

    lines: list[str] = []
    for system in sorted(unsupported_purls):
        purls = sorted(unsupported_purls[system])
        joined = " ".join(purls)
        lines.append(f"- {system} ({len(purls)}): {joined}")

    return "\n".join(lines)


def main() -> None:
    """Entry point."""
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <log-file> [<log-file> ...]", file=sys.stderr)
        sys.exit(1)

    all_counts: dict[str, dict[str, int]] = {}
    all_warnings: list[str] = []
    all_errors: list[str] = []
    all_unsupported_purls: dict[str, set[str]] = {}

    for path in sys.argv[1:]:
        counts, warnings, errors, unsupported_purls = parse_log(path)
        all_counts.update(counts)
        all_warnings.extend(warnings)
        all_errors.extend(errors)
        for system, purls in unsupported_purls.items():
            all_unsupported_purls.setdefault(system, set()).update(purls)

    unsupported_group_count = len(all_unsupported_purls)
    unsupported_purl_count = sum(len(values) for values in all_unsupported_purls.values())
    unsupported_summary = _format_unsupported_ecosystems(all_unsupported_purls)

    emit_github_output(
        all_counts,
        all_warnings,
        all_errors,
        extra_outputs={
            "unsupported_ecosystem_group_count": str(unsupported_group_count),
            "unsupported_ecosystem_purl_count": str(unsupported_purl_count),
            "unsupported_ecosystems": unsupported_summary,
        },
    )


if __name__ == "__main__":
    main()
