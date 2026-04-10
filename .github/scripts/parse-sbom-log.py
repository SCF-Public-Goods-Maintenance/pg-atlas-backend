"""
Parse Procrastinate worker log output and emit GitHub Actions outputs for SBOM worker.

Reads captured worker output from a file and extracts:
- Per-queue status counts (succeeded, failed, todo, doing)
- Error and warning messages from task execution
- Detailed SBOM parsed OK logs lines

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-sbom-log.py sbom-worker.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add the scripts directory to sys.path to allow importing sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from worker_log_utils import emit_github_output, parse_base_log_line


def parse_log(log_path: str) -> tuple[dict[str, dict[str, int]], list[str], list[str], list[str]]:
    """
    Parse a single log file.

    Returns:
        A tuple of (status_counts_by_queue, warnings, errors, spdx_parsed_lines).
    """
    status_counts: dict[str, dict[str, int]] = {}
    warnings: list[str] = []
    errors: list[str] = []
    spdx_parsed_lines: list[str] = []

    with open(log_path) as f:
        for line in f:
            line = line.rstrip("\n")

            if "SPDX document parsed OK:" in line:
                # Capture the remaining string after the log prefix
                # Example line: "2026-07-17 10:05:00,456 INFO  pg_atlas.ingestion.spdx: SPDX document parsed OK: name='foo'..."
                partition = line.split("SPDX document parsed OK:", 1)
                if len(partition) > 1:
                    spdx_parsed_lines.append(partition[1].strip())

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

    return status_counts, warnings, errors, spdx_parsed_lines


def main() -> None:
    """Entry point."""
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <log-file> [<log-file> ...]", file=sys.stderr)
        sys.exit(1)

    all_counts: dict[str, dict[str, int]] = {}
    all_warnings: list[str] = []
    all_errors: list[str] = []
    all_spdx_parsed: list[str] = []

    for path in sys.argv[1:]:
        counts, warnings, errors, spdx_parsed = parse_log(path)
        all_counts.update(counts)
        all_warnings.extend(warnings)
        all_errors.extend(errors)
        all_spdx_parsed.extend(spdx_parsed)

    extra_outputs: dict[str, str] = {}
    if all_spdx_parsed:
        # Format a nice markdown list for the details section
        bullets = [f"- {line}" for line in all_spdx_parsed]
        extra_outputs["spdx_details"] = "\n".join(bullets)
    else:
        extra_outputs["spdx_details"] = "No valid SPDX documents parsed."

    emit_github_output(all_counts, all_warnings, all_errors, extra_outputs=extra_outputs)


if __name__ == "__main__":
    main()
