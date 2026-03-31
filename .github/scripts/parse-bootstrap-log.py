"""
Parse Procrastinate worker log output and emit GitHub Actions outputs.

Reads captured worker output from a file and extracts:
- Per-queue status counts (succeeded, failed, todo, doing)
- Error and warning messages from task execution

Emits key=value pairs to ``$GITHUB_OUTPUT`` for use by downstream steps.

Usage::

    python .github/scripts/parse-bootstrap-log.py \\
        opengrants-worker.log package-deps-worker.log

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
import re
import sys

# Pattern for the structured status-counts line emitted by worker.py:
# "Queue <name> final status counts: todo=N doing=N succeeded=N failed=N ..."
_STATUS_RE = re.compile(
    r"Queue (?P<queue>\S+) final status counts:"
    r" todo=(?P<todo>\d+)"
    r" doing=(?P<doing>\d+)"
    r" succeeded=(?P<succeeded>\d+)"
    r" failed=(?P<failed>\d+)"
    r" cancelled=(?P<cancelled>\d+)"
    r" aborted=(?P<aborted>\d+)"
)

# Capture WARNING and ERROR lines for the detail section.
# Log format: "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
# asctime contains a space (e.g. "2026-07-17 10:05:00,456").
_WARN_ERROR_RE = re.compile(r"^\S+ \S+ (?P<level>WARNING|ERROR)\s+\S+: (?P<message>.+)$")


def parse_log(log_path: str) -> tuple[dict[str, dict[str, int]], list[str], list[str]]:
    """
    Parse a single log file.

    Returns:
        A tuple of (status_counts_by_queue, warnings, errors).
    """
    status_counts: dict[str, dict[str, int]] = {}
    warnings: list[str] = []
    errors: list[str] = []

    with open(log_path) as f:
        for line in f:
            line = line.rstrip("\n")

            m = _STATUS_RE.search(line)
            if m:
                status_counts[m.group("queue")] = {
                    "todo": int(m.group("todo")),
                    "doing": int(m.group("doing")),
                    "succeeded": int(m.group("succeeded")),
                    "failed": int(m.group("failed")),
                    "cancelled": int(m.group("cancelled")),
                    "aborted": int(m.group("aborted")),
                }

                continue

            wm = _WARN_ERROR_RE.search(line)
            if wm:
                level = wm.group("level")
                msg = wm.group("message")
                if level == "WARNING":
                    warnings.append(msg)
                else:
                    errors.append(msg)

    return status_counts, warnings, errors


def emit_github_output(
    all_counts: dict[str, dict[str, int]],
    all_warnings: list[str],
    all_errors: list[str],
) -> None:
    """Write key=value pairs to $GITHUB_OUTPUT."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if not output_file:
        # Local testing: print to stdout.
        for queue, counts in sorted(all_counts.items()):
            safe_queue = queue.replace("-", "_")
            for key, val in sorted(counts.items()):
                print(f"{safe_queue}_{key}={val}")

        print(f"warning_count={len(all_warnings)}")
        print(f"error_count={len(all_errors)}")
        if all_warnings:
            print(f"warnings={chr(10).join(all_warnings[:50])}")

        if all_errors:
            print(f"errors={chr(10).join(all_errors[:50])}")

        return

    with open(output_file, "a") as f:
        for queue, counts in sorted(all_counts.items()):
            safe_queue = queue.replace("-", "_")
            for key, val in sorted(counts.items()):
                f.write(f"{safe_queue}_{key}={val}\n")

        f.write(f"warning_count={len(all_warnings)}\n")
        f.write(f"error_count={len(all_errors)}\n")

        # Multiline values use heredoc syntax.
        if all_warnings:
            f.write("warnings<<EOF\n")
            for w in all_warnings[:50]:
                f.write(f"{w}\n")

            f.write("EOF\n")

        if all_errors:
            f.write("errors<<EOF\n")
            for e in all_errors[:50]:
                f.write(f"{e}\n")

            f.write("EOF\n")


def main() -> None:
    """Entry point."""
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <log-file> [<log-file> ...]", file=sys.stderr)
        sys.exit(1)

    all_counts: dict[str, dict[str, int]] = {}
    all_warnings: list[str] = []
    all_errors: list[str] = []

    for path in sys.argv[1:]:
        counts, warnings, errors = parse_log(path)
        all_counts.update(counts)
        all_warnings.extend(warnings)
        all_errors.extend(errors)

    emit_github_output(all_counts, all_warnings, all_errors)


if __name__ == "__main__":
    main()
