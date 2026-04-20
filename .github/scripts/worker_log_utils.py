"""
Shared utilities for parsing Procrastinate worker logs and emitting GitHub Actions outputs.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
import re
from typing import Any

# Pattern for the structured status-counts line emitted by worker.py:
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
_WARN_ERROR_RE = re.compile(r"^\S+ \S+ (?P<level>WARNING|ERROR)\s+\S+: (?P<message>.+)$")

_UNSUPPORTED_ECOSYSTEM_RE = re.compile(
    r"registry-crawl unsupported ecosystem: system=(?P<system>[A-Z0-9_+-]+) purls=(?P<purls>.+)$"
)


def parse_base_log_line(line: str) -> tuple[str, Any] | None:
    """
    Parse a single log line for standard status counts or warning/error messages.

    Returns:
        ('status', {"queue": name, "todo": N, ...})
        ('warning', "message")
        ('error', "message")
        None
    """
    m = _STATUS_RE.search(line)
    if m:
        return "status", {
            "queue": m.group("queue"),
            "todo": int(m.group("todo")),
            "doing": int(m.group("doing")),
            "succeeded": int(m.group("succeeded")),
            "failed": int(m.group("failed")),
            "cancelled": int(m.group("cancelled")),
            "aborted": int(m.group("aborted")),
        }

    unsupported_match = _UNSUPPORTED_ECOSYSTEM_RE.search(line)
    if unsupported_match:
        purls_text = unsupported_match.group("purls").strip()
        purls = [purl for purl in purls_text.split(" ") if purl]
        return "unsupported", {
            "system": unsupported_match.group("system"),
            "purls": purls,
        }

    wm = _WARN_ERROR_RE.search(line)
    if wm:
        level = wm.group("level")
        return ("warning" if level == "WARNING" else "error"), wm.group("message")

    return None


def emit_github_output(
    all_counts: dict[str, dict[str, int]],
    all_warnings: list[str],
    all_errors: list[str],
    extra_outputs: dict[str, str] | None = None,
) -> None:
    """Write key=value pairs to $GITHUB_OUTPUT."""
    extra_outputs = extra_outputs or {}
    output_file = os.environ.get("GITHUB_OUTPUT")

    if not output_file:
        # Local testing: print to stdout.
        for queue, counts in sorted(all_counts.items()):
            safe_queue = queue.replace("-", "_")
            for key, val in sorted(counts.items()):
                print(f"{safe_queue}_{key}={val}")

        print(f"warning_count={len(all_warnings)}")
        print(f"error_count={len(all_errors)}")
        for k, v in extra_outputs.items():
            if "\n" not in v:
                print(f"{k}={v}")
            else:
                print(f"{k}<<EOF\n{v}\nEOF")

        if all_warnings:
            print("warnings<<EOF\n")
            for w in all_warnings[:50]:
                print(f"* {w}")

            print("EOF")

        if all_errors:
            print("errors<<EOF\n")
            for e in all_errors[:50]:
                print(f"* {e}")

            print("EOF")

        return

    with open(output_file, "a") as f:
        for queue, counts in sorted(all_counts.items()):
            safe_queue = queue.replace("-", "_")
            for key, val in sorted(counts.items()):
                f.write(f"{safe_queue}_{key}={val}\n")

        f.write(f"warning_count={len(all_warnings)}\n")
        f.write(f"error_count={len(all_errors)}\n")

        for k, v in extra_outputs.items():
            if "\n" not in v:
                f.write(f"{k}={v}\n")
            else:
                f.write(f"{k}<<EOF\n{v}\nEOF\n")

        # Multiline values use heredoc syntax.
        if all_warnings:
            f.write("warnings<<EOF\n\n")
            for w in all_warnings[:50]:
                f.write(f"* {w}\n")

            f.write("EOF\n")

        if all_errors:
            f.write("errors<<EOF\n\n")
            for e in all_errors[:50]:
                f.write(f"* {e}\n")

            f.write("EOF\n")
