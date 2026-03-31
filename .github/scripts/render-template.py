"""
Render a Markdown template with variable substitution.

Reads a template file with ``{variable}`` placeholders and substitutes
them from ``KEY=VALUE`` pairs passed as command-line arguments.

Usage::

    python .github/scripts/render-template.py \\
        .github/templates/bootstrap-summary.md \\
        trigger=schedule run_id=12345 ...

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    """Entry point."""
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <template> [KEY=VALUE ...]", file=sys.stderr)
        sys.exit(1)

    template_path = Path(sys.argv[1])
    template = template_path.read_text()

    variables: dict[str, str] = {}
    for arg in sys.argv[2:]:
        if "=" in arg:
            key, _, value = arg.partition("=")
            variables[key] = value

    # Use format_map to allow missing keys to pass through unchanged.
    class SafeDict(dict[str, str]):
        def __missing__(self, key: str) -> str:
            return f"{{{key}}}"

    output = template.format_map(SafeDict(variables))
    print(output)


if __name__ == "__main__":
    main()
