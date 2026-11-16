"""
Print project IDs as arguments for a targeted Reference Graph Bootstrap.

Usage::

    uv run python scripts/print_pg_project_filter.py

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import re
from pathlib import Path

import yaml
from pydantic import TypeAdapter

_PROJECTS_FILE = Path(__file__).resolve().parents[1] / "pg_atlas" / "data" / "projects" / "pg-proposal-overrides.yml"
_SAFE_SHELL_ARGUMENT = re.compile(r"[A-Za-z0-9_./:+-]+\Z")
_PROJECTS_ADAPTER = TypeAdapter(dict[str, object])


def load_project_ids(path: Path) -> list[str]:
    """Load proposal override keys, excluding the YAML schema declaration."""

    projects = _PROJECTS_ADAPTER.validate_python(yaml.safe_load(path.read_text(encoding="utf-8")), strict=True)

    return [canonical_id for canonical_id in projects if canonical_id != "$schema"]


def format_shell_argument(value: str) -> str:
    """Return a shell argument, double-quoting values that contain unsafe characters."""

    if _SAFE_SHELL_ARGUMENT.fullmatch(value):
        return value

    escaped_value = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`").replace("!", "\\!")

    return f'"{escaped_value}"'


def main() -> None:
    """Print the seed command with all PG proposal project IDs as arguments."""

    command = ["uv", "run", "python", "-m", "pg_atlas.procrastinate.seed_opengrants"]
    command.extend(format_shell_argument(canonical_id) for canonical_id in load_project_ids(_PROJECTS_FILE))
    print(" ".join(command))


if __name__ == "__main__":
    main()
