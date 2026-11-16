"""Tests for the historical Go pseudo-version release cleanup."""

from __future__ import annotations

import importlib.util
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select, Update

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "pg_atlas"
    / "migrations"
    / "versions"
    / "20261116_1400_e93af408c615_remove_go_pseudo_versions.py"
)


def _load_migration() -> ModuleType:
    """Load the migration module directly for an isolated upgrade test."""
    spec = importlib.util.spec_from_file_location("remove_go_pseudo_versions_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    return migration


def test_upgrade_cleans_repo_and_external_release_lists(monkeypatch: Any) -> None:
    """Clean both subtype tables, update latest values, and preserve empty-list latest values."""
    pseudo = {
        "purl": "pkg:golang/github.com/org/module",
        "version": "v0.0.0-20260302000000-3f2a1b4c5d6e",
        "release_date": "2026-03-02T00:00:00Z",
    }
    commit = {
        "purl": "pkg:golang/github.com/org/module",
        "version": "3f2a1b4c5d6e",
        "release_date": "2026-03-01T00:00:00Z",
    }
    tagged = {
        "purl": "pkg:golang/github.com/org/module",
        "version": "v1.2.3",
        "release_date": "2026-02-01T00:00:00Z",
    }
    rows_by_table = {
        "repos": [
            SimpleNamespace(id=1, releases=[pseudo, commit, tagged]),
            SimpleNamespace(id=2, releases=[pseudo]),
            SimpleNamespace(
                id=3,
                releases=[
                    {
                        "purl": "pkg:cargo/module",
                        "version": pseudo["version"],
                        "release_date": pseudo["release_date"],
                    }
                ],
            ),
        ],
        "external_repos": [SimpleNamespace(id=4, releases=[pseudo, tagged])],
    }
    updates: list[tuple[str, Mapping[str, Any]]] = []

    class _Bind:
        def __init__(self) -> None:
            self.select_count = 0
            self.current_table: str | None = None

        def execute(self, statement: Any) -> Any:
            if isinstance(statement, Select):
                table_names = ("repos", "external_repos")
                self.current_table = table_names[self.select_count]
                self.select_count += 1

                return rows_by_table[self.current_table]

            if isinstance(statement, Update):
                assert self.current_table is not None
                compiled = statement.compile(dialect=postgresql.dialect())
                updates.append((self.current_table, compiled.params))

                return None

            raise AssertionError(f"Unexpected migration statement: {statement!r}")

    migration = _load_migration()
    monkeypatch.setattr(migration.op, "get_bind", lambda: _Bind())

    migration.upgrade()

    assert [(table_name, values["id_1"]) for table_name, values in updates] == [
        ("repos", 1),
        ("repos", 2),
        ("external_repos", 4),
    ]
    assert updates[0][1]["releases"] == [commit, tagged]
    assert updates[0][1]["latest_version"] == "v1.2.3"
    assert updates[1][1]["releases"] == []
    assert "latest_version" not in updates[1][1]
    assert updates[2][1]["releases"] == [tagged]
    assert updates[2][1]["latest_version"] == "v1.2.3"
