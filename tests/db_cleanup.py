"""
Helpers to clean up only rows created by tests.

This module snapshots primary-key values before a test and removes only the rows
that appear after the test. Pre-existing data is preserved by construction.

Set `PG_ATLAS_TEST_BREAK_BEFORE_CLEANUP=1` to trigger `breakpoint()` immediately
before deletion in teardown.
Set `PG_ATLAS_TEST_SKIP_CLEANUP=1` to skip teardown deletion (debug aid).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class TableSpec:
    """
    Describe how to identify rows in a table.

    `pk_columns` must match one primary key for single-column tables, or all
    primary key columns in order for composite-primary-key tables.
    """

    name: str
    pk_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableSnapshot:
    """Primary-key snapshot for a group of tables."""

    keys_by_table: dict[str, set[tuple[Any, ...]]]


def _format_pk_values(pk_values: tuple[Any, ...]) -> tuple[Any, ...]:
    """
    Normalize SQLAlchemy row values into a comparable tuple key.

    SQLAlchemy returns Row objects for text queries; converting each row to a
    tuple gives stable set semantics for snapshot diffing.
    """

    return tuple(pk_values)


def _debug_pause_before_cleanup() -> bool:
    """
    Optionally pause/skip cleanup for debugger-friendly test runs.

    Returns `True` when teardown deletion should be skipped.
    """

    if os.environ.get("PG_ATLAS_TEST_BREAK_BEFORE_CLEANUP") == "1":
        breakpoint()

    return os.environ.get("PG_ATLAS_TEST_SKIP_CLEANUP") == "1"


async def capture_snapshot(session: AsyncSession, table_specs: list[TableSpec]) -> TableSnapshot:
    """
    Capture current primary-key sets for each table in `table_specs`.

    The snapshot is used after the test to identify and delete only rows created
    during that test run.
    """

    keys_by_table: dict[str, set[tuple[Any, ...]]] = {}
    for table_spec in table_specs:
        columns_sql = ", ".join(table_spec.pk_columns)
        result = await session.execute(text(f"SELECT {columns_sql} FROM {table_spec.name}"))
        keys_by_table[table_spec.name] = {_format_pk_values(tuple(row)) for row in result.all()}

    return TableSnapshot(keys_by_table=keys_by_table)


async def cleanup_created_rows(
    session: AsyncSession,
    table_specs: list[TableSpec],
    snapshot: TableSnapshot,
) -> None:
    """
    Delete rows that were created after `snapshot` was taken.

    Deletion runs table-by-table in the order provided by `table_specs`. Put
    child/edge tables before parent tables to satisfy FK constraints.
    """

    if _debug_pause_before_cleanup():
        print("WARNING: test cleanup is skipped; this may break test isolation", file=sys.stderr)
        return

    for table_spec in table_specs:
        baseline_keys = snapshot.keys_by_table.get(table_spec.name, set())
        columns_sql = ", ".join(table_spec.pk_columns)
        current_rows = await session.execute(text(f"SELECT {columns_sql} FROM {table_spec.name}"))
        current_keys = {_format_pk_values(tuple(row)) for row in current_rows.all()}
        created_keys = current_keys - baseline_keys

        for created_key in created_keys:
            where_clauses = [f"{column_name} = :pk_{idx}" for idx, column_name in enumerate(table_spec.pk_columns)]
            where_sql = " AND ".join(where_clauses)
            params = {f"pk_{idx}": value for idx, value in enumerate(created_key)}
            await session.execute(text(f"DELETE FROM {table_spec.name} WHERE {where_sql}"), params)

    await session.commit()


SBOM_DB_TABLE_SPECS: list[TableSpec] = [
    TableSpec("depends_on", ("in_vertex_id", "out_vertex_id")),
    TableSpec("sbom_submissions", ("id",)),
    TableSpec("external_repos", ("id",)),
    TableSpec("repos", ("id",)),
    TableSpec("repo_vertices", ("id",)),
]


GITLOG_DB_TABLE_SPECS: list[TableSpec] = [
    TableSpec("contributed_to", ("contributor_id", "repo_id")),
    TableSpec("contributors", ("id",)),
    TableSpec("external_repos", ("id",)),
    TableSpec("repos", ("id",)),
    TableSpec("repo_vertices", ("id",)),
]


DB_MODELS_TABLE_SPECS: list[TableSpec] = [
    TableSpec("depends_on", ("in_vertex_id", "out_vertex_id")),
    TableSpec("sbom_submissions", ("id",)),
    TableSpec("external_repos", ("id",)),
    TableSpec("repos", ("id",)),
    TableSpec("projects", ("id",)),
    TableSpec("repo_vertices", ("id",)),
]
