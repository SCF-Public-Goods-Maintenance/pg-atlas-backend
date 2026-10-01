"""
Shared jsonb merge expression for ``Repo.repo_metadata`` writers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy import func, literal
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.elements import ColumnElement

from pg_atlas.db_models.repo_vertex import Repo


def repo_metadata_merge_expression(payload: Mapping[str, object]) -> ColumnElement[dict[str, object]]:
    """
    Build the jsonb expression that merges ``payload`` into ``Repo.repo_metadata``.

    Replaces exactly the payload's top-level keys and preserves every other
    key without a read-modify-write round trip, so concurrent writers of
    other metadata keys are never clobbered. Stored metadata can be SQL NULL
    or a JSON ``null`` (the ORM writes Python ``None`` as JSON null on this
    column); both collapse to an empty object before the merge — a plain
    ``coalesce`` would turn ``'null'::jsonb || object`` into an array.
    """

    base = func.coalesce(func.nullif(Repo.repo_metadata, literal(None, type_=JSONB)), literal({}, type_=JSONB))

    return base.op("||")(literal(dict(payload), type_=JSONB))
