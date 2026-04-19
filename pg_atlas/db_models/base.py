"""
SQLAlchemy declarative base, shared type aliases, enum types, and custom column
types for PG Atlas ORM models.

Design notes:
- All tables use integer surrogate PKs to avoid index fragmentation; ``canonical_id``
  is stored as a secondary unique column.
- ``HexBinary`` bridges PostgreSQL BYTEA and Python hex strings so that content
  hashes are stored compactly while remaining human-readable in application code.
- Enum columns use native PostgreSQL ENUM types for DB-level constraint enforcement.
- All timestamp columns use ``TIMESTAMP WITH TIME ZONE`` (``DateTime(timezone=True)``);
  the application only ever writes UTC datetimes — no localization.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import enum
from typing import Annotated, Any

import msgspec
import sqlalchemy.types as types
from sqlalchemy import Dialect, MetaData, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, mapped_column
from sqlalchemy.types import TypeDecorator

from pg_atlas.db_models.release import Release


def enum_values(obj: type[enum.Enum]) -> list[str]:
    """
    Return the ``.value`` strings for all members of a Python enum.

    Pass this as ``values_callable`` to ``sqlalchemy.Enum(...)`` so that the
    PostgreSQL ENUM type stores the *values* of each member (e.g. ``"in-dev"``)
    rather than the Python *names* (e.g. ``"in_dev"``).

    Using this consistently across all enum columns keeps the DB representation
    in sync with the Python enum values and avoids name/value drift over time.
    """
    return [e.value for e in obj]


# ---------------------------------------------------------------------------
# Custom column types
# ---------------------------------------------------------------------------


class HexBinary(types.TypeDecorator[str]):
    """
    Converts between fixed-length bytes and their hexadecimal string representations.

    Stores values as ``BYTEA`` in PostgreSQL (compact, byte-aligned storage) and
    exposes them as lowercase hex strings in Python (human-readable, hashlib-compatible).

    Example: a SHA-256 digest is stored as 32 bytes but accessed as a 64-character hex string.

    Usage::

        content_hash = Annotated[str, mapped_column(HexBinary(length=32))]
    """

    impl = types.LargeBinary
    cache_ok = True

    def __init__(self, length: int) -> None:
        super().__init__(length=length)

    def process_bind_param(self, value: str | None, dialect: Dialect) -> bytes | None:
        """Convert hex string → bytes before writing to the database."""
        if value is None:
            return None
        return bytes.fromhex(value)

    def process_result_value(self, value: bytes | None, dialect: Dialect) -> str:
        """Convert bytes → hex string after reading from the database."""
        if value is None:
            # Can occur during outer joins or eager loading of nullable relations.
            return ""
        return value.hex()


class ReleaseListJSONB(TypeDecorator[list[Release] | None]):
    """
    JSONB mapper that exposes release payloads as ``list[Release]`` in Python.
    Values are stored as plain JSON dictionaries in PostgreSQL.
    """

    impl = JSONB
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Any) -> list[dict[str, str]] | None:
        """Serialize ``Release`` structs into JSON-compatible dictionaries."""
        if value is None:
            return None

        return msgspec.to_builtins(value)

    def process_result_value(self, value: Any, dialect: Any) -> list[Release] | None:
        """Deserialize JSON dictionaries into ``Release`` structs."""
        if value is None:
            return None

        return msgspec.convert(value, list[Release])


# ---------------------------------------------------------------------------
# Enum types
# ---------------------------------------------------------------------------


class RepoVertexType(enum.Enum):
    """Discriminator values for the RepoVertex joined-table-inheritance hierarchy."""

    repo = "repo"
    external_repo = "external-repo"


class ProjectType(enum.Enum):
    """Classification of a Project within the PG Atlas universe."""

    public_good = "public-good"
    scf_project = "scf-project"


class ActivityStatus(enum.Enum):
    """
    Lifecycle status of a Project.

    Updated by the SCF Impact Survey (yearly) and higher-frequency signals;
    see the Activity Status Update Logic in the architecture docs.
    """

    live = "live"
    in_dev = "in-dev"
    discontinued = "discontinued"
    non_responsive = "non-responsive"


class Visibility(enum.Enum):
    """Whether a Repo is publicly accessible (affects data collection scope)."""

    public = "public"
    private = "private"


class EdgeConfidence(enum.Enum):
    """How firmly an edge was established."""

    verified_sbom = "verified-sbom"
    inferred_shadow = "inferred-shadow"


class SubmissionStatus(enum.Enum):
    """Processing state of an artifact-linked audit record."""

    pending = "pending"
    processed = "processed"
    failed = "failed"


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------

#: Constraint naming convention — generates deterministic DDL names that make
#: Alembic autogenerate diffs stable and readable across schema versions.
NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class PgBase(MappedAsDataclass, DeclarativeBase):
    """
    Shared declarative base for all PG Atlas ORM models.

    Combines ``MappedAsDataclass`` (model instances behave as plain Python dataclasses)
    with ``DeclarativeBase`` (SQLAlchemy 2.x ORM mapping). Subclasses are simultaneously
    dataclasses and mapped ORM classes.

    **Async requirement**: all ``relationship()`` declarations on subclasses MUST use
    ``lazy="selectin"`` (or be loaded via explicit ``selectinload``/``joinedload`` options).
    Default lazy loading will raise ``MissingGreenlet`` errors in an async context.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# ---------------------------------------------------------------------------
# Reusable annotated column type aliases
# ---------------------------------------------------------------------------

#: Integer surrogate primary key. Excluded from ``__init__`` — set by the DB.
#: Declare at the attribute level as: ``id: Mapped[intpk] = mapped_column(init=False)``
intpk = Annotated[int, mapped_column(primary_key=True)]

#: Standard canonical identifier column (e.g. ``"github:org/repo"``, DAOIP-5 URI).
#: Unique and indexed on every table that carries one.
canonical_id = Annotated[str, mapped_column(String(512), unique=True, index=True)]

#: SHA-256 content hash stored as 32 bytes (BYTEA), exposed as a 64-char hex string.
#: Used for email_hash on Contributor and sbom_content_hash on SbomSubmission.
content_hash = Annotated[str, mapped_column(HexBinary(length=32))]
