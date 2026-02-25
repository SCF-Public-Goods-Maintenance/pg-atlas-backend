"""SQLAlchemy declarative base and shared type aliases for PG Atlas models.

All ORM models should inherit from PgBase, which combines DeclarativeBase with
MappedAsDataclass so that model instances are usable as plain Python dataclasses.

Important: because the application uses async SQLAlchemy (asyncpg), lazy-loaded
relationships will raise MissingGreenlet errors at runtime. All relationships
MUST be declared with lazy="selectin" or loaded via explicit joinedload/selectinload
options in queries.

Author: SCF Public Goods Maintenance <https://github.com/SCF-Public-Goods-Maintenance>
"""

from __future__ import annotations

from typing import Annotated

from sqlalchemy import MetaData, String
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, mapped_column

# Constraint naming convention — generates deterministic names for all DDL
# constraints, which makes Alembic migration diffs clean and readable.
NAMING_CONVENTION: dict[str, str] = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class PgBase(MappedAsDataclass, DeclarativeBase):
    """Shared declarative base for all PG Atlas ORM models.

    Combines MappedAsDataclass for ergonomic Python dataclass behaviour with
    DeclarativeBase for SQLAlchemy 2.x ORM mapping. All subclasses are both
    dataclasses and mapped ORM classes.

    Remember: async context requires lazy="selectin" on all relationships.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# ---------------------------------------------------------------------------
# Reusable annotated column type aliases (extend as models are added in A2+)
# ---------------------------------------------------------------------------

# Integer surrogate primary key — use for all tables to avoid index fragmentation.
intpk = Annotated[int, mapped_column(primary_key=True, init=False)]

# Standard canonical ID column (e.g. "ecosystem:package", DAOIP-5 URI).
canonical_id_col = Annotated[str, mapped_column(String(512), unique=True, index=True)]
