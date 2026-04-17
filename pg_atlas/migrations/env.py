"""
Alembic environment for PG Atlas async SQLAlchemy migrations.

The database URL is read from settings (PG_ATLAS_DATABASE_URL), so this file
never contains credentials. The target metadata is derived from PgBase so that
``alembic revision --autogenerate`` detects schema diffs against the ORM models.

Async SQLAlchemy requires the async migration pattern: we use
``async_engine_from_config`` and ``connection.run_sync`` to drive the migration
context inside a coroutine that is kicked off from synchronous ``run_migrations_online``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
from logging.config import fileConfig
from typing import TYPE_CHECKING, Any, Literal

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

import pg_atlas.db_models  # noqa: F401 — registers all ORM models on PgBase.metadata  # type: ignore[reportUnusedImport]
from pg_atlas.config import settings
from pg_atlas.db_models.base import HexBinary, PgBase

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext
    from alembic.runtime.environment import NameFilterParentNames, NameFilterType
    from sqlalchemy.engine import Connection

# Alembic Config — provides access to alembic.ini values.
config = context.config

# Wire up the DB URL from application settings so credentials never live in ini.
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# Configure logging from alembic.ini.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

logger = logging.getLogger(__name__)

# All ORM models are registered on PgBase.metadata by the import above.
# Add new model modules to pg_atlas/db_models/__init__.py to include them in
# autogenerate diff detection.
target_metadata = PgBase.metadata


def include_name(name: str | None, type_: NameFilterType, parent_names: NameFilterParentNames) -> bool:
    """Automigrate only tables that are in the metadata; explicitly exclude all Procrastinate tables."""
    if name and type_ == "table":
        table_in_metadata = name in target_metadata.tables
        starts_with_procrastinate = name.startswith("procrastinate_")
        if table_in_metadata and starts_with_procrastinate:
            logger.error(f"Registered table {name} starts with prefix 'procrastinate_'")
            return False

        elif not table_in_metadata and not starts_with_procrastinate:
            logger.error(f"Unexpected table {name} found in the DB.")

        return table_in_metadata
    else:
        return True


def render_item(type_: str, obj: Any, autogen_context: AutogenContext) -> str | Literal[False]:
    """
    Custom type renderer for Alembic autogenerate.

    Maps ``HexBinary`` instances to the plain ``sa.LargeBinary(length=N)``
    expression so that generated migration files do not reference the custom
    type class and remain self-contained.
    """
    if type_ == "type" and isinstance(obj, HexBinary):
        # ``obj.impl.length`` is typed to be an int by `HexBinary.__init__`.
        length = getattr(obj.impl, "length", None)
        if not isinstance(length, int):
            raise TypeError("HexBinary length must be an integer for rendering")
        return f"sa.LargeBinary(length={length})"
    return False  # fall through to default rendering


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode (no live DB connection).

    Emits SQL to stdout or a file rather than executing against a database.
    Useful for generating migration scripts for review before applying.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_name=include_name,
        render_item=render_item,
        render_as_batch=False,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Configure the migration context for a live connection and run migrations."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_name=include_name,
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine and drive migrations via run_sync."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live database."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
