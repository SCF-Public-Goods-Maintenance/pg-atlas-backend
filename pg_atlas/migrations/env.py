"""Alembic environment for PG Atlas async SQLAlchemy migrations.

The database URL is read from settings (PG_ATLAS_DATABASE_URL), so this file
never contains credentials. The target metadata is derived from PgBase so that
``alembic revision --autogenerate`` detects schema diffs against the ORM models.

Async SQLAlchemy requires the async migration pattern: we use
``async_engine_from_config`` and ``connection.run_sync`` to drive the migration
context inside a coroutine that is kicked off from synchronous ``run_migrations_online``.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from pg_atlas.config import settings
from pg_atlas.db.base import PgBase

# Alembic Config — provides access to alembic.ini values.
config = context.config

# Wire up the DB URL from application settings so credentials never live in ini.
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

# Configure logging from alembic.ini.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ORM metadata for autogenerate support. Alembic compares this against the
# live schema to produce migration scripts. Import all model modules before
# this file is loaded so that their tables are registered on PgBase.metadata.
# (Models are added here as they are created in A2+.)
target_metadata = PgBase.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no live DB connection).

    Emits SQL to stdout or a file rather than executing against a database.
    Useful for generating migration scripts for review before applying.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Use the naming convention from PgBase.metadata so generated DDL
        # uses the same constraint names as the ORM.
        render_as_batch=False,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Configure the migration context for a live connection and run migrations."""
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
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
