"""
Set ``Project.pg_award`` from the project override files.

Usage::

    uv run python -m pg_atlas.procrastinate.pg_award

Runs the same reconciliation as ``sync_opengrants``: the entries of
``pg-proposal-overrides.yml`` are ``pg_award`` unless they set
``pg_award: false``, every other project is not. Then it logs how many
``pg_award`` projects have a ``Project`` row and warns once per ``pg_award``
key without one. It needs ``PG_ATLAS_DATABASE_URL`` only (the task module it
imports requires it at import): no OpenGrants, no GitHub token, no running
worker.

The command is meant for local databases and dumps. Production flags come from
the weekly bootstrap sync (``sync_opengrants``).

Order on a dump: migration (``uv run alembic upgrade heads``), this command,
maintenance collection, maintenance materialization. A writing materialization
before this command removes every stored maintenance profile, because no
project is ``pg_award`` yet.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Collection

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.project import Project
from pg_atlas.db_models.session import get_session_factory
from pg_atlas.procrastinate.upserts import reconcile_pg_award

logger = logging.getLogger(__name__)


async def missing_pg_award_projects(session: AsyncSession, pg_award_canonical_ids: Collection[str]) -> list[str]:
    """Return the sorted ``pg_award_canonical_ids`` that have no ``Project`` row."""

    found_canonical_ids = set(
        (
            await session.execute(select(Project.canonical_id).where(Project.canonical_id.in_(sorted(pg_award_canonical_ids))))
        ).scalars()
    )

    return sorted(set(pg_award_canonical_ids) - found_canonical_ids)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description="Set Project.pg_award from pg-proposal-overrides.yml.")


async def _async_main() -> None:
    """Reconcile the flags, then report the proposal keys without a ``Project`` row."""

    # Importing ``tasks`` builds the Procrastinate app, which requires ``PG_ATLAS_DATABASE_URL``.
    from pg_atlas.procrastinate.tasks import load_project_overrides

    project_overrides = load_project_overrides()
    pg_award_canonical_ids = {key for key, entry in project_overrides.items() if entry["pg_award"]}

    set_true, set_false = await reconcile_pg_award(pg_award_canonical_ids)
    logger.info(f"pg_award reconciled: set_true={set_true} set_false={set_false}")

    factory = get_session_factory()
    async with factory() as session:
        missing = await missing_pg_award_projects(session, pg_award_canonical_ids)

    logger.info(f"pg_award projects found: {len(pg_award_canonical_ids) - len(missing)} of {len(pg_award_canonical_ids)}")
    for canonical_id in missing:
        logger.warning(f"pg_award project without a Project row: {canonical_id}")


def main() -> None:
    _build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
