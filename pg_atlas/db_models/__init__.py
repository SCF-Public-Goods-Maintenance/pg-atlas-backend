"""
PG Atlas ORM models — single import point.

Importing this module registers all models on ``PgBase.metadata``, which is required
before running ``alembic revision --autogenerate`` or any code that calls
``Base.metadata.create_all()``.

Usage in Alembic env.py::

    import pg_atlas.db_models  # noqa: F401 — registers all models on PgBase.metadata
    from pg_atlas.db_models.base import PgBase

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from pg_atlas.db_models.base import PgBase
from pg_atlas.db_models.contributed_to import ContributedTo
from pg_atlas.db_models.contributor import Contributor
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.project import Project
from pg_atlas.db_models.repo_vertex import ExternalRepo, Repo, RepoVertex
from pg_atlas.db_models.sbom_submission import SbomSubmission

__all__ = [
    "PgBase",
    "Project",
    "RepoVertex",
    "Repo",
    "ExternalRepo",
    "Contributor",
    "DependsOn",
    "ContributedTo",
    "SbomSubmission",
]
