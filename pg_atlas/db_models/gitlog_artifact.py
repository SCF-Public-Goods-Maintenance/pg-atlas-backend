"""
GitLogArtifact audit table for git log processing attempts.

Each git log processing attempt writes one row, regardless of success or
failure. Successful rows store a pointer to the persisted raw git log artifact;
failure rows may keep ``artifact_path`` / ``gitlog_content_hash`` null when the
failure happened before artifact storage.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from pg_atlas.db_models.base import HexBinary, PgBase, SubmissionStatus, enum_values, intpk

if TYPE_CHECKING:
    from pg_atlas.db_models.repo_vertex import Repo


class GitLogArtifact(PgBase):
    """
    Audit row for one git log processing attempt for a repo.

    ``status=processed`` indicates contributor parsing and persistence succeeded.
    ``status=failed`` indicates a terminal or transient processing failure.
    """

    __tablename__ = "gitlog_artifacts"

    id: Mapped[intpk] = mapped_column(init=False)
    repo_id: Mapped[int] = mapped_column(ForeignKey("repos.id"))

    since_months: Mapped[int] = mapped_column(Integer)

    artifact_path: Mapped[str | None] = mapped_column(String(1024), default=None)
    gitlog_content_hash: Mapped[str | None] = mapped_column(HexBinary(length=32), default=None)

    status: Mapped[SubmissionStatus] = mapped_column(
        Enum(SubmissionStatus, name="submission_status", values_callable=enum_values, create_type=False),
        default=SubmissionStatus.pending,
    )
    error_detail: Mapped[str | None] = mapped_column(String(4096), default=None)

    submitted_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        init=False,
    )
    processed_at: Mapped[dt.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        init=False,
    )

    repo: Mapped[Repo] = relationship(
        lazy="selectin",
        init=False,
        repr=False,
    )


idx_gitlog_artifacts_repo_id = Index("ix_gitlog_artifacts_repo_id", GitLogArtifact.repo_id)
idx_gitlog_artifacts_submitted_at = Index("ix_gitlog_artifacts_submitted_at", GitLogArtifact.submitted_at)
