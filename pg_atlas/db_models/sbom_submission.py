"""
SbomSubmission audit/deduplication table.

Every SBOM submission creates a row in this table regardless of whether downstream
processing is triggered.  The ``content_hash`` (SHA-256 of the raw SBOM bytes) enables
deduplication: if the same hash has already been processed successfully, no re-processing
is needed.  The ``repository`` and ``actor`` OIDC claims are retained for auditability.

Raw SBOM bytes are stored out-of-band (see ``pg_atlas.storage.artifacts``) and referenced
via ``artifact_path``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime

from sqlalchemy import DateTime, Enum, Index, String, func
from sqlalchemy.orm import Mapped, mapped_column

from pg_atlas.db_models.base import PgBase, SubmissionStatus, content_hash, enum_values, intpk


class SbomSubmission(PgBase):
    """
    Audit record for a single SBOM submission event.

    One row per submission attempt. The ``content_hash`` field stores the SHA-256
    digest of the raw SBOM payload (as stored bytes → BYTEA in PostgreSQL, hex string
    in Python via ``HexBinary``). Duplicate hashes with a ``processed`` status indicate
    that the incoming SBOM is identical to a previously ingested version; the submission
    is acknowledged (202) but no downstream processing is triggered.
    """

    __tablename__ = "sbom_submissions"

    # --- identity ---
    id: Mapped[intpk] = mapped_column(init=False)

    # --- OIDC provenance (from verified GitHub OIDC token claims) ---
    #: ``repository`` claim: ``owner/repo`` string identifying the submitting repo.
    repository_claim: Mapped[str] = mapped_column(String(256))
    #: ``actor`` claim: GitHub username that triggered the workflow.
    actor_claim: Mapped[str] = mapped_column(String(128))

    # --- content ---
    #: SHA-256 digest of the raw submitted SBOM bytes.
    sbom_content_hash: Mapped[content_hash]

    #: Path or reference identifying the raw artifact in the backing store.
    #: For local dev: a relative filesystem path under the configured artifact store root.
    #: For production: an IPFS CID obtained through Filebase.
    artifact_path: Mapped[str] = mapped_column(String(1024))

    # --- processing state ---
    status: Mapped[SubmissionStatus] = mapped_column(
        Enum(SubmissionStatus, name="submission_status", values_callable=enum_values),
        default=SubmissionStatus.pending,
    )
    #: Human-readable detail for ``failed`` submissions (validation error, parse error, etc.)
    error_detail: Mapped[str | None] = mapped_column(String(4096), default=None)

    # --- audit timestamps (UTC) ---
    submitted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        init=False,
    )
    processed_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        init=False,
    )


# Indexes for common deduplication and audit queries.
idx_sbom_hash = Index("ix_sbom_submissions_content_hash", SbomSubmission.sbom_content_hash)
idx_sbom_repo_claim = Index("ix_sbom_submissions_repository_claim", SbomSubmission.repository_claim)
