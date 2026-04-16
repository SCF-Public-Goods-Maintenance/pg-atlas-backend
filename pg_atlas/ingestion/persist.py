"""
SBOM persistence flow for PostgreSQL-backed ingestion.

The HTTP request thread now owns only the synchronous admission steps:

0.  Check for an existing non-failed submission with the same repository and
    SBOM content hash; if it exists, record a duplicate ``SbomSubmission`` row
    and skip further processing.
1.  Store the raw SBOM bytes as an artifact (filesystem for local dev / CID
    for prod).
2.  Parse and validate the SPDX 2.3 document; on failure create a ``failed``
    ``SbomSubmission`` row so the payload is retained for manual triage.
3.  Create and commit a ``pending`` ``SbomSubmission`` audit row.
4.  Defer background processing to the dedicated Procrastinate ``sbom`` queue.

The worker path then owns the heavy graph mutation steps:

5.  Re-read the stored artifact and re-validate it.
6.  Upsert the submitting ``Repo`` vertex (canonical_id derived from the OIDC
    ``repository`` claim as ``pkg:github/owner/repo``).
7.  Upsert each declared package as an ``ExternalRepo`` vertex when needed and
    map SPDX package ids to graph vertices.
    TODO: after A5 we need to check for Project membership; some vertices will
    become ``Repo`` instead of ``ExternalRepo``.
8.  Bulk-replace outgoing ``DependsOn`` edges from the submitting repo and
    upsert any nested package-to-package ``DEPENDS_ON`` edges present in the
    SPDX relationships.
9.  Mark the ``SbomSubmission`` as ``processed`` and commit.

If any request-thread or worker step fails, the relevant ``SbomSubmission`` row
is marked ``failed`` with preserved error detail so the raw artifact remains
available for diagnosis.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
from collections.abc import Sequence
from typing import Any, TypedDict, cast

from pydantic import BaseModel
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import make_transient

from pg_atlas.db_models.base import EdgeConfidence, SubmissionStatus, Visibility
from pg_atlas.db_models.depends_on import DependsOn
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.db_models.sbom_submission import SbomSubmission
from pg_atlas.db_models.vertex_ops import get_vertex
from pg_atlas.db_models.vertex_ops import upsert_external_repo as _upsert_external_repo
from pg_atlas.ingestion.queue import defer_sbom_processing
from pg_atlas.ingestion.spdx import ParsedSbom, SpdxValidationError, parse_and_validate_spdx
from pg_atlas.storage.artifacts import read_artifact, store_artifact

logger = logging.getLogger(__name__)


class _DependsOnUpsertRow(TypedDict):
    """Typed payload for one nested ``DependsOn`` upsert row."""

    in_vertex_id: int
    out_vertex_id: int
    version_range: str | None
    confidence: EdgeConfidence


class SbomQueueingError(RuntimeError):
    """Raised when a validated SBOM could not be deferred for background processing."""


# ---------------------------------------------------------------------------
# Canonical ID helpers
# ---------------------------------------------------------------------------


def canonical_id_for_github_repo(repository: str) -> str:
    """
    Derive a PURL-style canonical ID for a GitHub repository from an OIDC claim.

    Args:
        repository: The OIDC ``repository`` claim, e.g. ``"owner/repo"``.

    Returns:
        A version-less PURL, e.g. ``"pkg:github/owner/repo"``.
    """
    return f"pkg:github/{repository}"


def _purl_from_external_refs(pkg: Any) -> str | None:
    """
    Extract the first PURL locator from an SPDX package's ``external_references``.

    Returns the locator string if any external reference has a type that
    contains ``"purl"`` (case-insensitive), otherwise ``None``.
    """
    for ref in getattr(pkg, "external_references", []):
        ref_type = str(getattr(ref, "reference_type", "")).lower()
        if "purl" in ref_type:
            return cast(str, ref.locator)

    return None


def strip_purl_version(purl: str) -> str:
    """
    Strip the ``@version`` suffix from a PURL to produce a stable canonical ID.

    Examples::

        "pkg:pypi/requests@2.32.0"  →  "pkg:pypi/requests"
        "pkg:github/owner/repo@main"  →  "pkg:github/owner/repo"
        "pkg:npm/%40scope/pkg@1.0"  →  "pkg:npm/%40scope/pkg"
    """
    if "@" in purl:
        return purl[: purl.rindex("@")]

    return purl


def canonical_id_for_spdx_package(pkg: Any) -> str:
    """
    Derive a stable, version-less canonical ID for an SPDX 2.3 package.

    Checks ``externalRefs`` for a PURL first and strips the version suffix
    to obtain a version-agnostic identifier.  Falls back to the lower-cased
    package name if no PURL is available.

    Args:
        pkg: A ``spdx_tools.spdx.model.Package`` instance.

    Returns:
        A canonical ID suitable for ``RepoVertex.canonical_id``.
    """
    purl = _purl_from_external_refs(pkg)
    if purl:
        return strip_purl_version(purl)

    return cast(str, pkg.name).lower()


def _version_for_spdx_package(pkg: Any) -> str:
    """
    Return the version string for an SPDX package, or ``""`` if unavailable.

    spdx-tools represents absent or non-assertable values as ``None``,
    ``"NOASSERTION"``, or ``"NONE"``; all are normalised to ``""``.
    """
    version = getattr(pkg, "version", None)
    if version is None:
        return ""

    v = str(version)
    if v.upper() in ("NOASSERTION", "NONE"):
        return ""

    return v


def _repo_url_for_spdx_package(pkg: Any) -> str | None:
    """
    Return the download URL for an SPDX package if it looks like an actual URL.

    Returns ``None`` for ``"NOASSERTION"`` / ``"NONE"`` entries.
    """
    loc = getattr(pkg, "download_location", None)
    if loc is None:
        return None

    loc_str = str(loc)
    if loc_str.startswith(("http", "git+")):
        return loc_str

    return None


# ---------------------------------------------------------------------------
# DB helpers — SELECT-then-INSERT/UPDATE upsert patterns
# ---------------------------------------------------------------------------


async def _upsert_repo(
    session: AsyncSession,
    canonical_id: str,
    display_name: str,
    latest_version: str,
    repo_url: str | None,
) -> Repo:
    """
    Insert a ``Repo`` vertex or update its mutable columns if it already exists.

    Uses a SELECT-then-INSERT/UPDATE pattern that is safe with SQLAlchemy JTI.
    ``session.flush()`` is called so that the returned object has its ``id``
    populated before the caller uses it.
    """
    result = await session.execute(select(Repo).where(Repo.canonical_id == canonical_id))
    repo = result.scalar_one_or_none()
    if repo is None:
        repo = Repo(
            canonical_id=canonical_id,
            display_name=display_name,
            visibility=Visibility.public,
            latest_version=latest_version,
            repo_url=repo_url,
        )
        session.add(repo)
    else:
        repo.display_name = display_name
        if latest_version:
            repo.latest_version = latest_version
        if repo_url:
            repo.repo_url = repo_url

    await session.flush()

    return repo


async def _replace_root_depends_on_edges(
    session: AsyncSession,
    source_id: int,
    dep_vertex_ids: dict[int, str],
) -> None:
    """
    Bulk-replace all root ``DependsOn`` edges originating from ``source_id``.

    Deletes every existing outgoing edge for the submitting repo and
    re-inserts the full set declared in the current SBOM.  This is
    idempotent: re-ingesting the same SBOM produces an identical edge set.

    Args:
        session: Active ``AsyncSession`` already in a transaction.
        source_id: ``repo_vertices.id`` of the submitting Repo.
        dep_vertex_ids: Mapping of ``vertex_id`` to ``version_range`` for
            the declared dependencies.  ``version_range`` may be an empty
            string, stored as ``NULL``.
    """
    await session.execute(delete(DependsOn).where(DependsOn.in_vertex_id == source_id))
    for out_id, version_range in dep_vertex_ids.items():
        edge = DependsOn(
            in_vertex_id=source_id,
            out_vertex_id=out_id,
            version_range=version_range or None,
            confidence=EdgeConfidence.verified_sbom,
        )
        session.add(edge)

    await session.flush()


async def _upsert_depends_on_edges(
    session: AsyncSession,
    edges: Sequence[tuple[int, int, str]],
) -> None:
    """
    Insert or update nested ``DependsOn`` edges with deterministic lock order.

    Nested SPDX edges are persisted with one PostgreSQL upsert statement rather
    than one ORM round trip per edge. The batch is deduplicated and sorted by
    the composite key so concurrent SBOM ingests acquire row locks in a stable
    order.
    """

    if not edges:
        return

    deduped_rows: dict[tuple[int, int], _DependsOnUpsertRow] = {}
    for source_id, target_id, version_range in edges:
        deduped_rows[(source_id, target_id)] = {
            "in_vertex_id": source_id,
            "out_vertex_id": target_id,
            "version_range": version_range or None,
            "confidence": EdgeConfidence.verified_sbom,
        }

    ordered_rows = [deduped_rows[key] for key in sorted(deduped_rows)]
    insert_stmt = pg_insert(DependsOn).values(ordered_rows)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[DependsOn.in_vertex_id, DependsOn.out_vertex_id],
        set_={
            "version_range": insert_stmt.excluded.version_range,
            "confidence": insert_stmt.excluded.confidence,
        },
    )

    await session.execute(upsert_stmt)

    await session.flush()


async def _upsert_sbom_vertices(
    session: AsyncSession,
    sbom: ParsedSbom,
    submitting_canonical_id: str,
    submitting_repo: Repo,
) -> tuple[dict[str, int], dict[int, str]]:
    """
    Upsert package vertices and map SPDX ids to graph vertex ids.

    Non-root package vertices are deduplicated by canonical id and upserted in
    canonical-id order. This reduces redundant updates and keeps row-lock
    acquisition stable across concurrent SBOM ingests.
    """

    spdx_id_to_vertex_id: dict[str, int] = {}
    vertex_versions: dict[int, str] = {}

    canonical_inputs: dict[str, tuple[str, str, str | None]] = {}
    canonical_spdx_ids: dict[str, list[str]] = {}

    for pkg in sbom.document.packages:
        pkg_canonical_id = canonical_id_for_spdx_package(pkg)
        pkg_spdx_id = pkg.spdx_id
        if pkg_canonical_id == submitting_canonical_id:
            spdx_id_to_vertex_id[pkg_spdx_id] = submitting_repo.id
            continue

        version = _version_for_spdx_package(pkg)
        repo_url = _repo_url_for_spdx_package(pkg)
        canonical_inputs[pkg_canonical_id] = (str(pkg.name), version, repo_url)
        canonical_spdx_ids.setdefault(pkg_canonical_id, []).append(pkg_spdx_id)

    for canonical_id in sorted(canonical_inputs):
        display_name, version, repo_url = canonical_inputs[canonical_id]

        try:
            dep_vertex = await _upsert_external_repo(
                session,
                canonical_id=canonical_id,
                display_name=display_name,
                latest_version=version,
                repo_url=repo_url,
            )
        except ValueError:
            existing = await get_vertex(session, canonical_id)
            if existing is None:
                continue

            dep_vertex = existing

        for pkg_spdx_id in canonical_spdx_ids[canonical_id]:
            spdx_id_to_vertex_id[pkg_spdx_id] = dep_vertex.id

        vertex_versions[dep_vertex.id] = version

    for root_spdx_id in sbom.root_spdx_ids:
        spdx_id_to_vertex_id[root_spdx_id] = submitting_repo.id

    return spdx_id_to_vertex_id, vertex_versions


def _plan_sbom_edges(
    sbom: ParsedSbom,
    submitting_repo_id: int,
    spdx_id_to_vertex_id: dict[str, int],
    vertex_versions: dict[int, str],
) -> tuple[dict[int, str], list[tuple[int, int, str]]]:
    """
    Return root replacement edges and nested relationship edges for one SBOM.
    """

    if not sbom.dependency_relationships:
        return vertex_versions, []

    root_edge_targets: dict[int, str] = {}
    nested_edge_targets: dict[tuple[int, int], str] = {}

    for relationship in sbom.dependency_relationships:
        source_vertex_id = spdx_id_to_vertex_id.get(relationship.source_spdx_id)
        target_vertex_id = spdx_id_to_vertex_id.get(relationship.target_spdx_id)
        if source_vertex_id is None or target_vertex_id is None or source_vertex_id == target_vertex_id:
            continue

        version_range = vertex_versions.get(target_vertex_id, "")
        if relationship.source_spdx_id in sbom.root_spdx_ids or source_vertex_id == submitting_repo_id:
            root_edge_targets[target_vertex_id] = version_range
            continue

        nested_edge_targets[(source_vertex_id, target_vertex_id)] = version_range

    nested_edges = [
        (source_vertex_id, target_vertex_id, nested_edge_targets[(source_vertex_id, target_vertex_id)])
        for source_vertex_id, target_vertex_id in sorted(nested_edge_targets)
    ]

    return root_edge_targets, nested_edges


async def _mark_submission_failed(
    session: AsyncSession,
    submission_id: int,
    error_detail: str,
) -> None:
    """
    Mark an existing submission row as failed.
    """

    await session.rollback()
    submission = await session.get(SbomSubmission, submission_id)
    if submission is None:
        logger.warning(f"SBOM submission missing while marking failed: submission_id={submission_id}")

        return

    submission.status = SubmissionStatus.failed
    submission.error_detail = error_detail[:4096]
    await session.commit()


async def _record_failed_validation(
    session: AsyncSession,
    repository: str,
    actor: str,
    content_hash_hex: str,
    artifact_path: str,
    error_detail: str,
) -> None:
    """
    Persist a failed submission row for a validation-time error.
    """

    failed_submission = SbomSubmission(
        repository_claim=repository,
        actor_claim=actor,
        sbom_content_hash=content_hash_hex,
        artifact_path=artifact_path,
        status=SubmissionStatus.failed,
        error_detail=error_detail[:4096],
    )
    session.add(failed_submission)
    await session.commit()


async def _persist_sbom_graph(
    session: AsyncSession,
    repository: str,
    actor: str,
    sbom: ParsedSbom,
) -> int:
    """
    Apply repo and dependency changes for one validated SBOM document.

    Returns:
        Number of dependency edges emitted from the submitting repo.
    """

    submitting_canonical_id = canonical_id_for_github_repo(repository)
    repo_display_name = repository.split("/")[-1]
    submitting_repo = await _upsert_repo(
        session,
        canonical_id=submitting_canonical_id,
        display_name=repo_display_name,
        latest_version="",
        repo_url=f"https://github.com/{repository}",
    )

    spdx_id_to_vertex_id, vertex_versions = await _upsert_sbom_vertices(
        session,
        sbom,
        submitting_canonical_id,
        submitting_repo,
    )
    root_edge_targets, nested_edges = _plan_sbom_edges(
        sbom,
        submitting_repo.id,
        spdx_id_to_vertex_id,
        vertex_versions,
    )

    await _replace_root_depends_on_edges(session, submitting_repo.id, root_edge_targets)
    await _upsert_depends_on_edges(session, nested_edges)

    logger.info(
        "SBOM graph applied: "
        f"repository={repository} actor={actor} packages={sbom.package_count} "
        f"root_edges={len(root_edge_targets)} nested_edges={len(nested_edges)}"
    )

    return len(root_edge_targets) + len(nested_edges)


async def parse_sbom_and_persist_graph(
    session: AsyncSession,
    submission_id: int,
    expected_status: SubmissionStatus = SubmissionStatus.pending,
) -> None:
    """
    Parse one stored SBOM artifact and persist its graph mutations.

    Missing submissions or rows that do not match ``expected_status`` are
    logged and ignored.
    """

    submission = await session.get(SbomSubmission, submission_id)
    if submission is None:
        logger.warning(f"SBOM submission not found: submission_id={submission_id}")

        return

    if submission.status != expected_status:
        logger.info(
            "Skipping SBOM submission with non-selected status: "
            f"submission_id={submission_id} status={submission.status.value} expected_status={expected_status.value}"
        )

        return

    try:
        raw_body = await read_artifact(submission.artifact_path)
        sbom = parse_and_validate_spdx(raw_body)
        await _persist_sbom_graph(session, submission.repository_claim, submission.actor_claim, sbom)
        submission.status = SubmissionStatus.processed
        submission.processed_at = dt.datetime.now(dt.UTC)
        submission.error_detail = None
        await session.commit()

    except FileNotFoundError as exc:
        logger.error(f"Stored SBOM artifact missing for submission_id={submission_id}")
        await _mark_submission_failed(session, submission_id, str(exc))
    except SpdxValidationError as exc:
        logger.error(f"Stored SBOM artifact became invalid for submission_id={submission_id}")
        await _mark_submission_failed(session, submission_id, str(exc))
    except Exception as exc:
        logger.exception(f"SBOM worker processing failed for submission_id={submission_id}")
        await _mark_submission_failed(session, submission_id, str(exc))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class SbomAcceptedResponse(BaseModel):
    """Response body returned on successful SBOM submission (202 Accepted)."""

    message: str
    repository: str
    package_count: int


async def handle_sbom_submission(
    session: AsyncSession | None,
    raw_body: bytes,
    claims: dict[str, Any],
) -> SbomAcceptedResponse:
    """
    Orchestrate the request-thread portion of SBOM ingestion for one submission.

    When ``session`` is ``None`` (database not configured) the function falls
    back to the pre-A3 logging stub so that the endpoint remains functional
    in environments without a database (CI, quick local runs).

    Steps (with database):

    0.  Check for the presence of the submitted SBOM.
    1.  Store the raw artifact on the filesystem (idempotent).
    2.  Parse and validate the SPDX 2.3 document.
    3a. On validation failure: commit a ``failed`` ``SbomSubmission`` row and
        re-raise ``SpdxValidationError`` so the router returns 422.
    3b. On success: commit a ``pending`` ``SbomSubmission`` row.
    4.  Defer background processing to Procrastinate.
    5.  If the defer fails: mark the pending row ``failed`` and raise
        ``SbomQueueingError`` so the router returns 503.

    Args:
        session: SQLAlchemy ``AsyncSession``, or ``None`` when the database is
            not configured.
        raw_body: Raw SPDX 2.3 bytes from the HTTP request body.
        claims: Decoded GitHub OIDC JWT claims.  Must contain ``repository``
            (``"owner/repo"``) and ``actor`` (GitHub username).

    Returns:
        ``dict`` with keys ``message``, ``repository``, and ``package_count``
        suitable for constructing the 202 Accepted response body.

    Raises:
        SpdxValidationError: If ``raw_body`` cannot be parsed as SPDX 2.3.
            The exception is raised after a ``failed`` audit row has been
            committed (when ``session`` is not ``None``).
        SbomQueueingError: If validation succeeded but background processing
            could not be deferred.
    """
    repository: str = claims["repository"]
    actor: str = claims["actor"]

    parsed_sbom: ParsedSbom | None = None
    spdx_error: SpdxValidationError | None = None
    artifact_payload = raw_body
    content_hash_hex = hashlib.sha256(raw_body).hexdigest()

    try:
        parsed_sbom = parse_and_validate_spdx(raw_body)
        artifact_payload = parsed_sbom.unwrapped_bytes
        content_hash_hex = parsed_sbom.semantic_hash
    except SpdxValidationError as exc:
        spdx_error = exc
        if exc.unwrapped_bytes is not None:
            artifact_payload = exc.unwrapped_bytes

    artifact_filename = f"sboms/{content_hash_hex}.spdx.json"

    # ------------------------------------------------------------------
    # Fallback: no database configured — log and return stub response
    # ------------------------------------------------------------------
    if session is None:
        if spdx_error is not None:
            raise spdx_error

        assert parsed_sbom is not None
        logger.info(
            f"SBOM submission received (no DB): repository={repository} actor={actor} packages={parsed_sbom.package_count}"
        )

        return SbomAcceptedResponse(
            message="queued",
            repository=repository,
            package_count=parsed_sbom.package_count,
        )

    if spdx_error is not None:
        artifact_path, _ = await store_artifact(artifact_payload, artifact_filename)
        try:
            await _record_failed_validation(
                session,
                repository=repository,
                actor=actor,
                content_hash_hex=content_hash_hex,
                artifact_path=artifact_path,
                error_detail=str(spdx_error),
            )
            logger.info(f"SBOM validation failed, recorded for triage: repository={repository} hash={content_hash_hex}")
        except Exception:
            logger.exception(f"Failed to record failed SBOM submission for {repository}")

        raise spdx_error

    assert parsed_sbom is not None

    # ------------------------------------------------------------------
    # Check existing: if we know this SBOM for this repository, record the submission, skip processing
    # ------------------------------------------------------------------
    existing_submission = await session.scalar(
        select(SbomSubmission)
        .where(SbomSubmission.sbom_content_hash == content_hash_hex)
        .where(SbomSubmission.repository_claim == repository)
        .where(SbomSubmission.status != SubmissionStatus.failed)
    )
    if existing_submission:
        # construct a modified not-yet-persisted submission
        make_transient(existing_submission)
        existing_submission.id = None  # type: ignore[assignment] # pyright: ignore[reportAttributeAccessIssue]
        existing_submission.actor_claim = actor
        existing_submission.submitted_at = None  # type: ignore[assignment] # pyright: ignore[reportAttributeAccessIssue]
        # and commit it to the db
        session.add(existing_submission)
        await session.commit()

        return SbomAcceptedResponse(
            message="duplicate skipped",
            repository=repository,
            package_count=-1,
        )

    # ------------------------------------------------------------------
    # Store artifact — idempotent filesystem write, now always unwrapped SPDX JSON
    # ------------------------------------------------------------------
    artifact_path, _ = await store_artifact(artifact_payload, artifact_filename)

    # ------------------------------------------------------------------
    # Commit the pending submission row, then defer background processing
    # ------------------------------------------------------------------
    try:
        submission = SbomSubmission(
            repository_claim=repository,
            actor_claim=actor,
            sbom_content_hash=content_hash_hex,
            artifact_path=artifact_path,
            status=SubmissionStatus.pending,
        )
        session.add(submission)
        await session.commit()
    except Exception as exc:
        await session.rollback()
        logger.exception(f"Failed to persist pending SBOM submission for {repository}")
        try:
            await _record_failed_validation(
                session,
                repository=repository,
                actor=actor,
                content_hash_hex=content_hash_hex,
                artifact_path=artifact_path,
                error_detail=str(exc),
            )
        except Exception:
            logger.exception(f"Failed to commit failure record for {repository}")
        raise

    await session.refresh(submission)

    try:
        enqueued = await defer_sbom_processing(submission.id, repository_claim=repository)
    except Exception as exc:
        await _mark_submission_failed(session, submission.id, str(exc))
        raise SbomQueueingError(f"Could not enqueue SBOM submission {submission.id}") from exc

    if not enqueued:
        logger.info(
            f"SBOM submission accepted behind existing repo-scoped lock: submission_id={submission.id} repository={repository}"
        )

    return SbomAcceptedResponse(
        message="queued",
        repository=repository,
        package_count=parsed_sbom.package_count,
    )
