"""
SPDX 2.3 document parsing and validation for PG Atlas SBOM ingestion.

SBOMs submitted by the pg-atlas-sbom-action are SPDX 2.3 JSON documents
fetched from the GitHub Dependency Graph API. This module validates them and
extracts the package list for downstream processing.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict
from spdx_tools.spdx.model import Document
from spdx_tools.spdx.parser.error import SPDXParsingError
from spdx_tools.spdx.parser.parse_anything import parse_file

logger = logging.getLogger(__name__)


class SpdxValidationError(ValueError):
    """
    Raised when an SBOM submission fails SPDX 2.3 schema validation.

    Carries a human-readable ``detail`` string and an optional list of
    ``messages`` from the underlying spdx-tools parser for structured error
    reporting back to the caller.
    """

    def __init__(self, detail: str, messages: list[str] | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.messages = messages or []


@dataclass
class ParsedSbom:
    """
    Result of a successful SPDX 2.3 parse operation.

    Attributes:
        document: The parsed spdx-tools Document object. Use this for
            dependency extraction in A8.
        package_count: Number of packages declared in the SPDX document.
            Exposed directly for quick logging and response shaping without
            callers needing to inspect the document internals.
    """

    document: Document
    package_count: int


def _unwrap_github_api_envelope(raw: bytes) -> bytes:
    """
    Strip the GitHub Dependency Graph API envelope if present.

    The GitHub API endpoint ``/repos/{owner}/{repo}/dependency-graph/sbom``
    wraps the SPDX document in a top-level ``{"sbom": {…}}`` object. When
    the pg-atlas-sbom-action submits the raw API response, this function
    extracts the inner document so that spdx-tools can parse it correctly.

    Non-enveloped payloads (already bare SPDX JSON) are returned unchanged.
    Any bytes that cannot be decoded as UTF-8 JSON are returned unchanged so
    that the spdx-tools parser can produce the appropriate error message.
    """
    try:
        outer = json.loads(raw)
    except json.JSONDecodeError, UnicodeDecodeError:
        return raw

    if isinstance(outer, dict) and "sbom" in outer and isinstance(outer["sbom"], dict):
        return json.dumps(outer["sbom"]).encode()

    return raw


def parse_and_validate_spdx(raw: bytes) -> ParsedSbom:
    """
    Parse and validate a raw SPDX 2.3 JSON payload.

    Accepts either a bare SPDX 2.3 JSON document or the ``{"sbom": {…}}``
    envelope returned by the GitHub Dependency Graph API. The envelope is
    transparently stripped before parsing.

    Uses spdx-tools' ``parse_file`` which validates the document against the
    SPDX 2.3 JSON schema and returns a typed Document object. The document
    is not persisted here — that is the responsibility of the A8 processing
    pipeline.

    Args:
        raw: Raw bytes of the SPDX 2.3 JSON document (or GitHub API envelope)
            submitted by the action.

    Returns:
        ParsedSbom: Parsed document and package count on success.

    Raises:
        SpdxValidationError: If the bytes cannot be parsed as a valid SPDX 2.3
            JSON document, or if required fields (spdxVersion, SPDXID,
            documentNamespace, name) are missing or malformed.
    """
    raw = _unwrap_github_api_envelope(raw)

    # spdx-tools' parse_file requires a file path with a recognizable extension
    # to select the correct format parser. We write to a named temp file with a
    # .spdx.json suffix, parse it, then discard it immediately.
    try:
        with tempfile.NamedTemporaryFile(suffix=".spdx.json", delete=True) as tmp:
            tmp.write(raw)
            tmp.flush()
            document = parse_file(tmp.name)
    except SPDXParsingError as exc:
        messages = [str(m) for m in exc.get_messages()]
        logger.info(f"SPDX validation failed: {messages}")
        raise SpdxValidationError(
            detail="Invalid SPDX 2.3 document.",
            messages=messages,
        ) from exc
    except Exception as exc:
        logger.warning(f"Unexpected error during SPDX parsing: {exc}")
        raise SpdxValidationError(
            detail=f"Could not parse SPDX document: {exc}",
        ) from exc

    assert document, "Document is None"
    package_count = len(document.packages)
    logger.info(
        "SPDX document parsed OK: "
        f"name={document.creation_info.name!r} "
        f"spdx_version={document.creation_info.spdx_version} "
        f"packages={package_count}"
    )
    return ParsedSbom(document=document, package_count=package_count)


# ---------------------------------------------------------------------------
# Pydantic models for semantic-hash extraction
# ---------------------------------------------------------------------------


class _SbomPackage(BaseModel):
    """
    Typed view of one SPDX 2.3 package entry used for canonical sort-key access.

    All undeclared fields are preserved via ``extra="allow"`` so that
    ``model_dump()`` round-trips the complete package JSON for hashing.
    """

    model_config = ConfigDict(extra="allow")

    SPDXID: str = ""


class _SbomRelationship(BaseModel):
    """
    Typed view of one SPDX 2.3 relationship entry used for canonical sort keys.

    All undeclared fields are preserved via ``extra="allow"``.
    """

    model_config = ConfigDict(extra="allow")

    spdxElementId: str = ""
    relatedSpdxElement: str = ""
    relationshipType: str = ""


class _SbomDoc(BaseModel):
    """
    Minimal typed view of the SPDX document fields included in the semantic hash.

    Only ``name``, ``packages``, and ``relationships`` are extracted; all
    other top-level fields (``documentNamespace``, ``creationInfo``, etc.) are
    intentionally excluded (``extra="ignore"``) because they carry volatile
    metadata that must not affect the semantic hash.
    """

    model_config = ConfigDict(extra="ignore")

    name: str = ""
    packages: list[_SbomPackage] = []
    relationships: list[_SbomRelationship] = []


def compute_sbom_semantic_hash(raw: bytes) -> str:
    """
    Compute a stable semantic hash of an SPDX 2.3 SBOM document.

    The hash covers only the fields that reflect actual dependency changes:

    - ``sbom.name`` — repository identity (e.g. ``com.github.owner/repo``).
    - ``sbom.packages`` — sorted lexicographically by ``SPDXID``; captures
      every package's name, version, PURLs, and other attributes.
    - ``sbom.relationships`` — sorted by ``(spdxElementId,
      relatedSpdxElement, relationshipType)``; captures the full dependency
      graph structure.

    The following volatile fields are **excluded** from the hash:

    - ``sbom.documentNamespace`` — UUID regenerated on every submission.
    - ``sbom.creationInfo.created`` — ISO timestamp of the run.

    This means that re-submitting the same dependency set (common when CI
    runs are triggered without dependency changes) produces an identical hash,
    enabling deduplication in storage and the submission audit log.

    If the payload cannot be parsed as JSON, or the parsed value lacks a
    ``packages`` key, the function falls back to the raw SHA-256 of ``raw``
    so that structurally invalid payloads still receive a unique, deterministic
    identifier.

    Args:
        raw: Raw bytes of the SPDX 2.3 JSON document or GitHub API envelope.

    Returns:
        A SHA-256 hex digest of the canonical semantic content.
    """
    bare = _unwrap_github_api_envelope(raw)

    try:
        sbom = json.loads(bare)
    except json.JSONDecodeError, UnicodeDecodeError:
        return hashlib.sha256(raw).hexdigest()

    if not isinstance(sbom, dict) or "packages" not in sbom:
        return hashlib.sha256(raw).hexdigest()

    try:
        doc = _SbomDoc.model_validate(sbom)
    except Exception:
        return hashlib.sha256(raw).hexdigest()

    packages = sorted(doc.packages, key=lambda p: p.SPDXID)
    relationships = sorted(
        doc.relationships,
        key=lambda r: (r.spdxElementId, r.relatedSpdxElement, r.relationshipType),
    )
    canonical: dict[str, object] = {
        "name": doc.name,
        "packages": [p.model_dump(mode="json") for p in packages],
        "relationships": [r.model_dump(mode="json") for r in relationships],
    }
    canonical_bytes = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()

    return hashlib.sha256(canonical_bytes).hexdigest()
