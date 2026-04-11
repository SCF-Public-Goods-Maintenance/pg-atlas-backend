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
import logging
from dataclasses import dataclass
from operator import attrgetter
from typing import Any, cast

import msgspec
from spdx_tools.spdx.model import Document
from spdx_tools.spdx.parser.error import SPDXParsingError
from spdx_tools.spdx.parser.jsonlikedict.json_like_dict_parser import JsonLikeDictParser

logger = logging.getLogger(__name__)


_SBOM_DICT_DECODER = msgspec.json.Decoder(type=dict[str, object])
_DETERMINISTIC_ENCODER = msgspec.json.Encoder(order="deterministic")


def _empty_object_dict_list() -> list[dict[str, object]]:
    """Return a new empty list typed as ``list[dict[str, object]]``."""

    return []


class SpdxValidationError(ValueError):
    """
    Raised when an SBOM submission fails SPDX 2.3 schema validation.

    Carries a human-readable ``detail`` string and an optional list of
    ``messages`` from the underlying spdx-tools parser for structured error
    reporting back to the caller.

    When available, ``unwrapped_bytes`` contains the normalized SPDX JSON bytes
    (without the GitHub ``{"sbom": ...}`` envelope). This lets callers store
    canonicalized artifacts even when schema validation fails.
    """

    def __init__(
        self,
        detail: str,
        messages: list[str] | None = None,
        unwrapped_bytes: bytes | None = None,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.messages = messages or []
        self.unwrapped_bytes = unwrapped_bytes


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
        unwrapped_bytes: Canonical SPDX JSON bytes with no GitHub envelope.
            New artifacts should persist this representation.
        semantic_hash: Stable semantic hash of the canonical content used for
            deduplication in submission intake.
    """

    document: Document
    package_count: int
    unwrapped_bytes: bytes
    semantic_hash: str


class _SbomPackageKey(msgspec.Struct):
    """Sort key model for SPDX packages."""

    SPDXID: str = ""


class _SbomRelationshipKey(msgspec.Struct):
    """Sort key model for SPDX relationships."""

    spdxElementId: str = ""
    relatedSpdxElement: str = ""
    relationshipType: str = ""


class _SbomHashDoc(msgspec.Struct, forbid_unknown_fields=False):
    """
    Typed view of top-level fields used for semantic hash canonicalization.

    Unknown fields are ignored by design because volatile metadata such as
    ``documentNamespace`` and ``creationInfo.created`` must not influence the
    semantic hash.
    """

    name: str = ""
    packages: list[dict[str, object]] = msgspec.field(default_factory=_empty_object_dict_list)
    relationships: list[dict[str, object]] = msgspec.field(default_factory=_empty_object_dict_list)


@dataclass(frozen=True)
class _SortablePackage:
    """Raw package payload paired with a validated key for deterministic sort."""

    key: _SbomPackageKey
    payload: dict[str, object]


@dataclass(frozen=True)
class _SortableRelationship:
    """Raw relationship payload paired with a validated key for deterministic sort."""

    key: _SbomRelationshipKey
    payload: dict[str, object]


def _decode_unwrapped_sbom(raw: bytes) -> tuple[dict[str, object], bytes]:
    """
    Decode raw JSON bytes and unwrap GitHub's ``{"sbom": ...}`` envelope.

    Returns both the decoded dict and a deterministic JSON byte representation
    of that same unwrapped dict.
    """

    try:
        decoded = _SBOM_DICT_DECODER.decode(raw)
    except msgspec.DecodeError as exc:
        raise SpdxValidationError(
            detail="Invalid SPDX 2.3 document.",
            messages=[str(exc)],
        ) from exc

    sbom_obj: dict[str, object] = decoded
    envelope = decoded.get("sbom")
    if isinstance(envelope, dict):
        sbom_obj = cast(dict[str, object], envelope)

    unwrapped_bytes = _DETERMINISTIC_ENCODER.encode(sbom_obj)

    return sbom_obj, unwrapped_bytes


def _semantic_hash_from_sbom_dict(sbom_obj: dict[str, object], raw_fallback: bytes) -> str:
    """
    Build a stable semantic hash from decoded SPDX JSON.

    When required semantic fields are missing or structurally invalid, falls
    back to raw SHA-256 so invalid payloads still get deterministic IDs.
    """

    if "packages" not in sbom_obj:
        return hashlib.sha256(raw_fallback).hexdigest()

    try:
        doc = msgspec.convert(sbom_obj, type=_SbomHashDoc)
    except msgspec.ValidationError:
        return hashlib.sha256(raw_fallback).hexdigest()

    try:
        sortable_packages = [
            _SortablePackage(
                key=msgspec.convert(pkg, type=_SbomPackageKey),
                payload=pkg,
            )
            for pkg in doc.packages
        ]
        sortable_relationships = [
            _SortableRelationship(
                key=msgspec.convert(rel, type=_SbomRelationshipKey),
                payload=rel,
            )
            for rel in doc.relationships
        ]
    except msgspec.ValidationError:
        return hashlib.sha256(raw_fallback).hexdigest()

    sortable_packages.sort(key=attrgetter("key.SPDXID"))
    sortable_relationships.sort(
        key=attrgetter(
            "key.spdxElementId",
            "key.relatedSpdxElement",
            "key.relationshipType",
        )
    )

    canonical: dict[str, object] = {
        "name": doc.name,
        "packages": [pkg.payload for pkg in sortable_packages],
        "relationships": [rel.payload for rel in sortable_relationships],
    }
    canonical_bytes = _DETERMINISTIC_ENCODER.encode(canonical)

    return hashlib.sha256(canonical_bytes).hexdigest()


def parse_and_validate_spdx(raw: bytes) -> ParsedSbom:
    """
    Parse and validate a raw SPDX 2.3 JSON payload.

    Accepts either a bare SPDX 2.3 JSON document or the ``{"sbom": {…}}``
    envelope returned by the GitHub Dependency Graph API. The envelope is
    transparently stripped before parsing.

    This function is the single JSON decode point for raw SPDX bytes in the
    ingestion flow.

    Uses spdx-tools' ``JsonLikeDictParser`` for SPDX 2.3 validation and typed
    Document construction.

    Args:
        raw: Raw bytes of the SPDX 2.3 JSON document (or GitHub API envelope)
            submitted by the action.

    Returns:
        ParsedSbom: Parsed document, package count, canonical unwrapped bytes,
            and semantic hash.

    Raises:
        SpdxValidationError: If the bytes cannot be parsed as a valid SPDX 2.3
            JSON document, or if required fields (spdxVersion, SPDXID,
            documentNamespace, name) are missing or malformed.
    """

    sbom_obj, unwrapped_bytes = _decode_unwrapped_sbom(raw)
    # TODO: check if we always want a semantic hash when calling this
    semantic_hash = _semantic_hash_from_sbom_dict(sbom_obj, raw)
    parser_input = cast(dict[Any, Any], sbom_obj)
    parser: Any = JsonLikeDictParser()

    try:
        document = cast(Document, parser.parse(parser_input))
    except SPDXParsingError as exc:
        messages = [str(message) for message in exc.get_messages()]
        logger.info(f"SPDX validation failed: {messages}")
        raise SpdxValidationError(
            detail="Invalid SPDX 2.3 document.",
            messages=messages,
            unwrapped_bytes=unwrapped_bytes,
        ) from exc

    package_count = len(document.packages)
    logger.info(
        "SPDX document parsed OK: "
        f"name={document.creation_info.name!r} "
        f"spdx_version={document.creation_info.spdx_version} "
        f"packages={package_count}"
    )

    return ParsedSbom(
        document=document,
        package_count=package_count,
        unwrapped_bytes=unwrapped_bytes,
        semantic_hash=semantic_hash,
    )


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
    try:
        parsed = parse_and_validate_spdx(raw)
    except SpdxValidationError:
        return hashlib.sha256(raw).hexdigest()

    return parsed.semantic_hash
