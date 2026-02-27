"""
SPDX 2.3 document parsing and validation for PG Atlas SBOM ingestion.

SBOMs submitted by the pg-atlas-sbom-action are SPDX 2.3 JSON documents
fetched from the GitHub Dependency Graph API. This module validates them and
extracts the package list for downstream processing.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass

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


def parse_and_validate_spdx(raw: bytes) -> ParsedSbom:
    """
    Parse and validate a raw SPDX 2.3 JSON payload.

    Uses spdx-tools' ``parse_file`` which validates the document against the
    SPDX 2.3 JSON schema and returns a typed Document object. The document
    is not persisted here — that is the responsibility of the A8 processing
    pipeline.

    Args:
        raw: Raw bytes of the SPDX 2.3 JSON document submitted by the action.

    Returns:
        ParsedSbom: Parsed document and package count on success.

    Raises:
        SpdxValidationError: If the bytes cannot be parsed as a valid SPDX 2.3
            JSON document, or if required fields (spdxVersion, SPDXID,
            documentNamespace, name) are missing or malformed.
    """
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
        logger.info("SPDX validation failed: %s", messages)
        raise SpdxValidationError(
            detail="Invalid SPDX 2.3 document.",
            messages=messages,
        ) from exc
    except Exception as exc:
        logger.warning("Unexpected error during SPDX parsing: %s", exc)
        raise SpdxValidationError(
            detail=f"Could not parse SPDX document: {exc}",
        ) from exc

    assert document, "Document is None"
    package_count = len(document.packages)
    logger.info(
        "SPDX document parsed OK: name=%r spdx_version=%s packages=%d",
        document.creation_info.name,
        document.creation_info.spdx_version,
        package_count,
    )
    return ParsedSbom(document=document, package_count=package_count)
