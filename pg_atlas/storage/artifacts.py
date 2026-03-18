"""
Raw artifact storage for PG Atlas.

Provides a ``store_artifact`` function that persists raw bytes (e.g. a submitted SBOM
payload) to a backing store and returns a content-addressed reference path.

**Local development**: bytes are written to a filesystem directory configured via
``PG_ATLAS_ARTIFACT_STORE_PATH``. Docker Compose mounts this directory from the host
so artifacts survive container restarts.

**Production**: replace this module with a Storacha (web3.storage) backend that returns
a CID.  The ``SbomSubmission.artifact_path`` column is wide enough (1 024 chars) to hold
either a relative filesystem path or a ``bafy…`` CID string without schema changes.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

from pg_atlas.config import settings

logger = logging.getLogger(__name__)


def _compute_sha256(data: bytes) -> str:
    """Return the SHA-256 hex digest of ``data``."""
    return hashlib.sha256(data).hexdigest()


def _write_sync(dest: Path, data: bytes) -> None:
    """Synchronous file write — intended to run in a thread-pool executor."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary name then rename so concurrent readers never see a partial file.
    tmp = dest.with_suffix(".tmp")
    tmp.write_bytes(data)
    tmp.rename(dest)


async def store_artifact(data: bytes, filename: str) -> tuple[str, str]:
    """
    Persist ``data`` to the configured artifact store and return its reference path
    and SHA-256 hex digest.

    The file is placed at ``<ARTIFACT_STORE_PATH>/<filename>`` using an atomic write
    (write to ``<filename>.tmp`` then rename).  If a file with the same name already
    exists it is silently overwritten — callers should include the content hash in the
    filename to make writes idempotent.

    Args:
        data: Raw bytes to store (e.g. the full SBOM JSON payload).
        filename: Relative filename within the artifact store root
            (e.g. ``"sboms/sha256:<hex>.json"``).

    Returns:
        A ``(artifact_path, content_hash_hex)`` tuple where ``artifact_path`` is a
        string suitable for storage in ``SbomSubmission.artifact_path``.

    Raises:
        OSError: If the artifact store directory cannot be created or written to.
    """
    root: Path = settings.ARTIFACT_STORE_PATH
    dest = root / filename
    content_hex = _compute_sha256(data)

    # Offload the blocking I/O to a thread-pool executor so the event loop is not blocked.
    await asyncio.get_running_loop().run_in_executor(None, _write_sync, dest, data)

    logger.debug(f"Stored artifact {dest} (sha256={content_hex})")
    return str(filename), content_hex
