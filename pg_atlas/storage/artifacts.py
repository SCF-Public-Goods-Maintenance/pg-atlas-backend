"""
Raw artifact storage for PG Atlas.

Provides shared helpers for writing and reading raw artifacts such as submitted
SBOM payloads.

**Local development**: bytes are written to a filesystem directory configured via
``PG_ATLAS_ARTIFACT_STORE_PATH``. Docker Compose mounts this directory from the host
so artifacts survive container restarts.

**Production**: when ``PG_ATLAS_ARTIFACT_S3_ENDPOINT`` is configured, bytes are
uploaded to Filebase's S3-compatible API and the returned CID is stored in the
database. Artifact reads then resolve that CID through Filebase's IPFS gateway.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from aiobotocore.config import AioConfig
from aiobotocore.session import ClientCreatorContext, get_session

from pg_atlas.config import settings

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

logger = logging.getLogger(__name__)

_FILEBASE_REGION = "us-east-1"
_FILEBASE_GATEWAY_BASE_URL = "https://ipfs.filebase.io/ipfs"
_FILEBASE_S3_TIMEOUT = AioConfig(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2})


def _compute_sha256(data: bytes) -> str:
    """Return the SHA-256 hex digest of ``data``."""

    return hashlib.sha256(data).hexdigest()


def _local_artifact_path(filename: str) -> Path:
    """Return the absolute path for one local artifact-store-relative filename."""

    return settings.ARTIFACT_STORE_PATH / filename


def _write_sync(dest: Path, data: bytes) -> None:
    """Synchronous file write — intended to run in a thread-pool executor."""

    dest.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary name then rename so concurrent readers never see a partial file.
    tmp = dest.with_suffix(".tmp")
    tmp.write_bytes(data)
    tmp.rename(dest)


def _read_sync(path: Path) -> bytes:
    """Synchronous file read — intended to run in a thread-pool executor."""

    return path.read_bytes()


def _filebase_enabled() -> bool:
    """Return whether Filebase artifact storage is configured."""

    return settings.ARTIFACT_S3_ENDPOINT is not None


def _get_filebase_s3_client() -> ClientCreatorContext[S3Client]:
    """Return a Filebase S3 client context manager."""

    endpoint = settings.ARTIFACT_S3_ENDPOINT
    access_key = settings.FILEBASE_ACCESS_KEY
    secret_key = settings.FILEBASE_SECRET_KEY
    if endpoint is None or not access_key or not secret_key:
        raise ValueError("Filebase artifact storage is not fully configured.")

    session = get_session()

    return session.create_client(
        "s3",
        region_name=_FILEBASE_REGION,
        endpoint_url=str(endpoint),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=_FILEBASE_S3_TIMEOUT,
    )


async def _store_artifact_local(data: bytes, filename: str, content_hex: str) -> tuple[str, str]:
    """Persist bytes to the local filesystem artifact store."""

    dest = _local_artifact_path(filename)
    await asyncio.get_running_loop().run_in_executor(None, _write_sync, dest, data)

    logger.debug(f"Stored artifact {dest} (sha256={content_hex})")
    return filename, content_hex


async def _store_artifact_filebase(data: bytes, filename: str, content_hex: str) -> tuple[str, str]:
    """Persist bytes to Filebase and return the CID as the stored artifact path."""

    client_context = _get_filebase_s3_client()

    try:
        async with client_context as client:
            response = await client.put_object(
                Bucket=settings.ARTIFACT_S3_BUCKET,
                Key=filename,
                Body=data,
            )
    except Exception:
        logger.warning(f"Filebase artifact upload failed: bucket={settings.ARTIFACT_S3_BUCKET} key={filename}")

        raise

    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    cid = headers.get("x-amz-meta-cid")
    if not cid:
        logger.warning(
            f"Filebase artifact upload response did not include a CID: bucket={settings.ARTIFACT_S3_BUCKET} key={filename}"
        )
        raise RuntimeError("Filebase artifact upload response did not include a CID.")

    logger.debug(
        f"Stored artifact in Filebase bucket={settings.ARTIFACT_S3_BUCKET} key={filename} cid={cid} sha256={content_hex}"
    )

    return cid, content_hex


async def _read_artifact_local(artifact_path: str) -> bytes:
    """Read artifact bytes from the local filesystem store without blocking the loop."""

    return await asyncio.get_running_loop().run_in_executor(None, _read_sync, _local_artifact_path(artifact_path))


async def _read_artifact_filebase(artifact_path: str) -> bytes:
    """Read artifact bytes from the Filebase IPFS gateway using the stored CID."""

    url = f"{_FILEBASE_GATEWAY_BASE_URL}/{artifact_path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)

    if response.status_code == 404:
        raise FileNotFoundError(f"Filebase artifact not found: {artifact_path}")

    if response.status_code >= 400:
        logger.warning(f"Filebase artifact gateway error: cid={artifact_path} status={response.status_code}")
        raise OSError(f"Filebase artifact gateway returned HTTP {response.status_code} for {artifact_path}")

    return response.content


async def store_artifact(data: bytes, filename: str) -> tuple[str, str]:
    """
    Persist ``data`` to the configured artifact store and return its reference path
    and SHA-256 hex digest.

    In local mode, ``filename`` is treated as a path relative to
    ``ARTIFACT_STORE_PATH`` and written atomically via ``<filename>.tmp`` then
    rename. In Filebase mode, the same key is uploaded through the S3-compatible
    API and the returned CID becomes the stored artifact path.

    Args:
        data: Raw bytes to store (e.g. the full SBOM JSON payload).
        filename: Relative filename within the artifact store root
            (e.g. ``"sboms/sha256:<hex>.json"``).

    Returns:
        A ``(artifact_path, content_hash_hex)`` tuple where ``artifact_path`` is a
        string suitable for storage in ``SbomSubmission.artifact_path``.
    """

    content_hex = _compute_sha256(data)
    if _filebase_enabled():
        return await _store_artifact_filebase(data, filename, content_hex)

    return await _store_artifact_local(data, filename, content_hex)


async def read_artifact(artifact_path: str) -> bytes:
    """
    Read one stored artifact from the configured backing store.

    When Filebase storage is enabled, ``artifact_path`` is interpreted as the
    persisted CID and resolved through Filebase's IPFS gateway. Otherwise it is
    interpreted as a filesystem-relative artifact path under
    ``ARTIFACT_STORE_PATH``.
    """

    if _filebase_enabled():
        return await _read_artifact_filebase(artifact_path)

    return await _read_artifact_local(artifact_path)
