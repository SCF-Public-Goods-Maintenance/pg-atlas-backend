"""
Tests for artifact storage configuration and backend selection.

These tests focus on the new production-safe artifact storage seam:
  - Filebase settings must be complete when S3 mode is enabled
  - local filesystem storage remains the fallback when S3 mode is disabled

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from pg_atlas.config import Settings
from pg_atlas.storage.artifacts import store_artifact


def test_settings_require_filebase_credentials_when_endpoint_is_set() -> None:
    """
    Filebase mode must not activate without access credentials.
    """

    with pytest.raises(ValidationError):
        Settings(
            API_URL="https://test.pg-atlas.example",
            ARTIFACT_S3_ENDPOINT="https://s3.filebase.com",
            FILEBASE_ACCESS_KEY=None,
            FILEBASE_SECRET_KEY=None,
        )


async def test_store_artifact_falls_back_to_local_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Local artifact storage remains the default when Filebase is not configured.
    """

    from pg_atlas.config import settings

    raw_body = b'{"spdxVersion": "SPDX-2.3"}'
    filename = "sboms/local-test.spdx.json"

    monkeypatch.setattr(settings, "ARTIFACT_STORE_PATH", tmp_path)
    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", None)

    artifact_path, content_hash = await store_artifact(raw_body, filename)

    assert artifact_path == filename
    assert content_hash == hashlib.sha256(raw_body).hexdigest()
    assert (tmp_path / filename).read_bytes() == raw_body


async def test_store_artifact_uses_filebase_cid_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Filebase-backed storage stores the returned CID as artifact_path.
    """

    from pg_atlas.config import settings

    class _FakeClient:
        def __init__(self) -> None:
            self.put_calls: list[dict[str, Any]] = []

        async def put_object(self, **kwargs: Any) -> dict[str, Any]:
            self.put_calls.append(kwargs)
            return {
                "ResponseMetadata": {
                    "HTTPHeaders": {
                        "x-amz-meta-cid": "QmTestCidForStoreArtifact",
                    }
                }
            }

    class _FakeClientContext:
        def __init__(self, client: _FakeClient) -> None:
            self.client = client

        async def __aenter__(self) -> _FakeClient:
            return self.client

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

    class _FakeSession:
        def __init__(self, client: _FakeClient) -> None:
            self.client = client

        def create_client(self, *_args: Any, **_kwargs: Any) -> _FakeClientContext:
            return _FakeClientContext(self.client)

    raw_body = b'{"spdxVersion":"SPDX-2.3","name":"remote"}'
    filename = "sboms/remote-test.spdx.json"
    fake_client = _FakeClient()

    monkeypatch.setattr(settings, "ARTIFACT_S3_ENDPOINT", "https://s3.filebase.com")
    monkeypatch.setattr(settings, "ARTIFACT_S3_BUCKET", "test-bucket")
    monkeypatch.setattr(settings, "FILEBASE_ACCESS_KEY", "test-access")
    monkeypatch.setattr(settings, "FILEBASE_SECRET_KEY", "test-secret")
    monkeypatch.setattr("pg_atlas.storage.artifacts.get_session", lambda: _FakeSession(fake_client))

    artifact_path, content_hash = await store_artifact(raw_body, filename)

    assert artifact_path == "QmTestCidForStoreArtifact"
    assert content_hash == hashlib.sha256(raw_body).hexdigest()
    assert fake_client.put_calls == [
        {
            "Bucket": "test-bucket",
            "Key": filename,
            "Body": raw_body,
        }
    ]
