"""
Download the deps.dev proto and update the local copy if it has changed.

This script uses ONLY stdlib imports so it can run in CI without
``uv sync`` — a bare ``python`` interpreter is sufficient.

Usage::

    python pg_atlas/deps_dev/sync_proto.py

Exit codes:
    0 — no change detected, or update succeeded.
    1 — network / I/O error.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import hashlib
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Final

PROTO_URL: Final[str] = "https://raw.githubusercontent.com/google/deps.dev/main/api/v3alpha/apiv3alpha.proto"
SCRIPT_PATH: Final[Path] = Path(__file__).resolve()
LOCAL_PATH: Final[Path] = SCRIPT_PATH.parent / "protos" / "api-v3alpha.proto"


def get_local_hash(path: Path) -> str | None:
    """Compute the SHA-256 hex digest of a local file, or ``None`` if it does not exist."""

    if not path.exists():
        return None

    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def sync() -> int:
    """
    Fetch the upstream proto, compare hashes, and overwrite the local copy when they differ.

    Returns 0 on success (including "no change"), 1 on error.
    """

    LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)

    # ── fetch remote content ──────────────────────────────────────────
    print(f"📡 Requesting: {PROTO_URL}")
    try:
        with urllib.request.urlopen(PROTO_URL, timeout=30) as response:
            if response.status != 200:
                print(f"❌ HTTP Error: {response.status}", file=sys.stderr)

                return 1

            remote_bytes: bytes = response.read()

    except (urllib.error.URLError, OSError) as exc:
        print(f"❌ Network failure: {exc}", file=sys.stderr)

        return 1

    # ── compare hashes ────────────────────────────────────────────────
    remote_hash = hashlib.sha256(remote_bytes).hexdigest()
    local_hash = get_local_hash(LOCAL_PATH)

    if remote_hash == local_hash:
        print(f"✅ No changes detected (SHA-256: {remote_hash[:12]}…).")

        return 0

    # ── write update ──────────────────────────────────────────────────
    if local_hash is None:
        print(f"⬇️  First download → {LOCAL_PATH.name}")
    else:
        print(f"⚠️  Hash mismatch — updating {LOCAL_PATH.name}…")
        print(f"    local : {local_hash[:12]}…")
        print(f"    remote: {remote_hash[:12]}…")

    try:
        LOCAL_PATH.write_bytes(remote_bytes)
    except OSError as exc:
        print(f"❌ I/O error: {exc}", file=sys.stderr)

        return 1

    print(f"✨ Synced to {remote_hash[:12]}…")

    return 0


if __name__ == "__main__":
    sys.exit(sync())
