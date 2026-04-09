"""
Tests for ``compute_sbom_semantic_hash``.

Verifies that the semantic hash is stable across re-submissions that only
differ in volatile metadata (``documentNamespace``, ``creationInfo.created``,
array ordering), while changing when the package set or repository identity
changes.

Fixtures are minimal but structurally realistic SPDX 2.3 documents derived
from real pg-atlas-backend and withObsrvr/nebu submissions. Each fixture is
a GitHub Dependency Graph API envelope (``{"sbom": {…}}``).

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pg_atlas.ingestion.spdx import compute_sbom_semantic_hash

FIXTURES = Path(__file__).parent / "data_fixtures"

# ---------------------------------------------------------------------------
# Load fixtures
# ---------------------------------------------------------------------------

# pg-atlas-backend with procrastinate 3.8.1 — two runs, different metadata
_RUN1 = (FIXTURES / "pg_atlas_backend_v381_run1.spdx.json").read_bytes()
_RUN2 = (FIXTURES / "pg_atlas_backend_v381_run2.spdx.json").read_bytes()

# pg-atlas-backend with procrastinate 3.7.2 — genuinely different dependency set
_OLD_DEPS = (FIXTURES / "pg_atlas_backend_v372.spdx.json").read_bytes()

# withObsrvr/nebu with the same dep versions as run1/run2 — different repo
_OTHER_REPO = (FIXTURES / "nebu_v381.spdx.json").read_bytes()


# ---------------------------------------------------------------------------
# Core deduplication properties
# ---------------------------------------------------------------------------


def test_identical_deps_different_namespace_same_hash() -> None:
    """
    Two submissions for the same repo with the same packages but a different
    ``documentNamespace`` UUID must produce the same semantic hash.

    This is the primary deduplication invariant: a CI job triggered on a
    branch push without any dependency changes must not create a new artifact.
    """
    assert compute_sbom_semantic_hash(_RUN1) == compute_sbom_semantic_hash(_RUN2)


def test_identical_deps_different_timestamp_same_hash() -> None:
    """
    ``creationInfo.created`` differs between run1 and run2 but the hash
    must remain equal — timestamps are volatile metadata, not semantic content.
    """
    sbom1 = json.loads(_RUN1)["sbom"]
    sbom2 = json.loads(_RUN2)["sbom"]
    assert sbom1["creationInfo"]["created"] != sbom2["creationInfo"]["created"]
    assert compute_sbom_semantic_hash(_RUN1) == compute_sbom_semantic_hash(_RUN2)


def test_different_package_version_different_hash() -> None:
    """
    A genuine dependency upgrade (procrastinate 3.7.2 → 3.8.1) must produce
    a different hash even though all other metadata fields are identical in
    structure.
    """
    assert compute_sbom_semantic_hash(_RUN1) != compute_sbom_semantic_hash(_OLD_DEPS)


def test_different_repository_same_deps_different_hash() -> None:
    """
    Two different repositories submitting SBOMs with identical package
    versions must NOT produce the same hash.

    ``sbom.name`` (which encodes the repository owner/name) is included in
    the canonical input to prevent cross-repo collisions.
    """
    assert compute_sbom_semantic_hash(_RUN1) != compute_sbom_semantic_hash(_OTHER_REPO)


# ---------------------------------------------------------------------------
# Ordering invariance
# ---------------------------------------------------------------------------


def test_package_array_order_does_not_affect_hash() -> None:
    """
    The packages array is sorted before hashing, so a submission with
    packages listed in a different order yields the same hash.

    run2's packages are in a different order than run1's (mirroring what
    the real GitHub API responses showed between r_173 and r_185).
    """
    sbom1 = json.loads(_RUN1)["sbom"]
    sbom2 = json.loads(_RUN2)["sbom"]
    spdx_ids_1 = [p["SPDXID"] for p in sbom1["packages"]]
    spdx_ids_2 = [p["SPDXID"] for p in sbom2["packages"]]
    assert spdx_ids_1 != spdx_ids_2, "Fixture precondition: packages must be in different order"
    assert compute_sbom_semantic_hash(_RUN1) == compute_sbom_semantic_hash(_RUN2)


def test_relationship_array_order_does_not_affect_hash() -> None:
    """
    The relationships array is sorted before hashing.
    """
    sbom1 = json.loads(_RUN1)["sbom"]
    sbom2 = json.loads(_RUN2)["sbom"]
    rel_keys_1 = [r["spdxElementId"] + r["relatedSpdxElement"] for r in sbom1["relationships"]]
    rel_keys_2 = [r["spdxElementId"] + r["relatedSpdxElement"] for r in sbom2["relationships"]]
    assert rel_keys_1 != rel_keys_2, "Fixture precondition: relationships must be in different order"
    assert compute_sbom_semantic_hash(_RUN1) == compute_sbom_semantic_hash(_RUN2)


# ---------------------------------------------------------------------------
# Envelope handling
# ---------------------------------------------------------------------------


def test_github_api_envelope_stripped_before_hashing() -> None:
    """
    Bare SPDX JSON (without the ``{"sbom": {…}}`` envelope) must produce the
    same semantic hash as the enveloped form.
    """
    sbom_inner = json.loads(_RUN1)["sbom"]
    bare_bytes = json.dumps(sbom_inner).encode()
    assert compute_sbom_semantic_hash(_RUN1) == compute_sbom_semantic_hash(bare_bytes)


# ---------------------------------------------------------------------------
# Fallback for structurally invalid payloads
# ---------------------------------------------------------------------------


def test_non_sbom_json_falls_back_to_raw_sha256() -> None:
    """
    A JSON payload that lacks a ``packages`` key is not an SBOM; the function
    must fall back to the raw SHA-256 of the bytes so that invalid submissions
    still receive a unique, deterministic identifier.
    """
    invalid = b'{"not": "an sbom"}'
    assert compute_sbom_semantic_hash(invalid) == hashlib.sha256(invalid).hexdigest()


def test_non_json_bytes_falls_back_to_raw_sha256() -> None:
    """
    Bytes that cannot be decoded as JSON must fall back to raw SHA-256
    without raising an exception.
    """
    garbage = b"\xff\xfe not json at all"
    assert compute_sbom_semantic_hash(garbage) == hashlib.sha256(garbage).hexdigest()
