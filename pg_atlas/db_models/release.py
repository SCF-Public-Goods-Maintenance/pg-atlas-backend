from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import msgspec
from pydantic_core import core_schema

_SEMVER_RE = re.compile(r"^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
_COMMIT_HASH_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


class Release(msgspec.Struct, frozen=True):
    """One normalized release record keyed by package PURL + version."""

    purl: str
    version: str
    release_date: str = ""  # if populated, the value MUST be `isoformat`

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        """Provide Pydantic v2 validation/serialization support for FastAPI models."""
        return core_schema.with_info_plain_validator_function(
            cls._pydantic_validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                msgspec.to_builtins  # efficiently converts Struct -> dict/list
            ),
        )

    @classmethod
    def _pydantic_validate(cls, value: Any, _info: core_schema.ValidationInfo) -> Release:
        try:
            return msgspec.convert(value, cls)
        except (msgspec.ValidationError, TypeError) as e:
            raise ValueError(f"Invalid Release payload: {e}") from e

    @classmethod
    def __get_pydantic_json_schema__(cls, _core_schema: Any, _handler: Any) -> dict[str, Any]:
        """Return the resolved object schema so OpenAPI generation does not depend on $defs references."""

        schema_doc = msgspec.convert(msgspec.json.schema(cls), type=dict[str, Any])
        # unwrap the valid JSON schema to return what is under `Release`
        return next(iter(schema_doc["$defs"].values()))


def merge_releases(existing: Sequence[object] | None, incoming: Sequence[object] | None) -> list[Release] | None:
    """
    Union-merge releases using (purl, version) as the identity key.

    Merge identity is ``(purl, version)`` and the output is sorted in reverse
    chronological order by lexical compare of the datetime prefix
    ``YYYY-MM-DDTHH:MM:SS``.

    If the same key appears with an empty existing release_date and a non-empty
    incoming release_date, the incoming value replaces the existing one.
    """
    norm_existing = _normalize_release_list(existing)
    norm_incoming = _normalize_release_list(incoming)

    if not norm_existing and not norm_incoming:
        return None

    # Dict comprehension for speed; incoming replaces existing if key matches.
    merged = {(r.purl, r.version): r for r in norm_existing}

    for r in norm_incoming:
        key = (r.purl, r.version)
        current = merged.get(key)

        # logic: update if new entry OR if current has no date and new one does
        if not current or (not current.release_date and r.release_date):
            merged[key] = r

    return sorted_releases_desc([rel for rel in merged.values()])


def sorted_releases_desc(releases: Sequence[Release]) -> list[Release]:
    """Sort releases by lexical datetime prefix in reverse chronological order."""

    return sorted(releases, key=lambda release: _release_datetime_prefix(release.release_date), reverse=True)


def preferred_latest_version(releases: list[Release]) -> str:
    """
    Pick one latest-version candidate from the newest release window.

    Preference order in ``releases[:9]``:
    1. semver-like string
    2. commit-hash-like string
    3. first non-empty arbitrary string
    """

    newest_window = releases[:9]

    for release in newest_window:
        if _is_semver_like(release.version):
            return release.version

    for release in newest_window:
        if _is_commit_hash(release.version):
            return release.version

    for release in newest_window:
        if release.version:
            return release.version

    return ""


def _normalize_release_list(values: Sequence[object] | None) -> list[Release]:
    """Convert release-like payloads into ``Release`` structs, tolerating legacy null dates."""

    if values is None:
        return []

    normalized_values: list[object] = []
    for value in values:
        if isinstance(value, dict):
            normalized = msgspec.convert(value, type=dict[str, object])
            if normalized.get("release_date") is None:
                normalized["release_date"] = ""

            normalized_values.append(normalized)
            continue

        normalized_values.append(value)

    return msgspec.convert(normalized_values, list[Release])


def _is_semver_like(version: str) -> bool:
    """Return whether a version string resembles semver, optionally prefixed with ``v``."""

    return bool(_SEMVER_RE.fullmatch(version))


def _is_commit_hash(version: str) -> bool:
    """Return whether a version string looks like a hex git commit hash."""

    return bool(_COMMIT_HASH_RE.fullmatch(version))


def _release_datetime_prefix(release_date: str) -> str:
    """Return the stable datetime prefix used for lexical recency sorting."""

    return release_date[:19]
