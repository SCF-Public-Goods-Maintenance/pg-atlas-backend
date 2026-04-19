from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import msgspec
from pydantic_core import core_schema


class Release(msgspec.Struct, frozen=True):
    """One normalized release record keyed by package PURL + version."""

    purl: str
    version: str
    release_date: str

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

    Existing order is preserved; new keys append at the end.
    If the same key appears with an empty existing release_date and a non-empty
    incoming release_date, the incoming value replaces the existing one.
    """
    norm_existing = msgspec.convert(existing, list[Release]) if existing is not None else []
    norm_incoming = msgspec.convert(incoming, list[Release]) if incoming is not None else []

    if not norm_existing and not norm_incoming:
        return None

    # dict comprehension for speed; incoming replaces existing if key matches
    merged = {(r.purl, r.version): r for r in norm_existing}

    for r in norm_incoming:
        key = (r.purl, r.version)
        current = merged.get(key)

        # logic: update if new entry OR if current has no date and new one does
        if not current or (not current.release_date and r.release_date):
            merged[key] = r

    return list(merged.values())
