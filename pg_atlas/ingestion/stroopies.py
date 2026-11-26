"""

Read deployed Stroopies membership records with unsigned Stellar simulations.
Store each completed batch separately and reuse its ledger-stamped evidence on
later local runs. Publish a complete refresh atomically before using it.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, cast

from stellar_sdk import Account, Network, SorobanServer, StrKey, TransactionBuilder, scval, xdr
from stellar_sdk.client.requests_client import RequestsClient
from tqdm import tqdm

from pg_atlas.ingestion.declared_maintainers import Member, Snapshot, validate_snapshot

CONTRACT_ID = "CDC5ZX53PWDPVS5AGZPHJ6WHFPB66AZFFEHK4NCFH2AR7ZZQUKOGJFGB"
MAINNET_RPC = "https://rpc.lightsail.network"
_PUBLIC_SOURCE = StrKey.encode_ed25519_public_key(bytes(32))


def _simulate(rpc_url: str, method: str, token_id: int | None = None) -> dict[str, Any]:
    params = [] if token_id is None else [scval.to_uint32(token_id)]
    envelope = (
        TransactionBuilder(Account(_PUBLIC_SOURCE, 0), Network.PUBLIC_NETWORK_PASSPHRASE)
        .append_invoke_contract_function_op(CONTRACT_ID, method, params)
        .set_timeout(30)
        .build()
    )
    with SorobanServer(rpc_url, client=RequestsClient(num_retries=1, post_timeout=15)) as rpc:
        response = rpc.simulate_transaction(envelope)

    if response.error or not response.results or len(response.results) != 1 or response.latest_ledger < 1:
        raise ValueError(f"Could not read Stroopies {method} for token {token_id}: {response.error or 'missing result'}")

    return {"ledger": response.latest_ledger, "xdr": response.results[0].xdr}


def _record(raw: dict[str, Any]) -> dict[str, Any]:
    if type(raw["ledger"]) is not int or raw["ledger"] < 1:
        raise ValueError("Stroopies response has no valid ledger")

    record = scval.to_native(xdr.SCVal.from_xdr(raw["xdr"]))
    if not isinstance(record, dict):
        raise ValueError("Expected a Stroopies contract record")

    return record


def _member(token_id: int, raw: dict[str, Any]) -> Member:
    record = _record(raw)
    github = [account for account in record["external_accounts"]["accounts"] if account["provider"] == 1]
    projects: Any = record["projects"]
    revoked = record["revoked"]
    if (
        len(github) > 1
        or type(revoked) is not bool
        or not isinstance(projects, list)
        or any(not isinstance(project, str) for project in cast(list[object], projects))
    ):
        raise ValueError(f"Malformed Stroopies member {token_id}")

    login = github[0]["handle"] if github else None
    if github and (not isinstance(login, str) or not login):
        raise ValueError(f"Missing GitHub handle on Stroopies member {token_id}")

    return Member(token_id, login, cast(list[str], projects), revoked, ledger=raw["ledger"])


def _token_count(settings: dict[str, Any]) -> int:
    """Read and validate the next unused token ID from a settings response."""
    count = _record(settings)["next_token_id"]
    if type(count) is not int or count < 0:
        raise ValueError("Invalid Stroopies next_token_id")

    return count


def _decode_batch(batch: dict[str, Any]) -> list[Member]:
    """Decode a complete batch and verify that each member has ledger provenance."""
    if batch["contract_id"] != CONTRACT_ID:
        raise ValueError("Stroopies cache refers to a different contract")

    before, after = batch["settings_before"], batch["settings_after"]
    count = _token_count(before)
    _token_count(after)
    start = batch["start_token_id"]
    records = batch["members"]
    if type(start) is not int or not 0 <= start <= count or len(records) != count - start:
        raise ValueError("Stroopies cache contains an incomplete batch")

    members = [_member(token_id, raw) for token_id, raw in enumerate(records, start)]
    if after["ledger"] < before["ledger"] or any(
        member.ledger is None or not before["ledger"] <= member.ledger <= after["ledger"] for member in members
    ):
        raise ValueError("Stroopies member reads fall outside the recorded ledger range")

    return members


def _load_members(directory: Path) -> list[Member]:
    """Load contiguous batches starting at the most recent full snapshot."""
    batches: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json"), key=lambda path: int(path.stem), reverse=True):
        batch = json.loads(path.read_bytes())
        if batch["settings_before"]["ledger"] != int(path.stem):
            raise ValueError(f"Stroopies cache ledger does not match {path.name}")

        batches.append(batch)
        if batch["start_token_id"] == 0:
            break

    members: list[Member] = []
    for batch in reversed(batches):
        if batch["start_token_id"] != len(members):
            raise ValueError("Stroopies cache has a gap or overlapping batches")

        members.extend(_decode_batch(batch))

    return members


def _write_batch(directory: Path, batch: dict[str, Any]) -> None:
    """Publish a validated batch atomically, leaving existing files intact on failure."""
    directory.mkdir(parents=True, exist_ok=True)
    ledger = batch["settings_before"]["ledger"]
    with NamedTemporaryFile(dir=directory, delete=False) as temporary:
        temp_path = Path(temporary.name)
        try:
            temporary.write(json.dumps(batch).encode("utf-8"))
            temporary.flush()
            temp_path.replace(directory / f"{ledger}.json")
        finally:
            temp_path.unlink(missing_ok=True)


def cached_snapshot(directory: Path, rpc_url: str, *, refresh: bool) -> Snapshot:
    """Use cached tokens and simulate only new IDs, or replace the effective cache."""
    with SorobanServer(rpc_url, client=RequestsClient(num_retries=1, post_timeout=15)) as rpc:
        if rpc.get_network().passphrase != Network.PUBLIC_NETWORK_PASSPHRASE:
            raise ValueError("Stroopies RPC must be on Stellar mainnet")

    before = _simulate(rpc_url, "settings")
    count = _token_count(before)
    cached = [] if refresh else _load_members(directory)
    start = len(cached)
    if start > count:
        raise ValueError("Stroopies RPC returned fewer token IDs than the local cache")

    if not refresh and start == count:
        return validate_snapshot(Snapshot(before["ledger"], 0, count, cached))

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = pool.map(partial(_simulate, rpc_url, "member"), range(start, count))
        members = list(tqdm(results, total=count - start, desc="Stroopies members"))

    after = _simulate(rpc_url, "settings")
    if _token_count(after) != count:
        print(
            f"WARNING Stroopies token range changed from {count} to {_token_count(after)}; "
            f"cached IDs below {count} remain available for the next run.",
            file=sys.stderr,
        )

    batch = {
        "contract_id": CONTRACT_ID,
        "start_token_id": start,
        "settings_before": before,
        "settings_after": after,
        "members": members,
    }
    snapshot = validate_snapshot(Snapshot(after["ledger"], 0, count, cached + _decode_batch(batch)))
    _write_batch(directory, batch)
    if refresh:
        for path in directory.glob("*.json"):
            if path.stem.isdecimal() and int(path.stem) < before["ledger"]:
                path.unlink(missing_ok=True)

    return snapshot
