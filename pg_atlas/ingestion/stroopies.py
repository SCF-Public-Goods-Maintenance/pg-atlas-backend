"""Read-only Stroopies simulations and their explicitly refreshed local cache."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, cast

from stellar_sdk import Account, Network, SorobanServer, StrKey, TransactionBuilder, scval, xdr
from stellar_sdk.client.requests_client import RequestsClient

from pg_atlas.ingestion.declared_maintainers import Member, Snapshot, validate_snapshot

CONTRACT_ID = "CDC5ZX53PWDPVS5AGZPHJ6WHFPB66AZFFEHK4NCFH2AR7ZZQUKOGJFGB"
MAINNET_RPC = "https://rpc.lightsail.network"
_PUBLIC_SOURCE = StrKey.encode_ed25519_public_key(bytes(32))
_CACHE_MAX_AGE = timedelta(days=1)


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


def _decode_cache(cache: dict[str, Any]) -> Snapshot:
    if cache["contract_id"] != CONTRACT_ID:
        raise ValueError("Stroopies cache refers to a different contract")
    read_at = datetime.fromisoformat(cache["read_at"])
    if not timedelta() <= datetime.now(UTC) - read_at <= _CACHE_MAX_AGE:
        raise ValueError("Stroopies cache is stale; rerun with --refresh-stroopies")

    first = _record(cache["settings_before"])["next_token_id"]
    last = _record(cache["settings_after"])["next_token_id"]
    records = cache["members"]
    if type(first) is not int or first < 0 or type(last) is not int or first != last or len(records) != last:
        raise ValueError("Stroopies token range changed or the read was incomplete; retry the refresh")
    start = cache["settings_before"]["ledger"]
    end = cache["settings_after"]["ledger"]
    members = [_member(token_id, raw) for token_id, raw in enumerate(records)]
    if end < start or any(member.ledger is None or not start <= member.ledger <= end for member in members):
        raise ValueError("Stroopies member reads fall outside the recorded ledger range")
    return validate_snapshot(Snapshot(end, 0, last, members))


def cached_snapshot(path: Path, rpc_url: str, *, refresh: bool) -> Snapshot:
    if not refresh:
        if not path.exists():
            raise ValueError("No Stroopies cache; run with --refresh-stroopies first")
        return _decode_cache(json.loads(path.read_bytes()))

    with SorobanServer(rpc_url, client=RequestsClient(num_retries=1, post_timeout=15)) as rpc:
        if rpc.get_network().passphrase != Network.PUBLIC_NETWORK_PASSPHRASE:
            raise ValueError("Stroopies RPC must be on Stellar mainnet")

    read_at = datetime.now(UTC).isoformat()
    before = _simulate(rpc_url, "settings")
    count = _record(before)["next_token_id"]
    if type(count) is not int or count < 0:
        raise ValueError("Invalid Stroopies next_token_id")
    with ThreadPoolExecutor(max_workers=4) as pool:
        members = list(pool.map(partial(_simulate, rpc_url, "member"), range(count)))
    cache = {
        "contract_id": CONTRACT_ID,
        "read_at": read_at,
        "settings_before": before,
        "settings_after": _simulate(rpc_url, "settings"),
        "members": members,
    }
    snapshot = _decode_cache(cache)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary.write(json.dumps(cache).encode("utf-8"))
        temp_path = Path(temporary.name)
    try:
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)
    return snapshot
