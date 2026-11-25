"""Reconcile maintainer evidence for the local project override draft."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import msgspec
from mistletoe import Document
from mistletoe.block_token import Heading
from mistletoe.span_token import Link

_LOGIN = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
_TEAM_HEADINGS = {"team & experience", "team and experience", "team", "people", "maintainers", "project team"}
_GITHUB_SECTIONS = {
    "about",
    "apps",
    "collections",
    "contact",
    "events",
    "explore",
    "features",
    "issues",
    "login",
    "marketplace",
    "new",
    "notifications",
    "orgs",
    "pricing",
    "pulls",
    "search",
    "settings",
    "topics",
}


class Member(msgspec.Struct, forbid_unknown_fields=True):
    token_id: int
    github_login: str | None
    project_ids: list[str]
    revoked: bool
    ledger: int | None = None


class Snapshot(msgspec.Struct, forbid_unknown_fields=True):
    ledger: int
    first_token_id: int
    next_token_id: int
    members: list[Member]


def read_snapshot(path: Path) -> Snapshot:
    """Require a result for every token slot enumerated by the snapshot."""
    return validate_snapshot(msgspec.json.decode(path.read_bytes(), type=Snapshot))


def validate_snapshot(snapshot: Snapshot) -> Snapshot:
    token_ids = sorted(member.token_id for member in snapshot.members)
    if (
        snapshot.ledger < 1
        or not 0 <= snapshot.first_token_id <= snapshot.next_token_id
        or len(token_ids) != snapshot.next_token_id - snapshot.first_token_id
        or any(token_id != snapshot.first_token_id + offset for offset, token_id in enumerate(token_ids))
    ):
        raise ValueError("Stroopies snapshot is incomplete or has invalid ledger or token bounds")
    for member in snapshot.members:
        if member.github_login is not None and not _valid_login(member.github_login):
            raise ValueError(f"Invalid GitHub login for Stroopies token {member.token_id}")
    return snapshot


def _valid_login(login: str) -> bool:
    return len(login) <= 39 and _LOGIN.fullmatch(login) is not None


def _plain_text(token: Any) -> str:
    if token.children:
        return "".join(_plain_text(child) for child in token.children)
    return getattr(token, "content", "")


def _links(token: Any) -> Iterator[str]:
    if isinstance(token, Link):
        yield token.target
    for child in token.children or ():
        yield from _links(child)


def project_page_logins(markdown: str) -> list[str]:
    """Gather GitHub profile links only from recognized team sections."""
    logins: set[str] = set()
    section_level: int | None = None
    for token in Document(markdown).children or ():
        if isinstance(token, Heading):
            level = cast(int, token.level)
            if section_level is not None and level <= section_level:
                section_level = None
            if " ".join(_plain_text(token).lower().split()) in _TEAM_HEADINGS:
                section_level = level if section_level is None else section_level
            continue
        if section_level is None:
            continue
        for target in _links(token):
            try:
                url = urlsplit(target)
            except ValueError:
                continue
            login = url.path.strip("/").lower()
            if url.scheme == "https" and url.netloc.lower() in {"github.com", "www.github.com"}:
                if _valid_login(login) and login not in _GITHUB_SECTIONS:
                    logins.add(login)
    return sorted(logins)


def reconcile(
    project_id: str,
    page_evidence: dict[str, list[dict[str, Any]]],
    snapshot: Snapshot,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return active draft entries and extra page candidates for comment-only review."""
    matching: dict[str, list[dict[str, Any]]] = {}
    linked: dict[str, list[int]] = {}
    for member in snapshot.members:
        if member.revoked or not member.github_login:
            continue
        login = member.github_login.lower()
        linked.setdefault(login, []).append(member.token_id)
        if project_id in member.project_ids:
            matching.setdefault(login, []).append(
                {
                    "source": "stroopies",
                    "token_id": member.token_id,
                    "ledger": member.ledger if member.ledger is not None else snapshot.ledger,
                }
            )

    active: list[dict[str, Any]] = []
    commented: list[dict[str, Any]] = []
    for login in sorted(matching.keys() | page_evidence.keys()):
        pages = sorted(page_evidence.get(login, []), key=lambda item: (item["source"], item["path"], item["commit_sha"]))
        evidence = sorted(matching.get(login, []), key=lambda item: item["token_id"]) + pages
        entry = {"github_login": login, "evidence": evidence}
        if login in matching:
            active.append(entry)
        elif login in linked:
            entry["conflict"] = (
                f"Project source lists {login}, but Stroopies token(s) "
                f"{', '.join(map(str, sorted(linked[login])))} do not declare {project_id}."
            )
            active.append(entry)
        elif matching:
            commented.append(entry)
        else:
            active.append(entry)
    return active, commented


def evidence_key(item: dict[str, Any]) -> tuple[str, str | int]:
    if item["source"] == "stroopies":
        return item["source"], item["token_id"]
    return item["source"], item["path"]


def plan_review(
    previous_items: list[dict[str, Any]],
    active: list[dict[str, Any]],
    commented: list[dict[str, Any]],
    rechecked: set[tuple[str, str | int]],
    rejected: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Keep reviewed decisions and remove entries only when all evidence was rechecked."""
    previous = {entry["github_login"].lower(): entry for entry in previous_items}
    current = {entry["github_login"]: entry for entry in active if entry["github_login"] not in rejected}
    suggestions: list[dict[str, Any]] = []
    for entry in commented:
        login = entry["github_login"]
        if login in rejected:
            continue
        if login in previous:
            current[login] = entry
        else:
            suggestions.append(entry)

    removals: dict[str, str] = {}
    for login, old in previous.items():
        if login in rejected:
            removals[login] = "reviewer rejected this candidate"
            continue
        old_refs = {evidence_key(item): item for item in old["evidence"]}
        new = current.get(login)
        if new is None:
            if old_refs and old_refs.keys() <= rechecked:
                removals[login] = "the rechecked sources no longer declare this maintainer"
            else:
                current[login] = old
        elif "conflict" not in new:
            new_refs = {evidence_key(item): item for item in new["evidence"]}
            retained = {key: item for key, item in old_refs.items() if key in new_refs or key not in rechecked}
            current[login] = {
                "github_login": old["github_login"],
                "evidence": list((new_refs | retained).values()),
            }
    return [current[login] for login in sorted(current)], suggestions, removals
