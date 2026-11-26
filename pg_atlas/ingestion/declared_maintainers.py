"""

Extract team-section GitHub profiles and compare them with Stroopies records.
Keep human decisions visible in the local override draft and distinguish
unsupported suggestions from changes to previously reviewed declarations.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import re
from collections.abc import Container, Iterator, Sequence
from typing import Any
from urllib.parse import urlsplit

import msgspec
from mistletoe import Document
from mistletoe.block_token import Heading
from mistletoe.span_token import AutoLink, InlineCode, Link, RawText

_LOGIN = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
_TEAM_HEADINGS = {"team & experience", "team and experience"}
_PROFILE_URL = re.compile(r"https?://(?:www\.)?github\.com/[^\s<>()\[\]]+", re.IGNORECASE)
_MENTION = re.compile(r"(?<![\w@./-])@([A-Za-z0-9-]+)(?![\w@/-])")
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


def _profile_login(target: str) -> str | None:
    """Accept a GitHub user's profile URL and skip repositories or site routes."""
    try:
        url = urlsplit(target.rstrip(".,;:!?"))
    except ValueError:
        return None

    login = url.path.strip("/").lower()
    if (
        url.scheme in {"http", "https"}
        and url.netloc.lower() in {"github.com", "www.github.com"}
        and _valid_login(login)
        and login not in _GITHUB_SECTIONS
    ):
        return login

    return None


def _token_logins(token: Any) -> Iterator[str]:
    """Extract linked profiles, bare profile URLs and standalone @mentions."""
    if isinstance(token, (AutoLink, Link)):
        target = getattr(token, "target", None)
        if isinstance(target, str):
            login = _profile_login(target)
            if login:
                yield login

        return

    if isinstance(token, (RawText, InlineCode)):
        text = _plain_text(token)
        for match in _PROFILE_URL.finditer(text):
            login = _profile_login(match[0])
            if login:
                yield login

        for match in _MENTION.finditer(_PROFILE_URL.sub(" ", text)):
            login = match[1].lower()
            if _valid_login(login):
                yield login

        return

    for child in token.children or ():
        yield from _token_logins(child)


def project_page_logins(markdown: str) -> list[str]:
    """Read only the H2 Team & Experience section, including its subsections."""
    logins: set[str] = set()
    in_team = False
    for token in Document(markdown).children or ():
        if isinstance(token, Heading):
            level = getattr(token, "level", None)
            if isinstance(level, int) and level <= 2:
                in_team = level == 2 and " ".join(_plain_text(token).lower().split()) in _TEAM_HEADINGS

            continue

        if in_team:
            logins.update(_token_logins(token))

    return sorted(logins)


def reconcile(
    project_id: str,
    page_evidence: dict[str, list[dict[str, Any]]],
    snapshot: Snapshot,
    *,
    previous_items: Sequence[dict[str, Any]] = (),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Draft entries using member reads at least as recent as the approved evidence."""
    approved_claims: dict[int, tuple[str, dict[str, Any]]] = {}
    for entry in previous_items:
        for evidence in entry.get("evidence", []):
            if evidence["source"] != "stroopies":
                continue

            token_id = evidence["token_id"]
            existing = approved_claims.get(token_id)
            if existing is None or evidence["ledger"] > existing[1]["ledger"]:
                approved_claims[token_id] = (entry["github_login"].lower(), evidence)

    matching: dict[str, list[dict[str, Any]]] = {}
    linked: dict[str, list[int]] = {}
    for member in snapshot.members:
        ledger = member.ledger if member.ledger is not None else snapshot.ledger
        approved = approved_claims.get(member.token_id)
        if approved is not None and ledger < approved[1]["ledger"]:
            login, evidence = approved
            matching.setdefault(login, []).append(evidence)
            continue

        if member.revoked or not member.github_login:
            continue

        login = member.github_login.lower()
        linked.setdefault(login, []).append(member.token_id)
        if project_id in member.project_ids:
            matching.setdefault(login, []).append(
                {
                    "source": "stroopies",
                    "token_id": member.token_id,
                    "ledger": ledger,
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
    rechecked: Container[tuple[str, str | int]],
    rejected: set[str],
    *,
    token_ledgers: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Preserve manual entries and recheck sourced entries against current evidence."""
    ledgers = token_ledgers or {}
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

        old_refs = {evidence_key(item): item for item in old.get("evidence", [])}
        new = current.get(login)
        if not old_refs and (new is None or "conflict" not in new):
            current[login] = old
            continue

        checked = {
            key
            for key, item in old_refs.items()
            if (ledgers.get(item["token_id"], 0) >= item["ledger"] if item["source"] == "stroopies" else key in rechecked)
        }
        if new is None:
            if old_refs.keys() <= checked:
                removals[login] = "the rechecked sources no longer declare this maintainer"
            else:
                current[login] = old
        elif "conflict" not in new:
            new_refs = {evidence_key(item): item for item in new["evidence"]}
            retained = {key: item for key, item in old_refs.items() if key in new_refs or key not in checked}
            current[login] = {
                "github_login": old["github_login"],
                "evidence": list((new_refs | retained).values()),
            }

    return [current[login] for login in sorted(current)], suggestions, removals
