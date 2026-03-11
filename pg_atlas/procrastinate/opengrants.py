"""
Async HTTP client for the OpenGrants API (grants.daostar.org).

Fetches SCF grant pools and applications, then transforms them into
``ScfProject`` instances suitable for upserting into the PG Atlas database.
Handles pagination (``limit`` / ``offset`` with ``pagination.hasNext``),
rate-limit back-off, and per-project deduplication across rounds.

API reference:
    https://github.com/metagov/Grants-Gateway-API/blob/main/server/routes.ts

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

from pg_atlas.config import settings
from pg_atlas.db_models.base import ActivityStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://grants.daostar.org/api/v1"
PAGE_SIZE = 100

#: Maximum number of retries for transient HTTP errors (429 / 5xx).
MAX_RETRIES = 4

#: Initial back-off delay (seconds) — doubles on each retry.
INITIAL_BACKOFF_S = 2.0

#: Regex for validating and parsing GitHub URLs.
#: Captures the owner (group 1) and optional repo name (group 2).
_GITHUB_URL_RE = re.compile(
    r"^https?://github\.com/([A-Za-z0-9\-_.]+)(?:/([A-Za-z0-9\-_.]+?))?(?:\.git)?/?$",
)


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class ScfProject:
    """
    Intermediate representation of an SCF project extracted from OpenGrants.

    Fields correspond 1-to-1 with the columns needed for a ``Project`` upsert.
    ``git_repo_url`` is not stored on ``Project`` but is passed to the
    downstream ``process_project`` task so it can target deps.dev lookups.
    """

    canonical_id: str
    display_name: str
    activity_status: ActivityStatus
    git_org_url: str | None
    git_repo_url: str | None
    project_metadata: dict[str, Any] = field(default_factory=lambda: {})


# ---------------------------------------------------------------------------
# GitHub URL helpers
# ---------------------------------------------------------------------------


def parse_github_url(raw_url: str) -> tuple[str | None, str | None]:
    """
    Validate and parse a GitHub URL into ``(org_url, repo_url)``.

    Returns:
        A 2-tuple:

        - If the URL points to a specific repo (``https://github.com/org/repo``),
          returns ``("https://github.com/org", "https://github.com/org/repo")``.
        - If the URL is org-level only (``https://github.com/org``),
          returns ``("https://github.com/org", None)``.
        - If the URL is not a valid GitHub URL, returns ``(None, None)``.
    """
    url = raw_url.strip().rstrip("/")

    match = _GITHUB_URL_RE.match(url)
    if not match:
        return None, None

    owner = match.group(1)
    repo = match.group(2)
    org_url = f"https://github.com/{owner}"

    if repo:
        return org_url, f"https://github.com/{owner}/{repo}"

    return org_url, None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _auth_headers() -> dict[str, str]:
    """Return authorization headers if an OpenGrants API key is configured."""

    if settings.OPENGRANTS_KEY:
        return {"Authorization": f"Bearer {settings.OPENGRANTS_KEY}"}

    return {}


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, str | int],
) -> httpx.Response:
    """
    Perform a GET request with exponential back-off on transient errors.

    Retries on HTTP 429 (rate-limited) and 5xx (server error) up to
    ``MAX_RETRIES`` times.  Uses the ``Retry-After`` header when present,
    otherwise doubles the delay starting from ``INITIAL_BACKOFF_S``.

    Raises:
        httpx.HTTPStatusError: After all retries are exhausted, or on any
            non-retryable HTTP error (4xx other than 429).
    """
    backoff = INITIAL_BACKOFF_S

    for attempt in range(1, MAX_RETRIES + 1):
        response = await client.get(url, params=params, headers=_auth_headers())

        if response.status_code < 400:
            return response

        retryable = response.status_code == 429 or response.status_code >= 500

        if not retryable or attempt == MAX_RETRIES:
            response.raise_for_status()

        # Honour Retry-After if the server provides it.
        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after and retry_after.isdigit() else backoff

        logger.warning(
            "HTTP %d from %s (attempt %d/%d) — retrying in %.1fs",
            response.status_code,
            url,
            attempt,
            MAX_RETRIES,
            delay,
        )
        await asyncio.sleep(delay)
        backoff *= 2

    # Unreachable, but satisfies the type checker.
    raise RuntimeError("retry loop exited unexpectedly")  # pragma: no cover


async def _paginated_get(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, str | int],
) -> list[dict[str, Any]]:
    """
    Fetch all pages of a paginated OpenGrants endpoint.

    Follows ``pagination.hasNext`` until exhausted, accumulating ``data``
    items from every page.

    Args:
        client: An ``httpx.AsyncClient`` instance.
        url: The full endpoint URL (without query params).
        params: Base query parameters (e.g. ``system``, ``sortOrder``).

    Returns:
        A flat list of every ``data`` item across all pages.
    """
    all_items: list[dict[str, Any]] = []
    offset = 0

    while True:
        page_params = {**params, "limit": PAGE_SIZE, "offset": offset}

        response = await _get_with_retry(client, url, page_params)
        body = response.json()

        data = body.get("data", [])
        all_items.extend(data)

        pagination = body.get("pagination", {})
        if not pagination.get("hasNext", False):
            break

        offset += PAGE_SIZE

    return all_items


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------


async def fetch_grant_pools(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """
    Fetch all SCF grant pools (rounds) from the OpenGrants API.

    Returns the raw API response ``data`` items (DAOIP-5 GrantPool dicts).
    """
    logger.info("Fetching SCF grant pools from %s", BASE_URL)

    pools = await _paginated_get(
        client,
        f"{BASE_URL}/grantPools",
        {"system": "scf", "sortOrder": "asc"},
    )

    logger.info("Fetched %d grant pools", len(pools))

    return pools


async def fetch_grant_applications(
    client: httpx.AsyncClient,
    pool_id: str,
) -> list[dict[str, Any]]:
    """
    Fetch all grant applications for a specific pool from the OpenGrants API.

    Args:
        client: An ``httpx.AsyncClient`` instance.
        pool_id: The DAOIP-5 pool identifier
            (e.g. ``"daoip-5:scf:grantPool:scf_#39"``).

    Returns:
        The raw API response ``data`` items (DAOIP-5 GrantApplication dicts).
    """
    logger.debug("Fetching applications for pool %s", pool_id)

    applications = await _paginated_get(
        client,
        f"{BASE_URL}/grantApplications",
        {"system": "scf", "sortOrder": "asc", "poolId": pool_id},
    )

    logger.debug("Fetched %d applications for pool %s", len(applications), pool_id)

    return applications


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------


def _get_ext(app: dict[str, Any], key: str) -> Any:
    """
    Retrieve a value from the ``io.scf`` extension namespace of an application.

    The OpenGrants API nests SCF-specific fields under
    ``extensions["io.scf"]["io.scf.<field>"]``. This helper provides concise
    access using the full dotted key (e.g. ``"io.scf.code"``).
    """

    return app.get("extensions", {}).get("io.scf", {}).get(key)


def _activity_status_from_tranche(app: dict[str, Any]) -> ActivityStatus:
    """
    Derive ``ActivityStatus`` from ``io.scf.trancheCompletionPercent``.

    - Below 100 → ``in_dev`` (still actively developing or delivering).
    - 100 or above → ``live`` (shipped on mainnet; project enters maintenance).
    - Missing or non-numeric → defaults to ``in_dev``.
    """
    raw = _get_ext(app, "io.scf.trancheCompletionPercent")
    if raw is None:
        return ActivityStatus.in_dev

    try:
        pct = float(raw)
    except TypeError, ValueError:
        return ActivityStatus.in_dev

    if pct >= 100:
        return ActivityStatus.live

    return ActivityStatus.in_dev


def _build_project_metadata(
    app: dict[str, Any],
    scf_submissions: list[dict[str, str]],
) -> dict[str, Any]:
    """
    Build the ``project_metadata`` JSONB dict for a ``ScfProject``.

    Extracts description, technical architecture, website, X / Twitter profile,
    SCF category, tranche completion, and the full round-history list.
    """
    meta: dict[str, Any] = {}

    description = _get_ext(app, "io.scf.oneSentenceDescription")
    if description:
        meta["description"] = description

    tech_arch = _get_ext(app, "io.scf.technicalArchitecture")
    if tech_arch:
        meta["technical_architecture"] = tech_arch

    website = _get_ext(app, "io.scf.website")
    if website:
        meta["website"] = website

    x_profile = _get_ext(app, "io.scf.x")
    if x_profile:
        meta["x_profile"] = x_profile

    category = _get_ext(app, "io.scf.category")
    if category:
        meta["scf_category"] = category

    tranche_completion = _get_ext(app, "io.scf.trancheCompletion")
    if tranche_completion:
        meta["scf_tranche_completion"] = tranche_completion

    meta["scf_submissions"] = scf_submissions

    return meta


def _map_application(
    app: dict[str, Any],
    scf_submissions: list[dict[str, str]],
) -> ScfProject:
    """
    Map a single OpenGrants GrantApplication dict to an ``ScfProject``.

    Uses the application's fields for scalar properties and the merged
    ``scf_submissions`` list (accumulated across all rounds) for history.
    """
    canonical_id = app.get("projectId") or app.get("id") or ""
    display_name = _get_ext(app, "io.scf.project") or app.get("projectName") or ""
    activity_status = _activity_status_from_tranche(app)

    # --- GitHub URL extraction ---
    git_org_url: str | None = None
    git_repo_url: str | None = None

    raw_code_url = _get_ext(app, "io.scf.code")

    if raw_code_url and isinstance(raw_code_url, str):
        org_url, repo_url = parse_github_url(raw_code_url)

        if org_url is None:
            logger.warning(
                "Invalid GitHub URL in io.scf.code for project %s: %r",
                canonical_id,
                raw_code_url,
            )
        else:
            git_org_url = org_url
            git_repo_url = repo_url

    project_metadata = _build_project_metadata(app, scf_submissions)

    return ScfProject(
        canonical_id=canonical_id,
        display_name=display_name,
        activity_status=activity_status,
        git_org_url=git_org_url,
        git_repo_url=git_repo_url,
        project_metadata=project_metadata,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def fetch_scf_projects(client: httpx.AsyncClient) -> list[ScfProject]:
    """
    Fetch all SCF projects from OpenGrants and return deduplicated ``ScfProject`` instances.

    Workflow:
        1. Fetch all grant pools (SCF rounds).
        2. For each pool, fetch all grant applications.
        3. Group applications by ``projectId``.
        4. For each group, keep the **latest** application's scalar fields
           but merge ``scf_submissions`` entries from every round.

    Args:
        client: A pre-configured ``httpx.AsyncClient`` (the caller controls
            timeouts, transport, and base-URL settings).

    Returns:
        A list of ``ScfProject`` dataclasses ready for ``Project`` upsert.
    """
    pools = await fetch_grant_pools(client)

    # Collect all applications across every pool / round.
    all_apps: list[dict[str, Any]] = []

    for pool in pools:
        pool_id = pool.get("id", "")
        pool_name = pool.get("name", pool_id)

        try:
            apps = await fetch_grant_applications(client, pool_id)
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Failed to fetch applications for pool %s (%s): %s",
                pool_id,
                pool_name,
                exc,
            )

            continue

        logger.info("Pool %s (%s): %d applications", pool_id, pool_name, len(apps))
        all_apps.extend(apps)

    logger.info("Total applications fetched across all pools: %d", len(all_apps))

    # ------------------------------------------------------------------
    # Deduplicate by projectId — keep the latest application's fields
    # but accumulate scf_submissions from every round.
    # ------------------------------------------------------------------
    # Pools are fetched in ascending order, so the last application per
    # projectId is the most recent one.
    project_latest: dict[str, dict[str, Any]] = {}
    project_submissions: dict[str, list[dict[str, str]]] = {}

    for app_data in all_apps:
        pid = app_data.get("projectId") or app_data.get("id") or ""
        if not pid:
            continue

        scf_round = _get_ext(app_data, "io.scf.round") or ""
        project_name = app_data.get("projectName") or ""
        submission_entry = {"round": scf_round, "title": project_name}

        project_submissions.setdefault(pid, []).append(submission_entry)

        # Overwrite with the latest (last seen = newest round).
        project_latest[pid] = app_data

    # Build ScfProject instances from the deduplicated map.
    projects: list[ScfProject] = []

    for pid, latest_app in project_latest.items():
        submissions = project_submissions.get(pid, [])
        project = _map_application(latest_app, submissions)
        projects.append(project)

    logger.info(
        "Deduplicated to %d unique projects from %d total applications",
        len(projects),
        len(all_apps),
    )

    return projects
