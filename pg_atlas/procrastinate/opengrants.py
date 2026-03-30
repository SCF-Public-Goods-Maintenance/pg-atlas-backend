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
from datetime import datetime, timezone
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
    category: str | None = None
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


def _retry_delay_from_headers(headers: httpx.Headers, fallback: float) -> float:
    """
    Compute the retry delay from rate-limit response headers.

    Checks ``x-ratelimit-reset`` (ISO-8601 timestamp) first, then
    ``Retry-After`` (seconds).  Returns *fallback* when neither is present.
    """
    reset_at = headers.get("x-ratelimit-reset")
    if reset_at:
        try:
            reset_dt = datetime.fromisoformat(reset_at)
            delay = (reset_dt - datetime.now(timezone.utc)).total_seconds()

            return max(delay, 0.5)
        except ValueError:
            pass

    retry_after = headers.get("Retry-After")
    if retry_after and retry_after.isdigit():
        return float(retry_after)

    return fallback


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, str | int],
) -> httpx.Response:
    """
    Perform a GET request with exponential back-off on transient errors.

    Retries on HTTP 429 (rate-limited) and 5xx (server error) up to
    ``MAX_RETRIES`` times.  Computes the delay from ``x-ratelimit-reset``
    or ``Retry-After`` headers when present, otherwise doubles the delay
    starting from ``INITIAL_BACKOFF_S``.

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

        delay = _retry_delay_from_headers(response.headers, fallback=backoff)

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
    logger.info(f"Fetching SCF grant pools from {BASE_URL}")

    pools = await _paginated_get(
        client,
        f"{BASE_URL}/grantPools",
        {"system": "scf", "sortOrder": "asc"},
    )

    logger.info(f"Fetched {len(pools)} grant pools")

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
    logger.debug(f"Fetching applications for pool {pool_id}")

    applications = await _paginated_get(
        client,
        f"{BASE_URL}/grantApplications",
        {"system": "scf", "sortOrder": "asc", "poolId": pool_id},
    )

    logger.debug(f"Fetched {len(applications)} applications for pool {pool_id}")

    return applications


async def fetch_opengrants_projects(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """
    Fetch all SCF projects from the ``/projects`` endpoint.

    Returns the raw API response ``data`` items (DAOIP-5 Project dicts) with
    full extension data including ``integrationStatus``, ``category``,
    ``socials``, and financial fields.
    """
    logger.info(f"Fetching SCF projects from {BASE_URL}/projects")

    projects = await _paginated_get(
        client,
        f"{BASE_URL}/projects",
        {"system": "scf"},
    )

    logger.info(f"Fetched {len(projects)} projects")

    return projects


# ---------------------------------------------------------------------------
# Mapping helpers
# ---------------------------------------------------------------------------


def _get_ext(app: dict[str, Any], key: str) -> Any:
    """
    Retrieve a value from the ``org.stellar.communityfund`` extension namespace of an application.

    The OpenGrants API nests SCF-specific fields under
    ``extensions["org.stellar.communityfund"]["org.stellar.communityfund.<field>"]``. This helper provides concise
    access using the full dotted key (e.g. ``"org.stellar.communityfund.code"``).
    """

    return app.get("extensions", {}).get("org.stellar.communityfund", {}).get(key)


# Map integrationStatus strings to ActivityStatus enum values.
_INTEGRATION_STATUS_MAP: dict[str, ActivityStatus] = {
    "Testnet": ActivityStatus.in_dev,
    "Development": ActivityStatus.in_dev,
    "Idea": ActivityStatus.in_dev,
    "Unknown": ActivityStatus.non_responsive,
    "Abandoned": ActivityStatus.discontinued,
    "Completed": ActivityStatus.discontinued,
    "Mainnet": ActivityStatus.live,
    "Live (on Mainnet)": ActivityStatus.live,
    "Expansion": ActivityStatus.live,
}


def _activity_status_from_integration_status(integration_status: str | None) -> ActivityStatus:
    """
    Derive ``ActivityStatus`` from the project-level ``integrationStatus`` field.

    Falls back to ``in_dev`` for empty/falsy or unexpected values.
    """
    if not integration_status:
        return ActivityStatus.in_dev

    status = _INTEGRATION_STATUS_MAP.get(integration_status)
    if status is not None:
        return status

    logger.warning(f"Unexpected integrationStatus {integration_status!r}, defaulting to in_dev")

    return ActivityStatus.in_dev


def _extract_github_from_socials(socials: list[dict[str, Any]]) -> tuple[str | None, str | None]:
    """
    Extract GitHub org and repo URLs from a project's ``socials`` array.

    Looks for ``{"name": "GitHub", "value": "<url>"}`` entries and passes
    them through ``parse_github_url``.

    Returns:
        ``(org_url, repo_url)`` or ``(None, None)`` if no GitHub link found.
    """
    for social in socials:
        if social.get("name") == "GitHub":
            raw = str(social.get("value", ""))
            if raw:
                return parse_github_url(raw)

    return None, None


def _check_project_completion(
    canonical_id: str,
    activity_status: ActivityStatus,
    total_awarded_usd: float | None,
    total_paid_usd: float | None,
) -> None:
    """
    Warn if a project is marked ``live`` but has not been fully paid out.

    Calculates ``project_completion = totalPaidUSD / totalAwardedUSD`` and
    logs a warning when the status is ``live`` but completion is below 1.0.
    """
    if activity_status != ActivityStatus.live:
        return

    if not total_awarded_usd or total_awarded_usd <= 0:
        return

    paid = total_paid_usd or 0.0
    completion = paid / total_awarded_usd

    if completion < 1.0:
        logger.warning(
            f"Project {canonical_id} is live but project_completion={completion:.2f} "
            f"(paid={paid:.0f}, awarded={total_awarded_usd:.0f})"
        )


def _activity_status_from_tranche(app: dict[str, Any]) -> ActivityStatus:
    """
    Derive ``ActivityStatus`` from ``org.stellar.communityfund.trancheCompletionPercent``.

    - Below 100 → ``in_dev`` (still actively developing or delivering).
    - 100 or above → ``live`` (shipped on mainnet; project enters maintenance).
    - Missing or non-numeric → defaults to ``in_dev``.
    """
    raw = _get_ext(app, "org.stellar.communityfund.trancheCompletionPercent")
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
    *,
    project: dict[str, Any] | None,
    latest_app: dict[str, Any] | None,
    scf_submissions: list[dict[str, str]],
) -> dict[str, Any]:
    """
    Build the ``project_metadata`` JSONB dict for a ``ScfProject``.

    Merges data from both the ``/projects`` endpoint and the latest grant
    application.  Application values win when both sources provide a value.
    Project-level financial and social fields fill gaps.
    """
    meta: dict[str, Any] = {}

    # --- From application (wins when present) ---
    if latest_app is not None:
        description = _get_ext(latest_app, "org.stellar.communityfund.oneSentenceDescription")
        if description:
            meta["description"] = description

        tech_arch = _get_ext(latest_app, "org.stellar.communityfund.technicalArchitecture")
        if tech_arch:
            meta["technical_architecture"] = tech_arch

        tranche_completion = _get_ext(latest_app, "org.stellar.communityfund.trancheCompletion")
        if tranche_completion:
            meta["scf_tranche_completion"] = tranche_completion

    # --- From project (fills gaps, adds financial/social fields) ---
    if project is not None:
        scf = project.get("extensions", {}).get("org.stellar.communityfund", {})

        website = scf.get("org.stellar.communityfund.website")
        if website:
            meta.setdefault("website", website)

        x_profile = scf.get("org.stellar.communityfund.x")
        if x_profile:
            meta.setdefault("x_profile", x_profile)

        # Financial fields (project-level only)
        for field_key, meta_key in (
            ("org.stellar.communityfund.totalAwardedUSD", "total_awarded_usd"),
            ("org.stellar.communityfund.totalPaidUSD", "total_paid_usd"),
            ("org.stellar.communityfund.awardedSubmissionsCount", "awarded_submissions_count"),
        ):
            val = scf.get(field_key)
            if val is not None:
                meta[meta_key] = val

        open_source = scf.get("org.stellar.communityfund.openSource")
        if open_source is not None:
            meta["open_source"] = open_source

        # Social / analytics URLs
        socials = project.get("socials", [])
        if socials:
            meta["socials"] = socials

        analytics = scf.get("org.stellar.communityfund.analytics")
        if analytics:
            meta["analytics"] = analytics

        regions = scf.get("org.stellar.communityfund.regionsOfOperation")
        if regions:
            meta["regions_of_operation"] = regions

    # Always include description from project description if not yet set by app
    if "description" not in meta and project is not None:
        proj_desc = project.get("description")
        if proj_desc:
            meta["description"] = proj_desc

    meta["scf_submissions"] = scf_submissions

    return meta


def _map_application(
    app: dict[str, Any],
    scf_submissions: list[dict[str, str]],
) -> ScfProject:
    """
    Map a single OpenGrants GrantApplication dict to an ``ScfProject``.

    Used as fallback for applications that have no matching project record.
    """
    canonical_id = app.get("projectId") or app.get("id") or ""
    display_name = _get_ext(app, "org.stellar.communityfund.project") or app.get("projectName") or ""
    activity_status = _activity_status_from_tranche(app)

    git_org_url: str | None = None
    git_repo_url: str | None = None

    raw_code_url = _get_ext(app, "org.stellar.communityfund.code")

    if raw_code_url and isinstance(raw_code_url, str):
        org_url, repo_url = parse_github_url(raw_code_url)

        if org_url is None:
            logger.warning(
                f"Invalid GitHub URL in org.stellar.communityfund.code for project {canonical_id}: {raw_code_url!r}"
            )
        else:
            git_org_url = org_url
            git_repo_url = repo_url

    project_metadata = _build_project_metadata(project=None, latest_app=app, scf_submissions=scf_submissions)

    return ScfProject(
        canonical_id=canonical_id,
        display_name=display_name,
        activity_status=activity_status,
        git_org_url=git_org_url,
        git_repo_url=git_repo_url,
        project_metadata=project_metadata,
    )


def _merge_project_and_applications(
    project: dict[str, Any],
    latest_app: dict[str, Any] | None,
    scf_submissions: list[dict[str, str]],
) -> ScfProject:
    """
    Merge data from a ``/projects`` record and its latest grant application
    into a single ``ScfProject``.

    Priority: latest application wins for fields that exist in both sources;
    the project record fills gaps.
    """
    scf = project.get("extensions", {}).get("org.stellar.communityfund", {})
    canonical_id = project.get("id", "")
    display_name = project.get("name", "")

    # --- Activity status from integrationStatus ---
    integration_status = scf.get("org.stellar.communityfund.integrationStatus")
    activity_status = _activity_status_from_integration_status(integration_status)

    # --- Completion check ---
    total_awarded = scf.get("org.stellar.communityfund.totalAwardedUSD")
    total_paid = scf.get("org.stellar.communityfund.totalPaidUSD")
    _check_project_completion(canonical_id, activity_status, total_awarded, total_paid)

    # --- Category ---
    category = scf.get("org.stellar.communityfund.category")

    # --- GitHub URL: latest application first, then project socials ---
    git_org_url: str | None = None
    git_repo_url: str | None = None

    if latest_app is not None:
        raw_code_url = _get_ext(latest_app, "org.stellar.communityfund.code")

        if raw_code_url and isinstance(raw_code_url, str):
            org_url, repo_url = parse_github_url(raw_code_url)

            if org_url is None:
                logger.warning(f"Invalid GitHub URL in application code for project {canonical_id}: {raw_code_url!r}")
            else:
                git_org_url = org_url
                git_repo_url = repo_url

    if git_org_url is None:
        socials = project.get("socials", [])
        git_org_url, git_repo_url = _extract_github_from_socials(socials)

    # --- Metadata ---
    project_metadata = _build_project_metadata(
        project=project,
        latest_app=latest_app,
        scf_submissions=scf_submissions,
    )

    return ScfProject(
        canonical_id=canonical_id,
        display_name=display_name,
        activity_status=activity_status,
        git_org_url=git_org_url,
        git_repo_url=git_repo_url,
        category=category,
        project_metadata=project_metadata,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def fetch_scf_projects(client: httpx.AsyncClient) -> list[ScfProject]:
    """
    Fetch all SCF projects from OpenGrants and return ``ScfProject`` instances.

    Merges data from the ``/projects`` endpoint with grant applications from
    ``/grantApplications``.  The latest application wins for fields that
    exist in both sources; the project record fills gaps.

    Workflow:
        1. Fetch all projects via ``/projects``.
        2. Fetch all grant pools and their applications.
        3. Group applications by ``projectId``, keeping the latest per project.
        4. For each project: merge with its latest application (if any).
        5. Log warnings for projects without applications and vice versa.

    Args:
        client: A pre-configured ``httpx.AsyncClient``.

    Returns:
        A list of ``ScfProject`` dataclasses ready for ``Project`` upsert.
    """
    # --- 1. Fetch projects ---
    raw_projects = await fetch_opengrants_projects(client)
    projects_by_id: dict[str, dict[str, Any]] = {}

    for proj in raw_projects:
        pid = proj.get("id", "")
        if pid:
            projects_by_id[pid] = proj

    # --- 2. Fetch all applications across all pools ---
    pools = await fetch_grant_pools(client)
    all_apps: list[dict[str, Any]] = []

    for pool in pools:
        pool_id = pool.get("id", "")
        pool_name = pool.get("name", pool_id)

        try:
            apps = await fetch_grant_applications(client, pool_id)
        except httpx.HTTPStatusError as exc:
            logger.error(f"Failed to fetch applications for pool {pool_id} ({pool_name}): {exc}")

            continue

        logger.info(f"Pool {pool_id} ({pool_name}): {len(apps)} applications")
        all_apps.extend(apps)

    logger.info(f"Total applications fetched across all pools: {len(all_apps)}")

    # --- 3. Group applications by projectId ---
    # Pools are fetched ascending, so the last seen application per projectId
    # is the most recent one.
    app_latest: dict[str, dict[str, Any]] = {}
    app_submissions: dict[str, list[dict[str, str]]] = {}

    for app_data in all_apps:
        pid = app_data.get("projectId") or app_data.get("id") or ""
        if not pid:
            continue

        scf_round = _get_ext(app_data, "org.stellar.communityfund.round") or ""
        project_name = app_data.get("projectName") or ""
        submission_entry = {"round": scf_round, "title": project_name}

        app_submissions.setdefault(pid, []).append(submission_entry)
        app_latest[pid] = app_data

    # --- 4. Merge projects with applications ---
    results: list[ScfProject] = []
    seen_project_ids: set[str] = set()

    for pid, project in projects_by_id.items():
        seen_project_ids.add(pid)
        latest_app = app_latest.get(pid)
        submissions = app_submissions.get(pid, [])

        if latest_app is None:
            name = project.get("name", pid)
            logger.warning(f"Project {pid} ({name}) has no applications")

        scf_project = _merge_project_and_applications(project, latest_app, submissions)
        results.append(scf_project)

    # --- 5. Handle applications with no matching project ---
    orphan_count = 0

    for pid, latest_app in app_latest.items():
        if pid in seen_project_ids:
            continue

        orphan_count += 1
        submissions = app_submissions.get(pid, [])
        scf_project = _map_application(latest_app, submissions)
        results.append(scf_project)

    if orphan_count:
        logger.warning(f"{orphan_count} applications had no matching project record")

    logger.info(f"Merged {len(projects_by_id)} projects + {len(app_latest)} app groups → {len(results)} ScfProject instances")

    return results
