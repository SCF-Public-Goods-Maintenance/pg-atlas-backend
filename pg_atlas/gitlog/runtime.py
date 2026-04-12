"""
Shared git log runtime used by CLI and Procrastinate workers.

This module centralizes repo resolution, retry/backoff behavior, per-repo
persistence, and run-level summary logging so local CLI and queue workers share
identical processing semantics.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import asyncio
import logging
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pg_atlas.config import settings
from pg_atlas.db_models.base import SubmissionStatus, Visibility
from pg_atlas.db_models.repo_vertex import Repo
from pg_atlas.gitlog.parser import RepoParseResult, parse_repo_with_raw_output
from pg_atlas.gitlog.persist import (
    GitLogAttemptAudit,
    PersistResult,
    persist_repo_result,
    record_gitlog_attempt,
)
from pg_atlas.storage.artifacts import store_artifact

logger = logging.getLogger(__name__)


@dataclass
class GitLogRunSummary:
    """Aggregated counters for one git log processing run."""

    repos_requested: int = 0
    repos_processed: int = 0
    repos_with_errors: int = 0
    total_contributors_stored: int = 0
    total_bot_commits: int = 0
    total_bot_contributors: int = 0
    total_edges_created: int = 0
    total_edges_updated: int = 0
    total_rate_limit_hits: int = 0
    first_rate_limit_hit_after_n_repos: int | None = None
    terminal_failure_repo_ids: set[int] = field(default_factory=set[int])
    terminal_failure_urls: list[str] = field(default_factory=list[str])
    error_urls: list[str] = field(default_factory=list[str])


def _log_repo_summary(result: RepoParseResult, persist: PersistResult | None, index: int, total: int, since: int) -> None:
    """Log one structured repo summary line block."""

    bot_pct = (result.bot_commit_count / result.total_commits * 100) if result.total_commits else 0.0
    human_count = len(result.contributors)
    flyby = sum(1 for c in result.contributors if c.number_of_commits == 1)
    flyby_str = f"{flyby} of {human_count} ({flyby / human_count * 100:.1f}%)" if human_count else "N/A"
    top = ", ".join(f"{c.display_name} ({c.number_of_commits})" for c in result.contributors[:5])

    logger.info(
        f"""
[{index}/{total}] Repo: {result.repo_url}
  Commits in window: {result.total_commits} ({since} months)
  Bot commits excluded: {result.bot_commit_count} from {result.bot_contributor_count} bots ({bot_pct:.1f}% of total)
  Human contributors: {human_count}
  Fly-by contributors: {flyby_str}
  Top contributors: {top or "(none)"}
        """.strip()
    )

    if result.errors:
        for err in result.errors:
            logger.warning(f"  Error: {err}")

    if persist:
        logger.info(
            f"  DB: {persist.contributors_created} contributors created, "
            f"{persist.contributors_updated} updated; {persist.edges_created} edges created, "
            f"{persist.edges_updated} updated"
        )


def _artifact_key_for_repo(repo_url: str) -> str:
    """Return stable artifact key suffix for one repository URL."""

    parsed = urllib.parse.urlparse(repo_url)
    repo_path = parsed.path.removesuffix(".git").strip("/")
    if repo_path:
        return repo_path

    return repo_url.replace(":", "_").replace("/", "_")


async def resolve_repos(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    from_db: bool,
    repo_urls: list[str],
    repo_ids: list[int] | None = None,
    exclude_private_for_db: bool = True,
) -> list[Repo]:
    """
    Resolve repos from DB IDs, from-db mode, or explicit URL lookup.

    In from-db mode, repos without a repo_url are skipped. Private repos are
    excluded by default.
    """

    async with session_factory() as session:
        if repo_ids is not None:
            stmt = select(Repo).where(Repo.id.in_(repo_ids)).order_by(Repo.id)
            rows = list((await session.execute(stmt)).scalars().all())

            return rows

        if from_db:
            stmt = select(Repo).where(Repo.repo_url.isnot(None), Repo.repo_url != "")
            if exclude_private_for_db:
                stmt = stmt.where(Repo.visibility != Visibility.private)

            rows = list((await session.execute(stmt.order_by(Repo.id))).scalars().all())
            logger.info(f"Found {len(rows)} candidate repos with repo_url in database")

            return rows

        matched: list[Repo] = []
        for url in repo_urls:
            stmt = select(Repo).where(Repo.repo_url == url)
            repo = (await session.execute(stmt)).scalar_one_or_none()
            if repo is None:
                logger.warning(f"No Repo found for URL {url} - skipping")
                continue

            matched.append(repo)

        return matched


async def _parse_repo_with_backoff(
    repo_url: str,
    clone_dir: Path,
    since_months: int,
    clone_timeout: float,
    max_retries: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
) -> tuple[RepoParseResult, bytes | None]:
    """Parse one repo with bounded backoff when rate limiting is detected."""

    total_rate_limit_hits = 0
    for attempt in range(max_retries + 1):
        result, raw_output = await parse_repo_with_raw_output(repo_url, clone_dir, since_months, clone_timeout)
        total_rate_limit_hits += result.rate_limit_hits

        if result.rate_limit_hits == 0:
            result.rate_limit_hits = total_rate_limit_hits

            return result, raw_output

        if attempt >= max_retries:
            result.rate_limit_hits = total_rate_limit_hits

            return result, raw_output

        backoff_seconds = min(initial_backoff_seconds * (2**attempt), max_backoff_seconds)
        logger.warning(
            f"Rate-limited while processing {repo_url}; retrying in {backoff_seconds:.1f}s ({attempt + 1}/{max_retries})"
        )
        await asyncio.sleep(backoff_seconds)

    raise RuntimeError("Unreachable backoff loop state")


async def run_gitlog_pipeline(
    session_factory: async_sessionmaker[AsyncSession],
    repos: list[Repo],
    *,
    since_months: int,
    clone_dir: Path,
    clone_timeout: float,
    clone_delay: float,
    max_rate_limit_retries: int,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    mark_terminal_failures_private: bool,
) -> GitLogRunSummary:
    """
    Execute gitlog parsing/persistence for a list of repos.

    Per-repo writes are committed in isolated transactions. Terminal git failures
    may be bulk-marked as private at the end of the run.
    """

    summary = GitLogRunSummary(repos_requested=len(repos))
    clone_dir.mkdir(parents=True, exist_ok=True)

    for index, repo in enumerate(repos, 1):
        if not repo.repo_url:
            continue

        result, raw_output = await _parse_repo_with_backoff(
            repo.repo_url,
            clone_dir,
            since_months,
            clone_timeout,
            max_rate_limit_retries,
            initial_backoff_seconds,
            max_backoff_seconds,
        )

        summary.repos_processed += 1
        summary.total_rate_limit_hits += result.rate_limit_hits
        if result.rate_limit_hits > 0 and summary.first_rate_limit_hit_after_n_repos is None:
            summary.first_rate_limit_hit_after_n_repos = summary.repos_processed

        persist_result: PersistResult | None = None
        artifact_path: str | None = None
        artifact_content_hash: str | None = None
        should_null_previous_artifact_paths = False

        if raw_output is not None:
            try:
                artifact_key = _artifact_key_for_repo(repo.repo_url)
                artifact_filename = f"git-logs/{artifact_key}.gitlog"
                artifact_path, artifact_content_hash = await store_artifact(raw_output, artifact_filename)
                should_null_previous_artifact_paths = True
            except (OSError, RuntimeError, ValueError) as exc:
                detail = f"Artifact storage failed: {type(exc).__name__}: {exc}"
                result.errors.append(detail)

        async with session_factory() as session:
            db_repo = await session.get(Repo, repo.id)
            if db_repo is None:
                raise ValueError(f"Repo {repo.id} no longer exists")

            try:
                if not result.errors:
                    persist_result = await persist_repo_result(session, db_repo, result)

                await session.commit()
            except SQLAlchemyError as exc:
                detail = f"DB persistence failed for {repo.repo_url}: {type(exc).__name__}: {exc}"
                logger.exception(detail)
                result.errors.append(detail)
                # do not try to persist the repo result again
                await session.rollback()

            attempt_status = SubmissionStatus.processed if not result.errors else SubmissionStatus.failed
            error_detail = "; ".join(result.errors) if result.errors else None
            try:
                await record_gitlog_attempt(
                    session,
                    db_repo.id,
                    GitLogAttemptAudit(
                        since_months=since_months,
                        status=attempt_status,
                        error_detail=error_detail,
                        artifact_path=artifact_path,
                        artifact_content_hash=artifact_content_hash,
                        null_previous_artifact_paths=should_null_previous_artifact_paths
                        and attempt_status == SubmissionStatus.processed,
                    ),
                )
                await session.commit()
            except SQLAlchemyError as audit_exc:
                logger.exception(f"DB audit persistence failed for {repo.repo_url}: {type(audit_exc).__name__}: {audit_exc}")
                logger.error(f"After previous errors: {error_detail}")

        if result.errors:
            summary.repos_with_errors += 1
            summary.error_urls.append(repo.repo_url)

        if result.terminal_git_failure:
            summary.terminal_failure_repo_ids.add(repo.id)
            summary.terminal_failure_urls.append(repo.repo_url)

        _log_repo_summary(result, persist_result, index, len(repos), since_months)

        summary.total_bot_commits += result.bot_commit_count
        summary.total_bot_contributors += result.bot_contributor_count
        if persist_result:
            summary.total_contributors_stored += persist_result.contributors_created + persist_result.contributors_updated
            summary.total_edges_created += persist_result.edges_created
            summary.total_edges_updated += persist_result.edges_updated

        if index < len(repos):
            await asyncio.sleep(clone_delay)

    if mark_terminal_failures_private and summary.terminal_failure_repo_ids:
        async with session_factory() as session:
            await session.execute(
                update(Repo).where(Repo.id.in_(summary.terminal_failure_repo_ids)).values(visibility=Visibility.private)
            )
            await session.commit()

        logger.warning(f"Marked {len(summary.terminal_failure_repo_ids)} repos as private due to terminal git failures")
        logger.warning(f"Gitlog terminal failures marked private: {', '.join(summary.terminal_failure_urls)}")

    logger.info(
        f"""
Git log parsing complete:
  Repos processed: {summary.repos_processed} ({summary.repos_with_errors} errors)
  Total contributors stored: {summary.total_contributors_stored}
  Total bot commits excluded: {summary.total_bot_commits} from {summary.total_bot_contributors} unique bots
  Total edges created/updated: {summary.total_edges_created} created, {summary.total_edges_updated} updated
        """.strip()
    )

    first_hit = (
        str(summary.first_rate_limit_hit_after_n_repos) if summary.first_rate_limit_hit_after_n_repos is not None else "none"
    )
    logger.info(
        f"Gitlog rate-limit stats: first_rate_limit_hit_after_n_repos={first_hit} "
        f"total_rate_limit_hits={summary.total_rate_limit_hits}"
    )

    if summary.error_urls:
        logger.info(f"Gitlog run errors: {summary.error_urls}")

    return summary


async def process_gitlog_repo_batch(repo_ids: list[int]) -> GitLogRunSummary:
    """
    Process one batch of repo IDs using settings-driven worker behavior.

    This entrypoint is used by Procrastinate tasks and intentionally does not
    expose local CLI override flags.
    """

    from pg_atlas.db_models.session import get_session_factory

    session_factory = get_session_factory()
    repos = await resolve_repos(
        session_factory,
        from_db=False,
        repo_urls=[],
        repo_ids=repo_ids,
        exclude_private_for_db=True,
    )
    if not repos:
        logger.info("No repos found for gitlog batch")

        return GitLogRunSummary(repos_requested=0)

    return await run_gitlog_pipeline(
        session_factory,
        repos,
        since_months=settings.GITLOG_SINCE_MONTHS,
        clone_dir=Path(settings.GITLOG_CLONE_DIR),
        clone_timeout=settings.GITLOG_CLONE_TIMEOUT,
        clone_delay=settings.GITLOG_CLONE_DELAY,
        max_rate_limit_retries=settings.GITLOG_RATE_LIMIT_MAX_RETRIES,
        initial_backoff_seconds=settings.GITLOG_RATE_LIMIT_INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds=settings.GITLOG_RATE_LIMIT_MAX_BACKOFF_SECONDS,
        mark_terminal_failures_private=True,
    )
