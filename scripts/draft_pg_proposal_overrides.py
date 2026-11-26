"""

Draft PG proposal overrides from open or merged proposals.

This script is intended for human-in-the-loop operation. It reads open pull
requests with label ``pg-award-proposal`` or merged markdown files from the
public-goods docs repository, extracts patchable project fields, and writes a
review draft to:

    pg_atlas/data/projects/new.pg-proposal-overrides.yml

The canonical baseline and matching source is:

    pg_atlas/data/projects/pg-proposal-overrides.yml

Usage:
    uv run python scripts/draft_pg_proposal_overrides.py
    uv run python scripts/draft_pg_proposal_overrides.py --source=merged
    uv run python scripts/draft_pg_proposal_overrides.py --source=merged --refresh-stroopies

Each run drafts maintainer declarations. Previously fetched Stroopies members
are reused until an explicit refresh. Add a comment like "# reject: github-handle"
by the project's maintainers list in the reviewed override file to suppress it.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from github import Github, GithubException
from github.Repository import Repository
from mistletoe import Document
from mistletoe.block_token import Heading, Paragraph, Table
from mistletoe.span_token import LineBreak
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString
from ruamel.yaml.tokens import CommentToken
from tqdm import tqdm

from pg_atlas.ingestion.declared_maintainers import plan_review, project_page_logins, reconcile
from pg_atlas.ingestion.stroopies import MAINNET_RPC, cached_snapshot
from pg_atlas.procrastinate.github import get_github_client

_DOCS_OWNER = "SCF-Public-Goods-Maintenance"
_DOCS_REPO = "scf-public-goods-maintenance.github.io"
_QUERY = "repo:SCF-Public-Goods-Maintenance/scf-public-goods-maintenance.github.io is:pr is:open label:pg-award-proposal"
_DOCS_REPO_URL = f"https://github.com/{_DOCS_OWNER}/{_DOCS_REPO}"

_DAOIP_PREFIX = "daoip-5:scf:project:"

_PROJECTS_DIR = Path("pg_atlas/data/projects")
_CANONICAL_PATH = _PROJECTS_DIR / "pg-proposal-overrides.yml"
_OUTPUT_PATH = _PROJECTS_DIR / "new.pg-proposal-overrides.yml"
_CACHE_DIR = Path("scripts/.github_cache")
_STROOPIES_CACHE_DIR = Path("scripts/.stroopies_cache")
_ENTRY_KEY_ORDER = (
    "display_name",
    "activity_status",
    "pg_award",
    "git_owner_url",
    "git_repo_urls",
    "category",
    "metadata",
)

yaml: YAML = YAML(typ="rt")
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.width = 120


@dataclass(frozen=True, slots=True)
class _ProposalRecord:
    """Raw proposal markdown and source metadata shared by both collectors."""

    project_path: str
    markdown_text: str
    source_label: str
    commit_sha: str


def main() -> int:
    """Generate draft proposal override patches from the selected proposal source."""

    args = _build_parser().parse_args()

    try:
        canonical_template, canonical_overrides = _load_canonical_overrides()
        snapshot = cached_snapshot(_STROOPIES_CACHE_DIR, MAINNET_RPC, refresh=args.refresh_stroopies)
    except Exception as exc:
        _print_error(f"Failed to load draft inputs: {exc}")

        return 1

    merged_overrides: dict[str, dict[str, Any]] = {key: dict(value) for key, value in canonical_overrides.items()}

    name_map = _collect_existing_name_map(canonical_overrides)

    try:
        gh = get_github_client()
        docs_repo = gh.get_repo(f"{_DOCS_OWNER}/{_DOCS_REPO}")
    except GithubException as exc:
        _print_error(f"GitHub repository initialization failed: {exc}")

        return 1

    except Exception as exc:
        _print_error(f"Unexpected GitHub initialization failure: {exc}")

        return 1

    try:
        if args.source == "open":
            proposal_records, skipped = _collect_open_proposals(gh, docs_repo, read_cache=args.read_cache)
        else:
            proposal_records, skipped = _collect_merged_proposals(docs_repo)

    except GithubException as exc:
        _print_error(f"GitHub {args.source} proposal collection failed: {exc}")

        return 1

    except Exception as exc:
        _print_error(f"Unexpected {args.source} proposal collection failure: {exc}")

        return 1

    processed: list[str] = []
    page_evidence: dict[str, dict[str, list[dict[str, Any]]]] = {}
    page_reads: dict[str, set[tuple[str, str | int]]] = {}
    source_kind = "proposal" if args.source == "open" else "project_page"

    for record in proposal_records:
        try:
            candidate = _extract_markdown_fields(record.markdown_text, record.source_label, record.project_path)
        except Exception as exc:
            print(f"WARNING {record.source_label} ({record.project_path}): failed to parse: {exc}")
            skipped += 1
            continue

        project_path = record.project_path
        slug = _extract_slug_from_path(project_path)
        normalized_slug = _normalize_alpha(slug)

        matched_key: str | None = None
        ambiguous = False

        if normalized_slug:
            matching_keys = name_map.get(normalized_slug, [])
            if matching_keys:
                matched_key = matching_keys[0]
                if len(matching_keys) > 1:
                    ambiguous = True
                    print(f"WARNING {record.source_label}: multiple name matches for slug={slug!r}; using {matched_key!r}")

        if matched_key is None:
            extracted_repo_urls = candidate.get("git_repo_urls")
            repo_urls_for_match = (
                [url for url in cast(list[Any], extracted_repo_urls) if isinstance(url, str)]
                if isinstance(extracted_repo_urls, list)
                else []
            )
            repo_match, scored_matches = _get_existing_repo_overlap(canonical_overrides, repo_urls_for_match)
            if repo_match is not None:
                matched_key = repo_match
                if len(scored_matches) > 1:
                    ambiguous = scored_matches[0][1] == scored_matches[1][1]
                    joined = ", ".join(f"{key}:{score}" for key, score in scored_matches)
                    print(
                        f"WARNING {record.source_label}: more than one repo-overlap match for {project_path}; "
                        f"selected {repo_match!r}. candidates={joined}"
                    )

        if matched_key is None:
            suffix = _slug_to_canonical_suffix(slug)
            if not suffix:
                print(f"WARNING {record.source_label}: unable to derive canonical key from slug {slug!r}; skipped")
                skipped += 1
                continue

            matched_key = f"{_DAOIP_PREFIX}{suffix}"

        source_tag = record.source_label
        existing_entry = merged_overrides.get(matched_key, {})
        has_front_matter = candidate.get("_has_front_matter") is True
        if has_front_matter and "display_name" not in candidate:
            candidate["display_name"] = _slug_to_display_name(slug)

        merged_entry = _merge_patch_entry(existing_entry, candidate, source_tag=source_tag)
        if not has_front_matter and not existing_entry:
            merged_entry["display_name"] = None

        merged_entry = _ensure_required_properties(merged_entry, fallback_display_name=_slug_to_display_name(slug))
        merged_overrides[matched_key] = merged_entry

        processed.append(matched_key)

        if matched_key in canonical_overrides and not ambiguous:
            _, body, _ = _parse_front_matter(
                record.markdown_text, source_label=record.source_label, source_path=record.project_path
            )
            project_evidence = page_evidence.setdefault(matched_key, {})
            evidence = {
                "source": source_kind,
                "path": record.project_path,
                "commit_sha": record.commit_sha,
            }
            page_reads.setdefault(matched_key, set()).add((source_kind, record.project_path))
            for login in project_page_logins(body):
                references = project_evidence.setdefault(login, [])
                if evidence not in references:
                    references.append(evidence)

    output_path = _OUTPUT_PATH
    review_comments: dict[str, list[dict[str, Any]]] = {}
    rejections = _reviewer_rejections(canonical_template)
    review_notes = {key: [f"reject: {login}" for login in sorted(logins)] for key, logins in rejections.items()}
    conflict_count = 0
    token_ledgers = {
        member.token_id: member.ledger if member.ledger is not None else snapshot.ledger for member in snapshot.members
    }
    for project_id in sorted(canonical_overrides):
        project = merged_overrides[project_id]
        metadata = project.get("metadata", {})
        previous = metadata.get("maintainers", [])
        active, commented = reconcile(project_id, page_evidence.get(project_id, {}), snapshot, previous_items=previous)
        if not previous and not active and not commented:
            continue

        reviewed, candidates, removals = plan_review(
            previous,
            active,
            commented,
            page_reads.get(project_id, set()),
            rejections.get(project_id, set()),
            token_ledgers=token_ledgers,
        )
        if previous or reviewed:
            project["metadata"] = {**metadata, "maintainers": reviewed}

        review_comments[project_id] = candidates
        review_notes.setdefault(project_id, []).extend(
            f"Proposed removal @{login}: {reason}." for login, reason in removals.items()
        )
        previous_by_login = {item["github_login"].lower(): item for item in previous}
        additions = sum(
            "conflict" not in item and previous_by_login.get(item["github_login"].lower()) != item for item in reviewed
        )
        conflicts = sum("conflict" in item for item in reviewed)
        conflict_count += conflicts
        if additions or removals or conflicts or candidates:
            print(
                f"{project_id}: {additions} added or updated, {len(removals)} proposed removals, "
                f"{conflicts} conflicts, {len(candidates)} commented candidates"
            )

    for key, value in list(merged_overrides.items()):
        suffix = key[len(_DAOIP_PREFIX) :] if key.startswith(_DAOIP_PREFIX) else key
        merged_overrides[key] = _ensure_required_properties(
            value,
            fallback_display_name=_slug_to_display_name(suffix),
        )

    output_text = _render_sorted_yaml(merged_overrides, canonical_template, review_comments, review_notes)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text, encoding="utf-8")
    except OSError as exc:
        _print_error(f"Failed to write draft file {output_path}: {exc}")

        return 1

    print(f"Processed proposals: {len(processed)}")
    print(f"Skipped proposals: {skipped}")
    print(f"Draft overrides written to {output_path}")

    if conflict_count:
        _print_error(f"Resolve {conflict_count} maintainer conflict(s) in the draft before continuing.")

        return 1

    print("\nReview the generated overrides before applying them through Reference Graph Bootstrap.")

    return 0


def _yaml_load(text: str) -> Any:
    """Load YAML text through ruamel's incompletely typed stream API."""

    return cast(Any, yaml).load(text)


def _yaml_dump(data: Any, stream: StringIO) -> None:
    """Dump YAML through ruamel's incompletely typed serializer API."""

    cast(Any, yaml).dump(data, stream)


def _print_error(message: str) -> None:
    """Print a major failure message to stderr."""

    print(message, file=sys.stderr)


def _normalize_alpha(value: str) -> str:
    """Return lowercased string with only ``a-z`` characters preserved."""

    lowered = value.lower()

    return "".join(ch for ch in lowered if "a" <= ch <= "z")


def _slug_to_canonical_suffix(slug: str) -> str:
    """Convert proposal slug to canonical suffix using underscore normalization."""

    lowered = slug.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")

    return normalized


def _extract_slug_from_path(path: str) -> str:
    """Extract proposal slug from docs/projects markdown file path."""

    return Path(path).stem


def _as_string_key_dict(value: Any) -> dict[str, Any] | None:
    """Return a ``dict[str, Any]`` when all keys are strings, else ``None``."""

    if not isinstance(value, dict):
        return None

    normalized: dict[str, Any] = {}
    raw_dict = cast(dict[Any, Any], value)
    for key, item in raw_dict.items():
        if not isinstance(key, str):
            return None

        normalized[key] = item

    return normalized


def _load_canonical_overrides() -> tuple[CommentedMap, dict[str, dict[str, Any]]]:
    """Load canonical overrides and return round-trip mapping plus normalized dict."""

    if not _CANONICAL_PATH.exists():
        raise FileNotFoundError(f"Canonical override file not found: {_CANONICAL_PATH}")

    raw_text = _CANONICAL_PATH.read_text(encoding="utf-8")
    loaded = _yaml_load(raw_text)
    parsed_opt = _as_string_key_dict(loaded)
    if parsed_opt is None and loaded is not None:
        raise ValueError(f"Canonical override file must parse to a mapping: {_CANONICAL_PATH}")

    parsed = parsed_opt or {}

    normalized: dict[str, dict[str, Any]] = {}
    for key, value in parsed.items():
        if key == "$schema":
            continue

        value_dict = _as_string_key_dict(value)
        if value_dict is None:
            raise ValueError(f"Override value for {key!r} must be a mapping")

        normalized[key] = value_dict

    canonical_map = loaded if isinstance(loaded, CommentedMap) else CommentedMap()

    return canonical_map, normalized


def _build_parser() -> argparse.ArgumentParser:
    """Create CLI parser for script runtime flags."""

    parser = argparse.ArgumentParser(description="Draft PG proposal overrides from open or merged proposals.")
    parser.add_argument(
        "--source",
        choices=("open", "merged"),
        default="open",
        help="Read open proposal PRs or proposals merged into docs/projects (default: open).",
    )
    parser.add_argument(
        "--read-cache",
        action="store_true",
        help="Read PR/file data from scripts/.github_cache when available before calling GitHub APIs.",
    )

    parser.add_argument("--refresh-stroopies", action="store_true", help="Replace the cache with a full contract read.")

    return parser


def _collect_open_proposals(gh: Github, docs_repo: Repository, *, read_cache: bool) -> tuple[list[_ProposalRecord], int]:
    """Collect raw markdown records from open proposal pull requests."""

    search_results = gh.search_issues(query=_QUERY)
    records: list[_ProposalRecord] = []
    skipped = 0

    for issue in tqdm(search_results, desc="Open proposals", unit="PR"):
        pr_number = issue.number

        pull_head_sha: str | None = None
        cached_pr = _load_cached_pr(pr_number) if read_cache else None
        if cached_pr is not None:
            pull_raw, changed_files = cached_pr
            pull_head_sha = _pull_head_sha_from_raw(pull_raw)
        else:
            try:
                pull = docs_repo.get_pull(pr_number)
                changed_files = [file.filename for file in pull.get_files()]
                pull_head_sha = pull.head.sha
                _save_cached_pr(pr_number, pull.raw_data, changed_files)
            except GithubException as exc:
                print(f"WARNING PR #{pr_number}: failed to inspect changed files: {exc}")
                skipped += 1
                continue

        valid_single, project_path = _is_valid_single_project_file(changed_files)
        if not valid_single or project_path is None:
            print(f"INFO PR #{pr_number}: skipped (changed files={len(changed_files)})")
            skipped += 1
            continue

        if not pull_head_sha:
            print(f"WARNING PR #{pr_number}: missing head sha for {project_path}")
            skipped += 1
            continue

        markdown_text = _load_cached_markdown(pr_number, project_path, pull_head_sha) if read_cache else None
        if markdown_text is None:
            try:
                source = docs_repo.get_contents(project_path, ref=pull_head_sha)
                if isinstance(source, list):
                    raise ValueError(f"Expected file content, got directory listing for {project_path}")

                content_bytes = source.decoded_content
                markdown_text = content_bytes.decode("utf-8")
                _save_cached_markdown(pr_number, project_path, pull_head_sha, content_bytes)
            except GithubException as exc:
                print(f"WARNING PR #{pr_number}: failed to fetch {project_path}: {exc}")
                skipped += 1
                continue
            except Exception as exc:
                print(f"WARNING PR #{pr_number}: failed to decode {project_path}: {exc}")
                skipped += 1
                continue

        records.append(
            _ProposalRecord(
                project_path=project_path,
                markdown_text=markdown_text,
                source_label=f"PR #{pr_number} ({project_path})",
                commit_sha=pull_head_sha,
            )
        )

    return records, skipped


def _collect_merged_proposals(docs_repo: Repository) -> tuple[list[_ProposalRecord], int]:
    """Collect raw markdown records from direct .md files under docs/projects."""

    commit_sha = docs_repo.get_branch("main").commit.sha
    entries = docs_repo.get_contents("docs/projects", ref=commit_sha)
    if not isinstance(entries, list):
        raise ValueError("Expected directory listing for docs/projects")

    records: list[_ProposalRecord] = []
    skipped = 0

    for entry in tqdm(entries, desc="Merged project pages", unit="file"):
        project_path = entry.path
        if not project_path.startswith("docs/projects/"):
            continue

        if not project_path.endswith(".md"):
            continue

        try:
            source = docs_repo.get_contents(project_path, ref=commit_sha)
            if isinstance(source, list):
                raise ValueError(f"Expected file content, got directory listing for {project_path}")

            markdown_text = source.decoded_content.decode("utf-8")
        except GithubException as exc:
            print(f"WARNING merged {project_path}: failed to fetch: {exc}")
            skipped += 1
            continue
        except Exception as exc:
            print(f"WARNING merged {project_path}: failed to decode: {exc}")
            skipped += 1
            continue

        records.append(
            _ProposalRecord(
                project_path=project_path,
                markdown_text=markdown_text,
                source_label=f"merged {project_path}",
                commit_sha=commit_sha,
            )
        )

    return records, skipped


def _cache_pr_json_path(pr_number: int) -> Path:
    """Return cache path for a PR metadata JSON snapshot."""

    return _CACHE_DIR / f"pr_{pr_number}.json"


def _cache_project_file_path(pr_number: int, project_path: str, commit_sha: str) -> Path:
    """Return cache path for a proposal markdown file preserving original filename."""

    return _CACHE_DIR / f"pr_{pr_number}" / commit_sha / project_path


def _load_cached_pr(pr_number: int) -> tuple[dict[str, Any], list[str]] | None:
    """Load cached PR metadata and changed files for a PR number."""

    cache_path = _cache_pr_json_path(pr_number)
    if not cache_path.exists():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None

    pull_raw = payload.get("pull")
    changed_files_raw = payload.get("changed_files")
    pull_data = _as_string_key_dict(pull_raw)
    if pull_data is None or not isinstance(changed_files_raw, list):
        return None

    changed_files_any = cast(list[Any], changed_files_raw)
    changed_files = [item for item in changed_files_any if isinstance(item, str)]

    return pull_data, changed_files


def _save_cached_pr(pr_number: int, pull_raw: dict[str, Any], changed_files: list[str]) -> None:
    """Persist PR metadata and changed file list as pretty JSON."""

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _cache_pr_json_path(pr_number)
    payload = {
        "pull": pull_raw,
        "changed_files": changed_files,
    }
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_cached_markdown(pr_number: int, project_path: str, commit_sha: str) -> str | None:
    """Load cached markdown content if available."""

    cache_path = _cache_project_file_path(pr_number, project_path, commit_sha)
    if not cache_path.exists():
        return None

    try:
        return cache_path.read_text(encoding="utf-8")
    except OSError:
        return None


def _save_cached_markdown(pr_number: int, project_path: str, commit_sha: str, content_bytes: bytes) -> None:
    """Persist fetched markdown bytes preserving original filename."""

    cache_path = _cache_project_file_path(pr_number, project_path, commit_sha)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(content_bytes)


def _pull_head_sha_from_raw(pull_raw: dict[str, Any]) -> str | None:
    """Extract head SHA from cached pull raw payload."""

    head_raw = pull_raw.get("head")
    head = _as_string_key_dict(head_raw)
    if head is None:
        return None

    sha = head.get("sha")
    if not isinstance(sha, str) or not sha:
        return None

    return sha


def _parse_front_matter(markdown_text: str, *, source_label: str, source_path: str) -> tuple[dict[str, Any], str, bool]:
    """Parse YAML front matter and return ``(front_matter, markdown_body, has_front_matter)``."""

    if not markdown_text.startswith("---\n"):
        print(f"WARNING {source_label}: missing YAML front matter in {source_path}")

        return {}, markdown_text, False

    end_index = markdown_text.find("\n---\n", 4)
    if end_index == -1:
        print(f"WARNING {source_label}: malformed YAML front matter in {source_path}")

        return {}, markdown_text, False

    fm_text = markdown_text[4:end_index]
    body = markdown_text[end_index + len("\n---\n") :]

    loaded = _yaml_load(fm_text)
    parsed = _as_string_key_dict(loaded)
    if parsed is None:
        raise ValueError("Front matter must be a YAML mapping")

    return parsed, body, True


def _extract_plain_text(token: Any) -> str:
    """Extract plain text from a mistletoe token recursively."""

    if token is None:
        return ""

    if isinstance(token, LineBreak):
        if token.soft:
            return " "

        return "\n"

    if hasattr(token, "content") and isinstance(token.content, str):
        return token.content

    children = getattr(token, "children", None)
    if not children:
        return ""

    parts = [_extract_plain_text(child) for child in children]

    return "".join(parts)


def _table_rows_to_pairs(table_token: Table) -> list[tuple[str, str]]:
    """Convert mistletoe table rows to ``(key, value)`` pairs."""

    rows: list[tuple[str, str]] = []
    table_rows = table_token.children or []
    for row in table_rows:
        cells = getattr(row, "children", [])
        if not cells:
            continue

        rendered_cells = [" ".join(_extract_plain_text(cell).split()) for cell in cells]

        if len(rendered_cells) >= 2:
            key = rendered_cells[0]
            value = rendered_cells[1]
            rows.append((key, value))

    return rows


def _normalize_row_header(value: str) -> str:
    """Normalize table row labels for flexible matching."""

    normalized = value.strip().lower()
    normalized = normalized.replace("**", "")

    return " ".join(normalized.split())


def _extract_project_description_after_h1(doc: Document) -> str | None:
    """Extract first paragraph after first H1 heading and trim outer underscores."""

    blocks = list(doc.children or [])
    seen_h1 = False

    for block in blocks:
        if isinstance(block, Heading):
            level = getattr(block, "level", 0)
            if level == 1:
                seen_h1 = True
                continue

            if seen_h1 and level == 1:
                return None

        if seen_h1 and isinstance(block, Paragraph):
            text = " ".join(_extract_plain_text(block).split())
            if text.strip().startswith("<!--"):
                continue

            cleaned = re.sub(r"<!--\s*markdownlint-(?:disable|enable)\s+MD036\s*-->", "", text, flags=re.IGNORECASE)
            trimmed = cleaned.strip("_").strip()
            if trimmed:
                return trimmed

    return None


def _is_valid_url(url_value: str) -> bool:
    """Return whether a string is a valid absolute URL with scheme and host."""

    try:
        parsed = urlparse(url_value)
    except ValueError:
        return False

    return bool(parsed.scheme and parsed.netloc)


def _is_github_url(url_value: str) -> bool:
    """Return whether a URL points to github.com."""

    if not _is_valid_url(url_value):
        return False

    parsed = urlparse(url_value)

    return (parsed.netloc or "").lower() in {"github.com", "www.github.com"}


def _normalize_github_repo_url(url_value: str) -> str | None:
    """Normalize GitHub repo URL to canonical lowercase host/owner/repo form."""

    if not _is_github_url(url_value):
        return None

    parsed = urlparse(url_value)
    parts = [segment for segment in parsed.path.split("/") if segment]
    if len(parts) < 2:
        return None

    owner = parts[0]
    repo = parts[1]
    if repo.endswith(".git"):
        repo = repo[:-4]

    if not owner or not repo:
        return None

    normalized = f"https://github.com/{owner}/{repo}"
    if normalized.lower() == _DOCS_REPO_URL.lower():
        return None

    return normalized


def _extract_github_owner_url(repo_urls: list[str]) -> str | None:
    """Derive owner URL when all repository URLs share the same owner."""

    owners: set[str] = set()
    for repo_url in repo_urls:
        parsed = urlparse(repo_url)
        parts = [segment for segment in parsed.path.split("/") if segment]
        if len(parts) < 2:
            continue

        owners.add(parts[0])

    if len(owners) != 1:
        return None

    owner = next(iter(owners))

    return f"https://github.com/{owner}"


def _extract_markdown_fields(markdown_text: str, source_label: str, source_path: str) -> dict[str, Any]:
    """Extract candidate patch fields from proposal markdown content."""

    front_matter, body, has_front_matter = _parse_front_matter(
        markdown_text,
        source_label=source_label,
        source_path=source_path,
    )
    doc = Document(body)

    candidate: dict[str, Any] = {}
    candidate["_has_front_matter"] = has_front_matter

    title = front_matter.get("title")
    if isinstance(title, str) and title.strip():
        candidate["display_name"] = title.strip()

    front_category = front_matter.get("category")
    if isinstance(front_category, str) and front_category.strip():
        candidate["category"] = front_category.strip()

    description = _extract_project_description_after_h1(doc)
    if description:
        candidate.setdefault("metadata", {})
        candidate["metadata"]["description"] = description

    first_table: Table | None = None
    for block in doc.children or []:
        if isinstance(block, Table):
            first_table = block
            break

    if first_table is None:
        return candidate

    row_pairs = _table_rows_to_pairs(first_table)
    row_map: dict[str, str] = {}

    all_values: list[str] = []
    for raw_key, raw_value in row_pairs:
        key = _normalize_row_header(raw_key)
        value = raw_value.strip()
        if not key or not value:
            continue

        row_map[key] = value
        all_values.append(value)

    github_urls: list[str] = []
    for value in all_values:
        if normalized_repo := _normalize_github_repo_url(value):
            github_urls.append(normalized_repo)

    github_urls = list(dict.fromkeys(github_urls))

    if not github_urls:
        if repository_value := row_map.get("repository"):
            if fallback_repo := _normalize_github_repo_url(repository_value):
                github_urls = [fallback_repo]

    if github_urls:
        candidate["git_repo_urls"] = github_urls

    table_category = row_map.get("category")
    if (
        table_category
        and isinstance(front_category, str)
        and front_category.strip()
        and table_category.strip().lower() != front_category.strip().lower()
    ):
        print(
            f"WARNING {source_label}: category mismatch for {source_path}: "
            f"front matter={front_category!r}, table={table_category!r}"
        )

    website_value = row_map.get("website")
    if website_value:
        if _is_valid_url(website_value):
            candidate.setdefault("metadata", {})
            candidate["metadata"]["website"] = website_value
        else:
            print(f"WARNING {source_label}: invalid website value for {source_path}: {website_value!r}")

    first_released_value = row_map.get("first released")
    if first_released_value:
        candidate.setdefault("metadata", {})
        candidate["metadata"]["first_released"] = first_released_value

    return candidate


def _collect_existing_name_map(overrides: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Build normalized name to canonical key list mapping from existing overrides."""

    name_map: dict[str, list[str]] = {}
    for key in overrides:
        if not key.startswith(_DAOIP_PREFIX):
            continue

        suffix = key[len(_DAOIP_PREFIX) :]
        normalized = _normalize_alpha(suffix)
        if not normalized:
            continue

        name_map.setdefault(normalized, []).append(key)

    for values in name_map.values():
        values.sort()

    return name_map


def _get_existing_repo_overlap(
    existing_overrides: dict[str, dict[str, Any]],
    extracted_repo_urls: list[str],
) -> tuple[str | None, list[tuple[str, int]]]:
    """Return best canonical key by repo URL overlap and scored candidates."""

    extracted_set = {url for url in extracted_repo_urls if _normalize_github_repo_url(url)}
    if not extracted_set:
        return None, []

    scored: list[tuple[str, int]] = []
    for key, entry in existing_overrides.items():
        raw_urls_value = entry.get("git_repo_urls")
        if not isinstance(raw_urls_value, list):
            continue

        raw_urls_any = cast(list[Any], raw_urls_value)
        raw_urls = [url for url in raw_urls_any if isinstance(url, str)]

        existing_set = {normalized for url in raw_urls for normalized in [_normalize_github_repo_url(url)] if normalized}
        if not existing_set:
            continue

        overlap = len(existing_set.intersection(extracted_set))
        if overlap > 0:
            scored.append((key, overlap))

    if not scored:
        return None, []

    scored_sorted = sorted(scored, key=lambda item: (-item[1], item[0]))

    return scored_sorted[0][0], scored_sorted


def _merge_patch_entry(existing: dict[str, Any], candidate: dict[str, Any], *, source_tag: str) -> dict[str, Any]:
    """Merge extracted candidate into existing entry with non-overwrite semantics."""

    merged = dict(existing)

    if "display_name" not in merged:
        display_name = candidate.get("display_name")
        if isinstance(display_name, str) and display_name:
            merged["display_name"] = display_name

    if "activity_status" not in merged:
        merged["activity_status"] = "live"

    if "git_owner_url" not in merged:
        source_repo_urls: list[str] = []
        merged_repo_urls = merged.get("git_repo_urls")
        if isinstance(merged_repo_urls, list):
            merged_repo_urls_any = cast(list[Any], merged_repo_urls)
            source_repo_urls = [url for url in merged_repo_urls_any if isinstance(url, str)]
        else:
            extracted_repo_urls = candidate.get("git_repo_urls")
            if isinstance(extracted_repo_urls, list):
                extracted_repo_urls_any = cast(list[Any], extracted_repo_urls)
                source_repo_urls = [url for url in extracted_repo_urls_any if isinstance(url, str)]

        owner_url = _extract_github_owner_url(source_repo_urls)
        if owner_url is not None:
            merged["git_owner_url"] = owner_url
        else:
            merged["git_owner_url"] = None
            print(f"WARNING {source_tag}: git_owner_url ambiguous; set to null")

    extracted_repo_urls = candidate.get("git_repo_urls")
    if isinstance(extracted_repo_urls, list) and extracted_repo_urls:
        merged["git_repo_urls"] = extracted_repo_urls

    category = candidate.get("category")
    if isinstance(category, str) and category:
        merged["category"] = category

    candidate_metadata = _as_string_key_dict(candidate.get("metadata"))
    if candidate_metadata is not None:
        existing_metadata = merged.get("metadata")
        existing_metadata_dict = _as_string_key_dict(existing_metadata)
        metadata: dict[str, Any] = dict(existing_metadata_dict) if existing_metadata_dict is not None else {}

        description = candidate_metadata.get("description")
        if isinstance(description, str) and description:
            metadata["description"] = description

        website = candidate_metadata.get("website")
        if isinstance(website, str) and website:
            metadata["website"] = website

        first_released = candidate_metadata.get("first_released")
        if isinstance(first_released, str) and first_released:
            metadata["first_released"] = first_released

        if metadata:
            merged["metadata"] = metadata

    return merged


def _ensure_required_properties(entry: dict[str, Any], fallback_display_name: str) -> dict[str, Any]:
    """Ensure required override keys are present; use null for unknown values."""

    ensured = dict(entry)

    if "display_name" not in ensured:
        ensured["display_name"] = fallback_display_name or None

    if "activity_status" not in ensured:
        ensured["activity_status"] = "live"

    if "git_owner_url" not in ensured:
        ensured["git_owner_url"] = None

    return ensured


def _to_quoted_yaml_value(value: Any) -> Any:
    """Recursively convert string scalar values to double-quoted scalars."""

    if isinstance(value, str):
        return DoubleQuotedScalarString(value)

    if isinstance(value, list):
        values_any = cast(list[Any], value)

        return [_to_quoted_yaml_value(item) for item in values_any]

    if isinstance(value, dict):
        mapped = CommentedMap()
        raw_dict = cast(dict[Any, Any], value)
        for key, item in raw_dict.items():
            mapped[key] = _to_quoted_yaml_value(item)

        return mapped

    return value


def _order_entry_keys(entry: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of an override entry with canonical key ordering."""

    ordered: dict[str, Any] = {}
    for key in _ENTRY_KEY_ORDER:
        if key in entry:
            ordered[key] = entry[key]

    for key, value in entry.items():
        if key in ordered:
            continue

        ordered[key] = value

    return ordered


def _comment_lines(value: Any) -> Iterator[str]:
    """Unpack Ruamel comment slots, which can contain tokens or nested lists."""
    if isinstance(value, CommentToken):
        yield from value.value.splitlines()
    elif type(value) in (list, tuple):
        for item in value:
            yield from _comment_lines(item)


def _yaml_node_comments(node: Any, *, nested: bool = False) -> Iterator[str]:
    """Read comments attached to a YAML node and, if requested, its children."""
    if not isinstance(node, (CommentedMap, CommentedSeq)):
        return

    comments = getattr(node, "ca")
    yield from _comment_lines([comments.comment, comments.end, *comments.items.values()])
    if nested:
        if isinstance(node, CommentedMap):
            children = (_as_string_key_dict(node) or {}).values()
        else:
            children = getattr(node, "__iter__")()

        for child in children:
            yield from _yaml_node_comments(child, nested=True)


def _reviewer_rejections(template_map: CommentedMap) -> dict[str, set[str]]:
    """Read tombstones from metadata/list nodes without rendering YAML again."""
    pattern = re.compile(r"""\s*# reject:\s*(?P<quote>["']?)(?P<login>[A-Za-z0-9-]+)(?P=quote)\s*""")
    rejections: dict[str, set[str]] = {}
    for project_id, project in (_as_string_key_dict(template_map) or {}).items():
        if not isinstance(project, CommentedMap):
            continue

        metadata = (_as_string_key_dict(project) or {}).get("metadata")
        if not isinstance(metadata, CommentedMap):
            continue

        maintainers = (_as_string_key_dict(metadata) or {}).get("maintainers")
        comments = (*_yaml_node_comments(metadata), *_yaml_node_comments(maintainers, nested=True))
        for line in comments:
            marker = pattern.fullmatch(line)
            if marker:
                rejections.setdefault(project_id, set()).add(marker["login"].lower())

    return rejections


def _render_sorted_yaml(
    overrides: dict[str, dict[str, Any]],
    template_map: CommentedMap,
    review_comments: dict[str, list[dict[str, Any]]],
    review_notes: dict[str, list[str]] | None = None,
) -> str:
    """Render deterministic YAML sorted by DAOIP key using round-trip formatting."""

    sorted_keys = sorted(overrides)
    ordered_map = CommentedMap()
    cast(Any, ordered_map).ca.comment = cast(Any, template_map).ca.comment
    ordered_map["$schema"] = template_map["$schema"]

    for key in sorted_keys:
        ordered_entry = _order_entry_keys(overrides[key])
        rendered_entry = _to_quoted_yaml_value(ordered_entry)
        candidates = review_comments.get(key, [])
        notes = list((review_notes or {}).get(key, []))
        if candidates:
            snippets = StringIO()
            _yaml_dump(_to_quoted_yaml_value(candidates), snippets)
            lines = [line.removeprefix("  ") for line in snippets.getvalue().splitlines()]
            notes.extend(["Confirm these project-page candidates before uncommenting:", *lines])

        if notes:
            metadata = rendered_entry["metadata"]
            maintainers = CommentedSeq(metadata.get("maintainers", []))
            if maintainers:
                cast(Any, maintainers).yaml_set_comment_before_after_key(0, before="\n".join(notes), indent=6)
            else:
                metadata.yaml_set_comment_before_after_key("maintainers", before="\n".join(notes), indent=4)

            metadata["maintainers"] = maintainers

        ordered_map[key] = rendered_entry
        cast(Any, ordered_map).yaml_set_comment_before_after_key(key, before="\n")

    buffer = StringIO()
    _yaml_dump(ordered_map, buffer)
    rendered = buffer.getvalue()

    return rendered


def _slug_to_display_name(slug: str) -> str:
    """Build a human-readable fallback display name from a proposal slug."""

    cleaned = slug.replace("_", " ").replace("-", " ").strip()
    words = [word for word in cleaned.split() if word]

    if not words:
        return slug

    return " ".join(word.capitalize() for word in words)


def _is_valid_single_project_file(pr_file_paths: list[str]) -> tuple[bool, str | None]:
    """Validate the strict single changed-file rule for proposal PR selection."""

    if len(pr_file_paths) != 1:
        return False, None

    only_path = pr_file_paths[0]
    if not only_path.startswith("docs/projects/"):
        return False, None
    if not only_path.endswith(".md"):
        return False, None

    return True, only_path


if __name__ == "__main__":
    raise SystemExit(main())
