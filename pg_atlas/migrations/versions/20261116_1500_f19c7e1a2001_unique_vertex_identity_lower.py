"""Reconcile case-only RepoVertex duplicates and enforce lowercase identity."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

import msgspec
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f19c7e1a2001"
down_revision = "e93af408c615"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")


def _render(value: Any) -> str:
    """Render a migration value compactly for decision logs."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _log_field_conflict(
    identity: str,
    field: str,
    kept_from_id: int,
    kept_value: Any,
    discarded_from_id: int,
    discarded_value: Any,
) -> None:
    """Log one conflicting value and its deterministic merge decision."""

    if kept_value != discarded_value:
        logger.info(
            f"RepoVertex merge field conflict identity={identity!r} field={field} "
            f"kept_from_id={kept_from_id} kept={_render(kept_value)} "
            f"discarded_from_id={discarded_from_id} discarded={_render(discarded_value)}"
        )


def _row_for_value(rows: list[dict[str, Any]], field: str, value: Any) -> dict[str, Any]:
    """Return the lowest-ID row carrying a selected field value."""

    return min((row for row in rows if row[field] == value), key=lambda row: row["id"])


def _preferred_row(rows: list[dict[str, Any]], field: str, *, prefer_non_null: bool = False) -> dict[str, Any]:
    """Select a field source from the survivor, optionally preferring a value."""

    candidates = rows
    if prefer_non_null and any(row[field] is not None for row in rows):
        candidates = [row for row in rows if row[field] is not None]

    return min(candidates, key=lambda row: row["id"])


def _maximum_row(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    """Select the highest non-null value, breaking ties by lowest ID."""

    candidates = [row for row in rows if row[field] is not None]
    if not candidates:
        return min(rows, key=lambda row: row["id"])

    maximum = max(row[field] for row in candidates)

    return _row_for_value(candidates, field, maximum)


def _sorted_value_row(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    """Select the lexicographically greatest value, matching the agreed version rule."""

    selected_value = rows[0][field]
    for row in rows[1:]:
        _, selected_value = sorted((selected_value, row[field]))

    return _row_for_value(rows, field, selected_value)


def _log_field_decision(
    identity: str,
    field: str,
    rows: list[dict[str, Any]],
    selected_row: dict[str, Any],
    selected_value: Any,
) -> None:
    """Log every discarded row value that differs from the selected value."""

    for row in rows:
        _log_field_conflict(identity, field, selected_row["id"], selected_value, row["id"], row[field])


def _merge_releases(rows: list[dict[str, Any]], identity: str) -> list[dict[str, Any]] | None:
    """Union release lists by PURL and version, filling a missing date from later rows."""

    merged: dict[tuple[str, str], dict[str, object]] = {}
    release_counts: list[tuple[int, int]] = []

    for vertex_row in rows:
        releases = msgspec.convert(vertex_row["releases"] or [], type=list[dict[str, object]])
        release_counts.append((vertex_row["id"], len(releases)))
        for release in releases:
            purl = release.get("purl")
            version = release.get("version")
            if not isinstance(purl, str) or not isinstance(version, str):
                raise ValueError(f"Invalid release entry for RepoVertex identity {identity!r}: {_render(release)}")

            key = (purl, version)
            existing = merged.get(key)
            if existing is None or (not existing.get("release_date") and release.get("release_date")):
                merged[key] = release

    result = sorted(merged.values(), key=lambda release: str(release.get("release_date", "")), reverse=True)
    raw_releases_differ = any(row["releases"] != rows[0]["releases"] for row in rows[1:])
    if raw_releases_differ or len(result) != release_counts[0][1]:
        logger.info(
            f"RepoVertex merge release counts identity={identity!r} source_counts={release_counts} merged_count={len(result)}"
        )

    return result or None


def _merge_metadata(rows: list[dict[str, Any]], identity: str) -> dict[str, object] | None:
    """Union metadata keys, keeping survivor values for conflicts and maxing per-PURL downloads."""

    merged: dict[str, object] = {}
    source_by_key: dict[str, int] = {}

    for vertex_row in rows:
        raw_metadata = vertex_row["metadata"]
        metadata = msgspec.convert(raw_metadata, type=dict[str, object]) if raw_metadata is not None else None
        if metadata is None:
            continue

        for key, value in metadata.items():
            existing = merged.get(key)
            if key not in merged:
                merged[key] = value
                source_by_key[key] = vertex_row["id"]
            elif key == "adoption_downloads_by_purl" and isinstance(existing, dict) and isinstance(value, dict):
                downloads = msgspec.convert(existing, type=dict[str, object])
                incoming_downloads = msgspec.convert(value, type=dict[str, object])
                download_source_ids = {purl: source_by_key[key] for purl in downloads}
                for purl, count in incoming_downloads.items():
                    if purl not in downloads:
                        downloads[purl] = count
                        download_source_ids[purl] = vertex_row["id"]
                        continue

                    if downloads[purl] == count:
                        continue

                    current = downloads[purl]
                    if isinstance(current, int) and isinstance(count, int) and count > current:
                        _log_field_conflict(
                            identity,
                            f"metadata.{key}.{purl}",
                            vertex_row["id"],
                            count,
                            download_source_ids[purl],
                            current,
                        )
                        downloads[purl] = count
                        download_source_ids[purl] = vertex_row["id"]
                    else:
                        _log_field_conflict(
                            identity,
                            f"metadata.{key}.{purl}",
                            download_source_ids[purl],
                            current,
                            vertex_row["id"],
                            count,
                        )

                merged[key] = downloads
            elif existing != value:
                _log_field_conflict(identity, f"metadata.{key}", source_by_key[key], existing, vertex_row["id"], value)

    return merged or None


def _merge_repo_group(connection: sa.Connection, identity: str, rows: list[dict[str, Any]]) -> None:
    """Merge fields from a same-identity Repo group into its lowest-ID row."""

    survivor = rows[0]
    selected: dict[str, tuple[dict[str, Any], Any]] = {}

    for field in ("display_name", "visibility"):
        source = _preferred_row(rows, field)
        selected[field] = (source, source[field])

    for field in ("repo_url", "project_id"):
        source = _preferred_row(rows, field, prefer_non_null=True)
        selected[field] = (source, source[field])

    for field in ("pony_factor", "criticality_score", "adoption_downloads", "adoption_stars", "adoption_forks"):
        source = _maximum_row(rows, field)
        selected[field] = (source, source[field])

    for field in ("latest_commit_date", "updated_at"):
        source = _maximum_row(rows, field)
        selected[field] = (source, source[field])

    source = _sorted_value_row(rows, "latest_version")
    selected["latest_version"] = (source, source["latest_version"])

    pushed_at_source = min(
        rows,
        key=lambda row: (
            row["pushed_at"] is None,
            -(row["pushed_at"].timestamp()) if row["pushed_at"] is not None else 0,
            -(row["pushed_at_observed_at"].timestamp()) if row["pushed_at_observed_at"] is not None else 0,
            row["id"],
        ),
    )
    selected["pushed_at"] = (pushed_at_source, pushed_at_source["pushed_at"])
    selected["pushed_at_observed_at"] = (pushed_at_source, pushed_at_source["pushed_at_observed_at"])

    for field, (source_row, value) in selected.items():
        _log_field_decision(identity, field, rows, source_row, value)

    releases = _merge_releases(rows, identity)
    metadata = _merge_metadata(rows, identity)
    repo_table = sa.table(
        "repos",
        sa.column("id", sa.Integer),
        sa.column("display_name", sa.String),
        sa.column("visibility", postgresql.ENUM(name="visibility", create_type=False)),
        sa.column("latest_version", sa.String),
        sa.column("project_id", sa.Integer),
        sa.column("latest_commit_date", sa.DateTime(timezone=True)),
        sa.column("pushed_at", sa.DateTime(timezone=True)),
        sa.column("pushed_at_observed_at", sa.DateTime(timezone=True)),
        sa.column("repo_url", sa.String),
        sa.column("pony_factor", sa.Integer),
        sa.column("criticality_score", sa.Integer),
        sa.column("adoption_downloads", sa.Integer),
        sa.column("adoption_stars", sa.Integer),
        sa.column("adoption_forks", sa.Integer),
        sa.column("releases", postgresql.JSONB),
        sa.column("metadata", postgresql.JSONB),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    values = {field: value for field, (_, value) in selected.items()}
    values.update(releases=releases, metadata=metadata)
    connection.execute(repo_table.update().where(repo_table.c.id == survivor["id"]).values(**values))


def _merge_external_repo_group(connection: sa.Connection, identity: str, rows: list[dict[str, Any]]) -> None:
    """Merge fields from a same-identity ExternalRepo group into its lowest-ID row."""

    survivor = rows[0]
    selected: dict[str, tuple[dict[str, Any], Any]] = {}
    for field in ("display_name",):
        source = _preferred_row(rows, field)
        selected[field] = (source, source[field])
    for field in ("repo_url",):
        source = _preferred_row(rows, field, prefer_non_null=True)
        selected[field] = (source, source[field])
    for field in ("criticality_score", "updated_at"):
        source = _maximum_row(rows, field)
        selected[field] = (source, source[field])
    source = _sorted_value_row(rows, "latest_version")
    selected["latest_version"] = (source, source["latest_version"])

    for field, (source_row, value) in selected.items():
        _log_field_decision(identity, field, rows, source_row, value)

    releases = _merge_releases(rows, identity)
    external_repo_table = sa.table(
        "external_repos",
        sa.column("id", sa.Integer),
        sa.column("display_name", sa.String),
        sa.column("latest_version", sa.String),
        sa.column("repo_url", sa.String),
        sa.column("criticality_score", sa.Integer),
        sa.column("releases", postgresql.JSONB),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    values = {field: value for field, (_, value) in selected.items()}
    values["releases"] = releases
    connection.execute(external_repo_table.update().where(external_repo_table.c.id == survivor["id"]).values(**values))


def _merge_redirect_group(identity: str, rows: list[dict[str, Any]]) -> None:
    """Log redirect payload conflicts; target IDs are remapped after all groups are known."""

    for field in ("target_vertex_id", "predicate"):
        source = _preferred_row(rows, field)
        _log_field_decision(identity, field, rows, source, source[field])


def _load_and_merge_vertex_groups(connection: sa.Connection) -> tuple[dict[int, int], dict[int, str]]:
    """Merge all duplicate identity groups and return old-to-survivor IDs and types."""

    rows = (
        connection.execute(
            sa.text("""
        WITH duplicate_identities AS (
            SELECT lower(canonical_id) AS identity_key
            FROM repo_vertices
            GROUP BY lower(canonical_id)
            HAVING count(*) > 1
        )
        SELECT
            lower(vertex.canonical_id) AS identity_key,
            vertex.id,
            vertex.canonical_id,
            vertex.vertex_type::text AS vertex_type,
            repo.display_name,
            repo.visibility::text AS visibility,
            repo.latest_version AS repo_latest_version,
            repo.project_id,
            repo.latest_commit_date,
            repo.pushed_at,
            repo.pushed_at_observed_at,
            repo.repo_url AS repo_url,
            repo.pony_factor,
            repo.criticality_score AS repo_criticality_score,
            repo.adoption_downloads,
            repo.adoption_stars,
            repo.adoption_forks,
            repo.releases AS repo_releases,
            repo.metadata,
            repo.updated_at AS repo_updated_at,
            external_repo.display_name AS external_display_name,
            external_repo.latest_version AS external_latest_version,
            external_repo.repo_url AS external_repo_url,
            external_repo.criticality_score AS external_criticality_score,
            external_repo.releases AS external_releases,
            external_repo.updated_at AS external_updated_at,
            redirect.target_vertex_id,
            redirect.predicate::text AS predicate
        FROM repo_vertices AS vertex
        JOIN duplicate_identities ON duplicate_identities.identity_key = lower(vertex.canonical_id)
        LEFT JOIN repos AS repo ON repo.id = vertex.id
        LEFT JOIN external_repos AS external_repo ON external_repo.id = vertex.id
        LEFT JOIN redirect_vertices AS redirect ON redirect.id = vertex.id
        ORDER BY identity_key, vertex.id
        """)
        )
        .mappings()
        .all()
    )

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result_row in rows:
        groups[result_row["identity_key"]].append(dict(result_row))

    id_map: dict[int, int] = {}
    vertex_types: dict[int, str] = {}
    for identity, group in groups.items():
        survivor = group[0]
        survivor_id = survivor["id"]
        vertex_type = survivor["vertex_type"]
        types = {vertex_row["vertex_type"] for vertex_row in group}
        logger.info(
            f"RepoVertex identity conflict identity={identity!r} keep_id={survivor_id} "
            f"keep_canonical_id={survivor['canonical_id']!r} "
            f"merge_ids={[vertex_row['id'] for vertex_row in group[1:]]} vertex_type={vertex_type}"
        )

        if len(types) != 1:
            raise ValueError(f"Cannot automatically merge mixed RepoVertex types for {identity!r}: {sorted(types)}")

        for vertex_row in group:
            id_map[vertex_row["id"]] = survivor_id
            vertex_types[vertex_row["id"]] = vertex_type
            if vertex_row["id"] != survivor_id:
                _log_field_conflict(
                    identity,
                    "canonical_id",
                    survivor_id,
                    survivor["canonical_id"],
                    vertex_row["id"],
                    vertex_row["canonical_id"],
                )

        if vertex_type == "repo":
            for vertex_row in group:
                vertex_row.update(
                    latest_version=vertex_row.pop("repo_latest_version"),
                    criticality_score=vertex_row.pop("repo_criticality_score"),
                    releases=vertex_row.pop("repo_releases"),
                    updated_at=vertex_row.pop("repo_updated_at"),
                )
            _merge_repo_group(connection, identity, group)
        elif vertex_type == "external-repo":
            for vertex_row in group:
                vertex_row.update(
                    display_name=vertex_row.pop("external_display_name"),
                    latest_version=vertex_row.pop("external_latest_version"),
                    repo_url=vertex_row.pop("external_repo_url"),
                    criticality_score=vertex_row.pop("external_criticality_score"),
                    releases=vertex_row.pop("external_releases"),
                    updated_at=vertex_row.pop("external_updated_at"),
                )
            _merge_external_repo_group(connection, identity, group)
        elif vertex_type == "redirect":
            _merge_redirect_group(identity, group)
        else:
            raise ValueError(f"Unsupported RepoVertex type for {identity!r}: {vertex_type!r}")

    return id_map, vertex_types


def _mapped_id(vertex_id: int, id_map: dict[int, int]) -> int:
    """Return the survivor ID for a conflicting vertex, or the original ID."""

    return id_map.get(vertex_id, vertex_id)


def _reconcile_depends_on(connection: sa.Connection, id_map: dict[int, int]) -> None:
    """Repoint affected dependency edges, drop self-loops, and prefer verified evidence."""

    affected_ids = sorted(id_map)
    if not affected_ids:
        return

    edge_rows = (
        connection.execute(
            sa.text("""
            SELECT in_vertex_id, out_vertex_id, confidence::text AS confidence, version_range
            FROM depends_on
            WHERE in_vertex_id IN :ids OR out_vertex_id IN :ids
            ORDER BY in_vertex_id, out_vertex_id
            """).bindparams(sa.bindparam("ids", expanding=True)),
            {"ids": affected_ids},
        )
        .mappings()
        .all()
    )
    if not edge_rows:
        return

    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for edge_row in edge_rows:
        mapped_source = _mapped_id(edge_row["in_vertex_id"], id_map)
        mapped_target = _mapped_id(edge_row["out_vertex_id"], id_map)
        grouped[(mapped_source, mapped_target)].append(dict(edge_row))

    selected_edges: list[dict[str, Any]] = []
    for (mapped_source, mapped_target), edges in grouped.items():
        if mapped_source == mapped_target:
            for edge_data in edges:
                logger.info(
                    f"DependsOn self-loop discarded original=({edge_data['in_vertex_id']},{edge_data['out_vertex_id']}) "
                    f"mapped=({mapped_source},{mapped_target}) confidence={edge_data['confidence']} "
                    f"version_range={edge_data['version_range']!r}"
                )
            continue

        kept = min(
            edges,
            key=lambda edge: (
                0 if edge["confidence"] == "verified-sbom" else 1,
                0 if edge["in_vertex_id"] == mapped_source else 1,
                0 if edge["out_vertex_id"] == mapped_target else 1,
                edge["in_vertex_id"],
                edge["out_vertex_id"],
            ),
        )
        if len(edges) > 1:
            logger.info(
                f"DependsOn collision mapped=({mapped_source},{mapped_target}) "
                f"kept_original=({kept['in_vertex_id']},{kept['out_vertex_id']}) "
                f"kept_confidence={kept['confidence']} kept_version_range={kept['version_range']!r}"
            )
            for edge_data in edges:
                if edge_data is not kept:
                    logger.info(
                        f"DependsOn edge discarded original=({edge_data['in_vertex_id']},{edge_data['out_vertex_id']}) "
                        f"mapped=({mapped_source},{mapped_target}) confidence={edge_data['confidence']} "
                        f"version_range={edge_data['version_range']!r}"
                    )
        elif (kept["in_vertex_id"], kept["out_vertex_id"]) != (mapped_source, mapped_target):
            logger.info(
                f"DependsOn edge repointed original=({kept['in_vertex_id']},{kept['out_vertex_id']}) "
                f"mapped=({mapped_source},{mapped_target}) confidence={kept['confidence']} "
                f"version_range={kept['version_range']!r}"
            )

        if kept["confidence"] == "verified-sbom" and (kept["in_vertex_id"], kept["out_vertex_id"]) != (
            mapped_source,
            mapped_target,
        ):
            logger.info(
                f"DependsOn verified-sbom edge repointed original=({kept['in_vertex_id']},{kept['out_vertex_id']}) "
                f"mapped=({mapped_source},{mapped_target}) version_range={kept['version_range']!r}"
            )

        selected_edges.append(
            {
                "in_vertex_id": mapped_source,
                "out_vertex_id": mapped_target,
                "confidence": kept["confidence"],
                "version_range": kept["version_range"],
            }
        )

    edge_table = sa.table(
        "depends_on",
        sa.column("in_vertex_id", sa.Integer),
        sa.column("out_vertex_id", sa.Integer),
        sa.column("confidence", postgresql.ENUM(name="edge_confidence", create_type=False)),
        sa.column("version_range", sa.String),
    )
    connection.execute(
        edge_table.delete().where(
            sa.or_(edge_table.c.in_vertex_id.in_(affected_ids), edge_table.c.out_vertex_id.in_(affected_ids))
        )
    )
    if selected_edges:
        connection.execute(edge_table.insert(), selected_edges)


def _reconcile_contributed_to(connection: sa.Connection, repo_id_map: dict[int, int]) -> None:
    """Repoint contribution edges, max the window count, and retain the full date span."""

    affected_ids = sorted(repo_id_map)
    if not affected_ids:
        return

    rows = (
        connection.execute(
            sa.text("""
            SELECT contributor_id, repo_id, number_of_commits, first_commit_date, last_commit_date
            FROM contributed_to
            WHERE repo_id IN :ids
            ORDER BY contributor_id, repo_id
            """).bindparams(sa.bindparam("ids", expanding=True)),
            {"ids": affected_ids},
        )
        .mappings()
        .all()
    )
    if not rows:
        return

    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mapped_repo_id = _mapped_id(row["repo_id"], repo_id_map)
        grouped[(row["contributor_id"], mapped_repo_id)].append(dict(row))

    selected_edges: list[dict[str, Any]] = []
    for (contributor_id, mapped_repo_id), edges in grouped.items():
        kept = min(edges, key=lambda edge: (edge["repo_id"] != mapped_repo_id, edge["repo_id"]))
        commits = max(edge["number_of_commits"] for edge in edges)
        first_date = min(edge["first_commit_date"] for edge in edges)
        last_date = max(edge["last_commit_date"] for edge in edges)
        if len(edges) > 1:
            logger.info(
                f"ContributedTo collision contributor={contributor_id} merged_repo={mapped_repo_id} "
                f"retained_edge_source_repo={kept['repo_id']} result_count={commits} "
                f"result_first={first_date} result_last={last_date}"
            )
            for edge in edges:
                disposition = "retained_source" if edge is kept else "discarded_source"
                logger.info(
                    f"ContributedTo edge {disposition} contributor={contributor_id} original_repo={edge['repo_id']} "
                    f"number_of_commits={edge['number_of_commits']} first={edge['first_commit_date']} "
                    f"last={edge['last_commit_date']}"
                )

            field_decisions = (
                ("number_of_commits", max(edges, key=lambda edge: edge["number_of_commits"]), commits, "max"),
                ("first_commit_date", min(edges, key=lambda edge: edge["first_commit_date"]), first_date, "min"),
                ("last_commit_date", max(edges, key=lambda edge: edge["last_commit_date"]), last_date, "max"),
            )
            for field, field_source, kept_value, policy in field_decisions:
                for edge in edges:
                    if edge[field] != kept_value:
                        logger.info(
                            f"ContributedTo field conflict contributor={contributor_id} merged_repo={mapped_repo_id} "
                            f"field={field} policy={policy} kept_from_repo={field_source['repo_id']} "
                            f"kept={kept_value} discarded_from_repo={edge['repo_id']} discarded={edge[field]}"
                        )
        elif kept["repo_id"] != mapped_repo_id:
            logger.info(
                f"ContributedTo edge repointed contributor={contributor_id} "
                f"original_repo={kept['repo_id']} mapped_repo={mapped_repo_id}"
            )

        selected_edges.append(
            {
                "contributor_id": contributor_id,
                "repo_id": mapped_repo_id,
                "number_of_commits": commits,
                "first_commit_date": first_date,
                "last_commit_date": last_date,
            }
        )

    contributed_table = sa.table(
        "contributed_to",
        sa.column("contributor_id", sa.Integer),
        sa.column("repo_id", sa.Integer),
        sa.column("number_of_commits", sa.Integer),
        sa.column("first_commit_date", sa.DateTime(timezone=True)),
        sa.column("last_commit_date", sa.DateTime(timezone=True)),
    )
    connection.execute(contributed_table.delete().where(contributed_table.c.repo_id.in_(affected_ids)))
    connection.execute(contributed_table.insert(), selected_edges)


def _reconcile_observations(connection: sa.Connection, repo_id_map: dict[int, int]) -> None:
    """Repoint dependent-observation references and merge any duplicate source/key rows."""

    affected_ids = sorted(repo_id_map)
    if not affected_ids:
        return

    rows = (
        connection.execute(
            sa.text("""
            SELECT id, source_repo_id, dependent_key, dependent_canonical_id, observed_owner,
                   observed_repo_name, dependent_repo_url, resolved_repo_id, package_ids,
                   last_seen_run_id, retired_at, retired_by_run_id, first_seen_at, last_seen_at
            FROM github_dependent_observations
            WHERE source_repo_id IN :ids
            ORDER BY id
            """).bindparams(sa.bindparam("ids", expanding=True)),
            {"ids": affected_ids},
        )
        .mappings()
        .all()
    )

    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        mapped_source = _mapped_id(row["source_repo_id"], repo_id_map)
        grouped[(mapped_source, row["dependent_key"])].append(dict(row))

    merged_rows: list[dict[str, Any]] = []
    for (mapped_source, dependent_key), observations in grouped.items():
        latest_seen = max(observations, key=lambda row: (row["last_seen_at"], row["id"]))
        latest_retirement = max(
            (row for row in observations if row["retired_at"] is not None),
            key=lambda row: (row["retired_at"], row["id"]),
            default=None,
        )
        is_retired = latest_retirement is not None and latest_retirement["retired_at"] >= latest_seen["last_seen_at"]
        package_ids: list[str] = []
        has_package_ids = False
        for observation in observations:
            if observation["package_ids"] is not None:
                has_package_ids = True
                package_ids.extend(msgspec.convert(observation["package_ids"], type=list[str]))
        unique_package_ids = sorted(set(package_ids)) if has_package_ids else None
        resolved_ids = sorted(
            {_mapped_id(row["resolved_repo_id"], repo_id_map) for row in observations if row["resolved_repo_id"] is not None}
        )
        if len(observations) > 1:
            keeper = min(observations, key=lambda row: row["id"])
            logger.info(
                f"GitHub dependent observation collision source_repo={mapped_source} dependent={dependent_key!r} "
                f"keep_id={keeper['id']} discard_ids={[row['id'] for row in observations if row is not keeper]} "
                f"merged_package_ids={unique_package_ids} first_seen={min(row['first_seen_at'] for row in observations)} "
                f"last_seen={max(row['last_seen_at'] for row in observations)}"
            )
            observation_id = keeper["id"]
        else:
            observation_id = observations[0]["id"]

        merged_rows.append(
            {
                "id": observation_id,
                "source_repo_id": mapped_source,
                "dependent_key": dependent_key,
                "dependent_canonical_id": latest_seen["dependent_canonical_id"],
                "observed_owner": latest_seen["observed_owner"],
                "observed_repo_name": latest_seen["observed_repo_name"],
                "dependent_repo_url": latest_seen["dependent_repo_url"],
                "resolved_repo_id": resolved_ids[0] if resolved_ids else None,
                "package_ids": unique_package_ids,
                "last_seen_run_id": latest_seen["last_seen_run_id"],
                "retired_at": latest_retirement["retired_at"] if is_retired and latest_retirement else None,
                "retired_by_run_id": latest_retirement["retired_by_run_id"] if is_retired and latest_retirement else None,
                "first_seen_at": min(row["first_seen_at"] for row in observations),
                "last_seen_at": max(row["last_seen_at"] for row in observations),
            }
        )

    observation_table = sa.table(
        "github_dependent_observations",
        sa.column("id", sa.Integer),
        sa.column("source_repo_id", sa.Integer),
        sa.column("dependent_key", sa.String),
        sa.column("dependent_canonical_id", sa.String),
        sa.column("observed_owner", sa.String),
        sa.column("observed_repo_name", sa.String),
        sa.column("dependent_repo_url", sa.String),
        sa.column("resolved_repo_id", sa.Integer),
        sa.column("package_ids", postgresql.JSONB),
        sa.column("last_seen_run_id", sa.Integer),
        sa.column("retired_at", sa.DateTime(timezone=True)),
        sa.column("retired_by_run_id", sa.Integer),
        sa.column("first_seen_at", sa.DateTime(timezone=True)),
        sa.column("last_seen_at", sa.DateTime(timezone=True)),
    )
    connection.execute(observation_table.delete().where(observation_table.c.source_repo_id.in_(affected_ids)))
    if merged_rows:
        connection.execute(observation_table.insert(), merged_rows)

    for old_id, new_id in repo_id_map.items():
        if old_id != new_id:
            connection.execute(
                observation_table.update()
                .where(observation_table.c.resolved_repo_id == old_id)
                .values(resolved_repo_id=new_id)
            )


def _repoint_simple_references(
    connection: sa.Connection,
    table_name: str,
    column_name: str,
    id_map: dict[int, int],
) -> None:
    """Repoint one scalar foreign-key column and log how many rows changed."""

    for old_id, new_id in id_map.items():
        if old_id != new_id:
            result = connection.execute(
                sa.text(f"UPDATE {table_name} SET {column_name} = :new_id WHERE {column_name} = :old_id"),
                {"old_id": old_id, "new_id": new_id},
            )
            if result.rowcount:
                logger.info(
                    f"RepoVertex references repointed table={table_name} column={column_name} "
                    f"old_id={old_id} new_id={new_id} rows={result.rowcount}"
                )


def upgrade() -> None:
    """Merge case-only RepoVertex duplicates, repoint references, then create the unique index."""

    connection = op.get_bind()
    id_map, vertex_types = _load_and_merge_vertex_groups(connection)
    if id_map:
        _reconcile_depends_on(connection, id_map)

        repo_id_map = {old_id: new_id for old_id, new_id in id_map.items() if vertex_types[old_id] == "repo"}
        _reconcile_contributed_to(connection, repo_id_map)
        _reconcile_observations(connection, repo_id_map)
        _repoint_simple_references(connection, "github_dependents_crawl_runs", "source_repo_id", repo_id_map)
        _repoint_simple_references(connection, "gitlog_artifacts", "repo_vertex_id", id_map)
        _repoint_simple_references(connection, "redirect_vertices", "target_vertex_id", id_map)

        losers = [(old_id, new_id, vertex_types[old_id]) for old_id, new_id in id_map.items() if old_id != new_id]
        for old_id, new_id, vertex_type in sorted(losers):
            child_table = {"repo": "repos", "external-repo": "external_repos", "redirect": "redirect_vertices"}[vertex_type]
            connection.execute(sa.text(f"DELETE FROM {child_table} WHERE id = :id"), {"id": old_id})
            connection.execute(sa.text("DELETE FROM repo_vertices WHERE id = :id"), {"id": old_id})
            logger.info(f"RepoVertex duplicate deleted id={old_id} merged_into={new_id} type={vertex_type}")

    remaining = connection.scalar(
        sa.text("""
            SELECT count(*)
            FROM (
                SELECT lower(canonical_id)
                FROM repo_vertices
                GROUP BY lower(canonical_id)
                HAVING count(*) > 1
            ) AS duplicate_groups
            """)
    )
    if remaining:
        raise RuntimeError(f"RepoVertex identity reconciliation left {remaining} duplicate lowercase groups")

    op.create_index("uq_repo_vertices_canonical_id_lower", "repo_vertices", [sa.text("lower(canonical_id)")], unique=True)
    logger.info("Created uq_repo_vertices_canonical_id_lower after successful identity reconciliation")


def downgrade() -> None:
    """Remove lowercase uniqueness; the data reconciliation is intentionally irreversible."""

    op.drop_index("uq_repo_vertices_canonical_id_lower", table_name="repo_vertices")
