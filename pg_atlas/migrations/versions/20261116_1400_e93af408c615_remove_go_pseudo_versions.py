"""data: remove Go module pseudo-versions from release lists

Revision ID: e93af408c615
Revises: 8bfb033f2303
Create Date: 2026-11-16 14:00:00.000000+00:00

"""

import logging
import re
from typing import Sequence, Union

import msgspec
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e93af408c615"
down_revision: Union[str, Sequence[str], None] = "8bfb033f2303"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

logger = logging.getLogger("alembic.runtime.migration")

_GO_PSEUDO_VERSION_PATTERN = re.compile(
    r"^v[0-9]+\.(0\.0-|\d+\.\d+-([^+]*\.)?0\.)\d{14}-[A-Za-z0-9]+(\+[0-9A-Za-z-]+)?$",
    re.ASCII,
)
_SEMVER_PATTERN = re.compile(r"^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
_COMMIT_HASH_PATTERN = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


def _release_string(release: object, key: str) -> str:
    """Return a string field from a release payload, or an empty string."""
    try:
        release_dict = msgspec.convert(release, type=dict[str, object])
    except msgspec.ValidationError:
        return ""

    value = release_dict.get(key)

    return value if isinstance(value, str) else ""


def _is_go_pseudo_version(release: object) -> bool:
    """Return whether a JSON release payload is a Go module pseudo-version."""
    purl = _release_string(release, "purl")
    version = _release_string(release, "version")
    if not purl.startswith("pkg:"):
        return False

    purl_type = purl.removeprefix("pkg:").split("/", 1)[0].lower()

    return purl_type == "golang" and _GO_PSEUDO_VERSION_PATTERN.fullmatch(version) is not None


def _preferred_latest_version(releases: list[object]) -> str:
    """Apply the repository's current latest-version preference to cleaned releases."""
    ordered = sorted(releases, key=lambda release: _release_string(release, "release_date")[:19], reverse=True)
    newest_window = ordered[:9]

    for release in newest_window:
        version = _release_string(release, "version")
        if _SEMVER_PATTERN.fullmatch(version):
            return version

    for release in newest_window:
        version = _release_string(release, "version")
        if _COMMIT_HASH_PATTERN.fullmatch(version):
            return version

    for release in newest_window:
        version = _release_string(release, "version")
        if version:
            return version

    return ""


def _clean_release_table(bind: sa.Connection, table_name: str) -> tuple[int, int]:
    """Remove pseudo-versions from one concrete release table."""
    table = sa.table(
        table_name,
        sa.column("id", sa.Integer()),
        sa.column("releases", postgresql.JSONB()),
        sa.column("latest_version", sa.String()),
    )
    rows = bind.execute(sa.select(table.c.id, table.c.releases).where(table.c.releases.is_not(None)))
    updated_rows = 0
    removed_releases = 0

    for row in rows:
        try:
            releases = msgspec.convert(row.releases, type=list[object])
        except msgspec.ValidationError:
            continue

        retained: list[object] = [release for release in releases if not _is_go_pseudo_version(release)]
        removed_count = len(releases) - len(retained)
        if not removed_count:
            continue

        values: dict[str, object] = {"releases": retained}
        if retained:
            values["latest_version"] = _preferred_latest_version(retained)

        bind.execute(table.update().where(table.c.id == row.id).values(**values))
        updated_rows += 1
        removed_releases += removed_count

    return updated_rows, removed_releases


def upgrade() -> None:
    """Remove pseudo-versions from Repo and ExternalRepo release lists."""
    bind = op.get_bind()
    totals = [0, 0]

    for table_name in ("repos", "external_repos"):
        updated_rows, removed_releases = _clean_release_table(bind, table_name)
        totals[0] += updated_rows
        totals[1] += removed_releases

    logger.info(f"Go pseudo-version cleanup updated_rows={totals[0]} removed_releases={totals[1]}")


def downgrade() -> None:
    """Irreversible release-data cleanup."""
    pass
