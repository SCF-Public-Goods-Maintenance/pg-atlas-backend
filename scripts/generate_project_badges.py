"""
Generate Shields.io badges for one or multiple project canonical_ids.

Usage:
    uv run python scripts/generate_project_badges.py CANONICAL_ID [CANONICAL_ID ...]

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import argparse
import urllib.parse

# Default namespace prefix if not supplied by the user
DEFAULT_PREFIX = "daoip-5:scf:project:"

# ---------------------------------------------------------------------------
# Configuration & Color Palette
# ---------------------------------------------------------------------------
DEFAULT_PALETTE = [
    "914CFF",  # PG Atlas (Brand Purple)
    "00B578",  # 90d Contributors (Emerald Green)
    "E5484D",  # Criticality (Coral Red)
    "0090FF",  # Pony Factor (Blue)
    "FF9900",  # Adoption (Orange)
]

# Badge Configurations: (Label, JSONPath Query)
BADGE_SPECS = [
    ("PG Atlas", "$.activity_status"),
    ("90d Contributors", "$.active_contributors_90d"),
    ("Criticality", "$.criticality_score"),
    ("Pony Factor", "$.pony_factor"),
    ("Adoption", "$.adoption_score"),
]


def normalize_canonical_id(raw_id: str, prefix: str = DEFAULT_PREFIX) -> str:
    """Ensure the canonical_id has the required prefix."""
    raw_id = raw_id.strip()
    if not raw_id.startswith(prefix):
        return f"{prefix}{raw_id}"
    return raw_id


def build_shields_badge_markdown(canonical_id: str, badge_idx: int, palette: list[str]) -> str:
    label, query = BADGE_SPECS[badge_idx]
    color = palette[badge_idx % len(palette)]

    # 1. Target API Endpoint
    api_url = f"https://api.pgatlas.xyz/projects/{canonical_id}"

    # 2. Shields.io Query Parameters
    params = {
        "url": api_url,
        "query": query,
        "label": label,
        "color": color,
    }

    # 3. Construct Shields.io Dynamic JSON Endpoint URL
    shields_url = f"https://img.shields.io/badge/dynamic/json?{urllib.parse.urlencode(params)}"

    # 4. Construct Destination Link (Web Project Page)
    encoded_id_for_link = urllib.parse.quote(canonical_id, safe="")
    link_url = f"https://www.pgatlas.xyz/projects/{encoded_id_for_link}"

    # 5. Return Markdown Image-Link pair
    return f"[![{label}]({shields_url})]({link_url})"


def generate_badge_row(canonical_id: str, palette: list[str]) -> str:
    badges = [build_shields_badge_markdown(canonical_id, i, palette) for i in range(len(BADGE_SPECS))]
    return "\n".join(badges)


def main():
    parser = argparse.ArgumentParser(description="Generate Markdown Shields.io badge rows for PG Atlas projects.")
    parser.add_argument(
        "canonical_ids",
        metavar="CANONICAL_ID",
        nargs="+",
        help="One or more project IDs (with or without 'daoip-5:scf:project:' prefix)",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=f"Prefix to auto-prepend if missing (default: '{DEFAULT_PREFIX}')",
    )

    args = parser.parse_args()

    badge_rows: list[str] = []
    for raw_id in args.canonical_ids:
        canonical_id = normalize_canonical_id(raw_id, prefix=args.prefix)
        badge_rows.append(generate_badge_row(canonical_id, DEFAULT_PALETTE))

    # Print newline-separated badges, separated by a blank line between project args
    print("\n\n".join(badge_rows))


if __name__ == "__main__":
    main()
