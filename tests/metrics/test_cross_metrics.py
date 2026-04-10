"""
Cross-metrics tests:
- active subgraph projection (A6) -> criticality (A9).

These tests run against the **real local dev-DB snapshot** and are always
skipped on CI (which runs against an empty database).  They exist to verify
that the metrics pipeline produces expected values on the known dataset, not
to test the data itself.

Skip policy
-----------
All tests in this module check for the presence of five sentinel repos at the
start of every test function (via an autouse fixture).  If any sentinel is
absent the entire test is skipped.  Because CI uses an empty database, these
tests never run there.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import networkx as nx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pg_atlas.db_models.repo_vertex import RepoVertex
from pg_atlas.metrics.active_subgraph import project_active_subgraph
from pg_atlas.metrics.criticality import compute_criticality
from pg_atlas.metrics.graph_builder import build_dependency_graph

# ---------------------------------------------------------------------------
# Known-data constants (derived from psql queries against the March 2026 dump)
# ---------------------------------------------------------------------------

# Five repos whose presence certifies a real DB snapshot is loaded.
#   pkg:github/stellarcarbon/sc-audit     → live   (seed: in-degree 0 in dep graph)
#   pkg:github/StellarCN/py-stellar-base  → live   (reachable: 7 live/in-dev repos depend on it)
#   pkg:github/Consulting-Manao/tansu     → NULL   (orphan — no parent project, since move to org)
#   pkg:github/Soneso/stellar-ios-mac-sdk → NULL   (orphan)
#   pkg:github/withObsrvr/nebu            → NULL   (orphan)
SENTINEL_CANONICAL_IDS: frozenset[str] = frozenset(
    {
        "pkg:github/stellarcarbon/sc-audit",
        "pkg:github/StellarCN/py-stellar-base",
        "pkg:github/Consulting-Manao/tansu",
        "pkg:github/Soneso/stellar-ios-mac-sdk",
        "pkg:github/withObsrvr/nebu",
    }
)

# SELECT rv.canonical_id, count(*) AS n
# FROM repo_vertices rv JOIN depends_on d ON d.out_vertex_id = rv.id
# JOIN repos src ON src.id = d.in_vertex_id
# JOIN projects p ON p.id = src.project_id
# WHERE p.activity_status IN ('live','in-dev')
# GROUP BY rv.canonical_id ORDER BY n DESC LIMIT 10;
#
# These are direct active dependents; transitive criticality >= these values.
_KNOWN_DIRECT_ACTIVE_DEPENDENTS: dict[str, int] = {
    "pkg:golang/github.com/stretchr/testify": 36,
    "pkg:npm/axios": 34,
    "pkg:cargo/serde": 22,
    "pkg:cargo/serde_json": 20,
    "pkg:golang/github.com/spf13/cobra": 18,
    "pkg:npm/bignumber.js": 17,
    "pkg:cargo/tokio": 17,
    "pkg:golang/github.com/ethereum/go-ethereum": 16,
    "pkg:npm/dotenv": 15,
    "pkg:cargo/thiserror": 15,
}

# SELECT dst_rv.canonical_id, count(*) AS n
# FROM repo_vertices dst_rv JOIN repos dst_r ON dst_r.id = dst_rv.id
# JOIN depends_on d ON d.out_vertex_id = dst_rv.id
# JOIN repos src ON src.id = d.in_vertex_id
# JOIN projects p ON p.id = src.project_id
# WHERE p.activity_status IN ('live','in-dev')
# GROUP BY dst_rv.canonical_id ORDER BY n DESC LIMIT 10;
#
# Restricted to Repos (intra-Stellar Repo→Repo dependencies).
_KNOWN_REPO_DIRECT_ACTIVE_DEPENDENTS: dict[str, int] = {
    "pkg:github/StellarCN/py-stellar-base": 7,
    "pkg:github/OneKeyHQ/pynacl": 4,
    "pkg:github/bandprotocol/bandchain": 3,
    "pkg:github/agnosticeng/mapstructure-hooks": 3,
    "pkg:github/Beans-BV/dotnet-stellar-sdk": 3,
    "pkg:github/omeganetwork-tech/hyper": 3,
    "pkg:github/bandprotocol/chain": 2,
    "pkg:github/diadata-org/diadata": 2,
    "pkg:github/thehubdao/portal-contracts": 2,
    "pkg:github/bandprotocol/go-owasm": 2,
}

# A discontinued repo with no dependency edges — cannot appear in G_active.
_INACTIVE_ISOLATED_REPO = "pkg:github/PlutoDAO/gov"

# ---------------------------------------------------------------------------
# Skip guard fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def require_real_db_with_sentinel_data(db_session: AsyncSession) -> None:
    """
    Skip the test when the 5 sentinel repos are not all present in the DB.

    Depends on ``db_session``, which itself skips when PG_ATLAS_DATABASE_URL is
    unset — so CI with an empty DB never reaches this fixture body.
    """

    result = await db_session.execute(
        select(RepoVertex.canonical_id).where(RepoVertex.canonical_id.in_(SENTINEL_CANONICAL_IDS))
    )
    found: frozenset[str] = frozenset(row[0] for row in result)

    if found != SENTINEL_CANONICAL_IDS:
        pytest.skip(f"Sentinel repos not in DB: {SENTINEL_CANONICAL_IDS - found}")


# ---------------------------------------------------------------------------
# Test: active subgraph projection on real data
# ---------------------------------------------------------------------------


async def test_db_graph_projection_uses_project_activity_status(db_session: AsyncSession) -> None:
    """
    Build the full DB-backed dependency graph and project the A6 active subgraph.

    Verifies the following properties of the March 2026 dataset:

    1. G_active contains at least 2 000 nodes — safe floor given 2 108 active
       repos (1 427 live + 681 in-dev) with ~98.7 % in-degree 0 (seeds).
    2. G_active is strictly smaller than G — inactive repos (466 discontinued
       + 60 non-responsive) are pruned.
    3. All nodes in G_active are dep-layer vertices (Repo or ExternalRepo).
    4. The internal bookkeeping metadata set by project_active_subgraph()
       is consistent with the actual graph size.
    5. Two known live repos appear in G_active:
       - sc-audit (seed: in-degree 0, activity_status=live)
       - py-stellar-base (reachable: 7 live Repos depend on it directly)
    6. A known isolated discontinued repo (PlutoDAO/gov) is absent.
    """

    G = await build_dependency_graph(db_session)
    G_active = project_active_subgraph(G)

    # 1. Size lower bound.
    assert G_active.number_of_nodes() >= 2000, f"Expected >= 2000 active-subgraph nodes; got {G_active.number_of_nodes()}"

    # 2. Pruning of inactive repos.
    assert G_active.number_of_nodes() < G.number_of_nodes(), (
        "G_active must be a strict subgraph of G (inactive repos exist in the dataset)"
    )

    # 3. Only dep-layer vertices.
    for node, data in G_active.nodes(data=True):
        assert data.get("vertex_type") in (
            "Repo",
            "ExternalRepo",
        ), f"Unexpected vertex_type for {node!r}: {data.get('vertex_type')!r}"

    # 4. Metadata bookkeeping.
    assert G_active.graph["nodes_retained"] == G_active.number_of_nodes()

    # 5. Known live repos are present.
    assert "pkg:github/stellarcarbon/sc-audit" in G_active, "sc-audit (live, in-degree 0) must be an active-subgraph seed"
    assert "pkg:github/StellarCN/py-stellar-base" in G_active, (
        "py-stellar-base (live, 7 live-project dependents) must be reachable in the active subgraph"
    )

    # 6. Isolated inactive repo is absent.
    assert _INACTIVE_ISOLATED_REPO not in G_active, (
        f"{_INACTIVE_ISOLATED_REPO!r} (discontinued, no dep edges) must be absent from G_active"
    )


# ---------------------------------------------------------------------------
# Test: criticality scores on the active subgraph
# ---------------------------------------------------------------------------


async def test_active_criticality(db_session: AsyncSession) -> None:
    """
    Compute A9 transitive criticality on the A6 active subgraph.

    Verifies the following properties of the April 2026 dataset:

    1. Every node in G_active receives a criticality score.
    2. The maximum score exceeds 20 (the dataset has well-known hub packages
       with 36+ direct active dependents; transitive counts are higher).
    3. Chain invariant: for each package in _KNOWN_DIRECT_ACTIVE_DEPENDENTS,
       the transitive criticality score is >= its known direct count.
    4. Ranking: the top transitive critical package is pkg:npm/axios (direct count 34).
    """

    G = await build_dependency_graph(db_session)
    G_active = project_active_subgraph(G)
    criticality = compute_criticality(G_active)

    # 1. Score coverage matches active subgraph size.
    assert len(criticality) >= 2000, f"Expected scores for >= 2000 nodes; got {len(criticality)}"

    # 2. High-criticality packages exist in the dataset.
    assert max(criticality.values()) > 20, f"Expected max criticality > 20; got {max(criticality.values())}"

    # 3. Chain invariant: transitive >= direct for every known package.
    for pkg, known_direct in _KNOWN_DIRECT_ACTIVE_DEPENDENTS.items():
        assert pkg in criticality, f"{pkg!r} has {known_direct} direct active dependents but is absent from criticality scores"

        score = criticality[pkg]
        assert score >= known_direct, (
            f"{pkg!r}: transitive criticality {score} < known direct count {known_direct} (invariant violated)"
        )

    # 4. Ranking: top transitive critical package.
    top_by_transitive = max(
        (pkg for pkg in _KNOWN_DIRECT_ACTIVE_DEPENDENTS if pkg in criticality),
        key=lambda p: criticality[p],
    )
    assert top_by_transitive == "pkg:npm/axios", f"Expected top transitive to be axios; got {top_by_transitive!r}"


# ---------------------------------------------------------------------------
# Test: criticality restricted to the Stellar Repo layer
# ---------------------------------------------------------------------------


async def test_active_criticality_within_stellar(db_session: AsyncSession) -> None:
    """
    Compute A9 transitive criticality restricted to the intra-Stellar Repo layer.

    The active subgraph is first filtered to Repo-only nodes (ExternalRepo nodes
    are excluded).  Criticality is then computed over this purely intra-Stellar
    induced subgraph, which captures the Repo→Repo dependency structure within
    the Stellar ecosystem — i.e., how many Stellar-hosted projects transitively
    depend on each other Stellar-hosted repository.

    Verifies the following properties of the April 2026 dataset:

    1. Every Repo in the active subgraph receives a score (coverage).
    2. At least one Repo has a non-zero criticality (intra-ecosystem dependencies exist).
    3. Every package in _KNOWN_REPO_DIRECT_ACTIVE_DEPENDENTS is present in the Stellar
       criticality scores and has a non-zero score.  The DB direct-Repo count is a
       lower bound on reachability in the full graph, but not necessarily on the
       intra-subgraph transitive count (active Repos that are not reachable from any
       active leaf are excluded from the active subgraph projection).
    4. lightsail-network/js-xdr has the highest intra-Stellar criticality score (score=14 in
       the April 2026 dataset).  Higher direct-Repo count does not guarantee higher
       transitive in-subgraph criticality — js-xdr sits at a deeper shared-dependency
       position within the active Stellar Repo graph.
    """

    G = await build_dependency_graph(db_session)
    G_active = project_active_subgraph(G)

    # Restrict to Repo-only nodes (drop ExternalRepo).
    repo_nodes: set[str] = {n for n, d in G_active.nodes(data=True) if d.get("vertex_type") == "Repo"}
    G_stellar: nx.DiGraph[str] = G_active.subgraph(repo_nodes)

    criticality = compute_criticality(G_stellar)

    # 1. Every Repo node in G_stellar gets a score.
    assert len(criticality) == len(repo_nodes), f"Expected {len(repo_nodes)} scores; got {len(criticality)}"

    # 2. Intra-Stellar dependencies exist — at least one non-zero score.
    assert any(v > 0 for v in criticality.values()), "Expected at least one Repo→Repo dependency in the active subgraph"

    # 3. Known high-dependence packages are present and non-trivially scored.
    for pkg, known_direct in _KNOWN_REPO_DIRECT_ACTIVE_DEPENDENTS.items():
        assert pkg in criticality, (
            f"{pkg!r} has {known_direct} direct Repo dependents but is absent from Stellar criticality scores"
        )
        assert criticality[pkg] > 0, f"{pkg!r}: expected non-zero Stellar criticality; got {criticality[pkg]}"

    # 4. js-xdr tops intra-Stellar criticality (April 2026 snapshot).
    top_stellar = max(criticality, key=criticality.__getitem__)
    assert top_stellar == "pkg:github/lightsail-network/js-xdr", (
        f"Expected js-xdr to have highest Stellar criticality; got {top_stellar!r} (score={criticality[top_stellar]})"
    )
