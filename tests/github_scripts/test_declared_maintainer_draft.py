"""

Exercise the local D4 reviewer workflow with proposal excerpts, simulated
Stroopies batches, and both GitHub collection modes.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from ruamel.yaml.comments import CommentedMap

from pg_atlas.ingestion.declared_maintainers import Member, Snapshot, plan_review, project_page_logins, reconcile
from scripts.draft_pg_proposal_overrides import _render_sorted_yaml, _yaml_load

_PROJECT = "daoip-5:scf:project:example"
_PAGE = {"source": "project_page", "path": "docs/projects/example.md", "commit_sha": "a" * 40}


def test_extracts_profile_links_in_team_tables_and_prose() -> None:
    markdown = """
## Introduction
[Unrelated](https://github.com/unrelated)

## Team & Experience
Our sole maintainer is [Alice](https://github.com/Alice).

| Name | Profile |
| --- | --- |
| Bob | [Bob](https://github.com/Bob) |

See [PR](https://github.com/example/repo/pull/12) and [dependency](https://github.com/example/library).

## Budget
[A sponsor](https://github.com/sponsor)
"""
    assert project_page_logins(markdown) == ["alice", "bob"]


def test_reconciles_evidence_and_renders_uncommentable_candidate() -> None:
    snapshot = Snapshot(
        ledger=123,
        first_token_id=1,
        next_token_id=3,
        members=[
            Member(1, "ALICE", [_PROJECT], False),
            Member(2, "charlie", [], False),
        ],
    )
    active, commented = reconcile(_PROJECT, {name: [_PAGE] for name in ("alice", "bob", "charlie")}, snapshot)
    assert active[0] == {
        "github_login": "alice",
        "evidence": [{"source": "stroopies", "token_id": 1, "ledger": 123}, _PAGE],
    }
    assert active[1]["github_login"] == "charlie"
    assert "token(s) 2" in active[1]["conflict"]
    assert commented == [{"github_login": "bob", "evidence": [_PAGE]}]

    draft = _render_sorted_yaml(
        {_PROJECT: {"metadata": {"maintainers": active}}},
        CommentedMap({"$schema": "./schema-override-project.yml"}),
        {_PROJECT: commented},
    )
    assert _yaml_load(draft)[_PROJECT]["metadata"]["maintainers"] == active
    uncommented = "\n".join(
        line.replace("# ", "", 1) if line.startswith("      # ") and "Confirm these" not in line else line
        for line in draft.splitlines()
    )
    assert _yaml_load(uncommented)[_PROJECT]["metadata"]["maintainers"] == [*commented, *active]


def test_page_candidate_is_active_when_stroopies_has_no_claim() -> None:
    snapshot = Snapshot(ledger=123, first_token_id=1, next_token_id=1, members=[])
    assert reconcile(_PROJECT, {"bob": [_PAGE]}, snapshot) == ([{"github_login": "bob", "evidence": [_PAGE]}], [])


@pytest.mark.parametrize("source_kind", ["merged", "open"])
def test_draft_command_records_pinned_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_kind: str) -> None:
    from scripts import draft_pg_proposal_overrides as command

    docs_repo = Mock()
    sha = "f" * 40
    docs_repo.get_branch.return_value.commit.sha = sha
    page = b"---\ntitle: Example\n---\n## Team & Experience\n[Alice](https://github.com/alice)\n"
    files = [SimpleNamespace(path=_PAGE["path"])]
    content = SimpleNamespace(decoded_content=page)
    docs_repo.get_contents.side_effect = [files, content] if source_kind == "merged" else [content]
    pull = docs_repo.get_pull.return_value
    pull.head.sha = sha
    pull.raw_data = {"head": {"sha": sha}}
    pull.get_files.return_value = [SimpleNamespace(filename=_PAGE["path"])]
    github = Mock()
    github.get_repo.return_value = docs_repo
    github.search_issues.return_value = [SimpleNamespace(number=3)]
    monkeypatch.setattr(command, "get_github_client", lambda: github)
    monkeypatch.setattr(command, "_CACHE_DIR", tmp_path / "github-cache")
    template = CommentedMap({"$schema": "../schema-override-project.yml"})
    overrides = {
        _PROJECT: {
            "display_name": "Example",
            "activity_status": "live",
            "git_owner_url": "https://github.com/example",
            "metadata": {"description": "Example project"},
        }
    }
    monkeypatch.setattr(command, "_load_canonical_overrides", lambda: (template, overrides))
    output = tmp_path / "new.pg-proposal-overrides.yml"
    monkeypatch.setattr(command, "_OUTPUT_PATH", output)
    snapshot = Mock(return_value=Snapshot(123, 0, 0, []))
    monkeypatch.setattr(command, "cached_snapshot", snapshot)
    monkeypatch.setattr("sys.argv", ["draft_pg_proposal_overrides.py", f"--source={source_kind}"])

    assert command.main() == 0
    assert [args.kwargs["ref"] for args in docs_repo.get_contents.call_args_list] == (
        [sha, sha] if source_kind == "merged" else [sha]
    )
    draft = _yaml_load(output.read_text(encoding="utf-8"))
    source = "project_page" if source_kind == "merged" else "proposal"
    assert draft[_PROJECT]["metadata"]["maintainers"] == [
        {"github_login": "alice", "evidence": [{**_PAGE, "source": source, "commit_sha": sha}]},
    ]

    snapshot.side_effect = ValueError("Incomplete batch")
    valid_output = output.read_bytes()
    assert command.main() == 1
    assert output.read_bytes() == valid_output


def test_rejection_comment_round_trips_and_suppresses_candidate() -> None:
    from scripts.draft_pg_proposal_overrides import _reviewer_rejections

    template = _yaml_load(f"""
$schema: ../schema-override-project.yml
{_PROJECT}:
  metadata:
    # reject: "Alice"
    maintainers:
      - github_login: "someone" # reject: Bob
      # reject: 'Charlie'
""")
    rejected = _reviewer_rejections(template)
    assert rejected == {_PROJECT: {"alice", "bob", "charlie"}}
    active = [{"github_login": "alice", "evidence": [_PAGE]}]
    assert plan_review([], active, [], (), rejected[_PROJECT]) == ([], [], {})
    rendered = _render_sorted_yaml(
        {_PROJECT: {"metadata": {"maintainers": []}}},
        template,
        {},
        {_PROJECT: ["reject: alice"]},
    )
    assert _reviewer_rejections(_yaml_load(rendered)) == {_PROJECT: {"alice"}}


def test_review_only_removes_entries_when_supporting_sources_were_rechecked() -> None:

    old = {"github_login": "alice", "evidence": [_PAGE]}
    assert plan_review([old], [], [], (), set()) == ([old], [], {})
    entries, _, removals = plan_review([old], [], [], {("project_page", _PAGE["path"])}, set())
    assert entries == []
    assert "no longer declare" in removals["alice"]
    refreshed = {"github_login": "alice", "evidence": [{**_PAGE, "commit_sha": "b" * 40}]}
    assert plan_review([old], [], [refreshed], {("project_page", _PAGE["path"])}, set()) == ([old], [], {})


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("- GitHub: <https://github.com/rawritude>", ["rawritude"]),
        ("Rashmi – github: https://github.com/Rashmi-278, discord: other", ["rashmi-278"]),
        ("- GitHub: @Utilitycoder", ["utilitycoder"]),
        ("GitHub: https://github.com/rahul-soshte Discord: other", ["rahul-soshte"]),
        (
            "https://github.com/salaheldinsoliman @mohamedbasuony "
            "<https://github.com/abdallah-abdelnaby> [Islam](https://github.com/Islam-Imad)",
            ["abdallah-abdelnaby", "islam-imad", "mohamedbasuony", "salaheldinsoliman"],
        ),
        (
            "- Pam — @pselle • Registry Lead\n- Willem — @willemneal\n- Chad — @chadoh\n- Zach — @zachfedor",
            ["chadoh", "pselle", "willemneal", "zachfedor"],
        ),
    ],
)
def test_detects_reported_team_references(text: str, expected: list[str]) -> None:
    """Recognize the profile forms encountered in the reviewed proposals."""
    markdown = f"## People\n@outside\n## Team and Experience\n{text}\n## Budget\n@also-outside"
    assert project_page_logins(markdown) == expected


def test_manual_maintainer_can_coexist_with_source_backed_entry() -> None:
    """Keep a manually reviewed entry even when its sources are empty."""
    manual = {"github_login": "alice"}
    sourced = {"github_login": "bob", "evidence": [_PAGE]}
    entries, _, removals = plan_review([manual], [sourced], [], {("project_page", _PAGE["path"])}, set())
    assert entries == [manual, sourced]
    assert removals == {}


def test_incremental_read_and_failed_refresh_preserve_valid_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Reuse old tokens, keep incremental batches, and reject incomplete refreshes."""
    from pg_atlas.ingestion import stroopies

    rpc = MagicMock()
    rpc.return_value.__enter__.return_value.get_network.return_value.passphrase = stroopies.Network.PUBLIC_NETWORK_PASSPHRASE
    monkeypatch.setattr(stroopies, "SorobanServer", rpc)

    def decoded_record(raw: dict[str, Any]) -> dict[str, Any]:
        return raw

    def decoded_member(token_id: int, raw: dict[str, Any]) -> Member:
        return Member(token_id, "alice", [], False, raw["ledger"])

    monkeypatch.setattr(stroopies, "_record", decoded_record)
    monkeypatch.setattr(stroopies, "_member", decoded_member)
    full = {
        "contract_id": stroopies.CONTRACT_ID,
        "start_token_id": 0,
        "settings_before": {"next_token_id": 1, "ledger": 100},
        "settings_after": {"next_token_id": 1, "ledger": 101},
        "members": [{"ledger": 100}],
    }
    original = tmp_path / "100.json"
    original.write_text(json.dumps(full), encoding="utf-8")
    read = Mock(side_effect=[{"next_token_id": 2, "ledger": 201}, {"ledger": 201}, {"next_token_id": 3, "ledger": 202}])
    monkeypatch.setattr(stroopies, "_simulate", read)
    snapshot = stroopies.cached_snapshot(tmp_path, "https://rpc.example", refresh=False)
    assert [(member.token_id, member.ledger) for member in snapshot.members] == [(0, 100), (1, 201)]
    assert [call.args[2] for call in read.call_args_list if len(call.args) == 3] == [1]
    assert (tmp_path / "201.json").exists()
    assert "token range changed" in capsys.readouterr().err

    saved = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    read.side_effect = [{"next_token_id": 2, "ledger": 300}, ValueError("unavailable"), {"ledger": 300}]
    with pytest.raises(ValueError, match="unavailable"):
        stroopies.cached_snapshot(tmp_path, "https://rpc.example", refresh=True)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == saved

    read.side_effect = [
        {"next_token_id": 2, "ledger": 301},
        {"ledger": 301},
        {"ledger": 301},
        {"next_token_id": 2, "ledger": 301},
    ]
    refreshed = stroopies.cached_snapshot(tmp_path, "https://rpc.example", refresh=True)
    assert [member.ledger for member in refreshed.members] == [301, 301]
    assert [member.ledger for member in stroopies._load_members(tmp_path)] == [301, 301]
    assert {path.name for path in tmp_path.glob("*.json")} == {"301.json"}


def test_rejection_after_nested_maintainer_evidence() -> None:
    """Recognize a list-level rejection attached to the preceding evidence field."""
    from scripts.draft_pg_proposal_overrides import _reviewer_rejections

    template = _yaml_load(f"""
$schema: ../schema-override-project.yml
{_PROJECT}:
  metadata:
    maintainers:
      - github_login: "alice"
        evidence:
          - source: stroopies
            token_id: 1
            ledger: 200
      # reject: "bob"
""")

    assert _reviewer_rejections(template) == {_PROJECT: {"bob"}}


def test_manual_maintainer_stays_manual_when_a_source_agrees() -> None:
    """Keep the manual declaration through source discovery and withdrawal."""
    manual = {"github_login": "Alice"}
    sourced = {"github_login": "alice", "evidence": [_PAGE]}
    reads = {("project_page", _PAGE["path"])}
    approved, _, _ = plan_review([manual], [sourced], [], reads, set())
    refreshed, _, removals = plan_review(approved, [], [], reads, set())

    assert approved == refreshed == [manual]
    assert removals == {}


def test_older_stroopies_cache_cannot_remove_an_approved_maintainer() -> None:
    """Require a token read at least as recent as the prior approval."""
    approved = {"github_login": "alice", "evidence": [{"source": "stroopies", "token_id": 1, "ledger": 200}]}
    retained, _, removals = plan_review([approved], [], [], (), set(), token_ledgers={1: 100})
    removed, _, confirmed_removals = plan_review([approved], [], [], (), set(), token_ledgers={1: 201})

    assert retained == [approved]
    assert removals == {}
    assert removed == []
    assert "alice" in confirmed_removals


def test_older_stroopies_cache_cannot_conflict_with_approved_evidence() -> None:
    """Ignore older membership claims while still surfacing a newer contradiction."""
    approved = {"github_login": "alice", "evidence": [{"source": "stroopies", "token_id": 0, "ledger": 200}]}
    page_evidence = {"alice": [_PAGE], "bob": [_PAGE]}
    stale = Snapshot(100, 0, 1, [Member(0, "alice", [], False, 100)])
    fresh = Snapshot(201, 0, 1, [Member(0, "alice", [], False, 201)])
    active, commented = reconcile(_PROJECT, page_evidence, stale, previous_items=[approved])
    reviewed, candidates, removals = plan_review([approved], active, commented, (), set(), token_ledgers={0: 100})
    conflicts, _ = reconcile(_PROJECT, page_evidence, fresh, previous_items=[approved])

    assert reviewed == [{"github_login": "alice", "evidence": [*approved["evidence"], _PAGE]}]
    assert removals == {}
    assert candidates == [{"github_login": "bob", "evidence": [_PAGE]}]
    assert "conflict" in conflicts[0]
