from pathlib import Path

import pytest
from ruamel.yaml.comments import CommentedMap

from pg_atlas.ingestion.declared_maintainers import Member, Snapshot, project_page_logins, read_snapshot, reconcile
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


def test_incomplete_stroopies_snapshot_fails(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text('{"ledger":123,"first_token_id":1,"next_token_id":2,"members":[]}', encoding="utf-8")
    with pytest.raises(ValueError, match="incomplete"):
        read_snapshot(snapshot)


def test_draft_command_reads_merged_page_at_recorded_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace
    from unittest.mock import Mock

    from scripts import draft_pg_proposal_overrides as command

    docs_repo = Mock()
    sha = "f" * 40
    docs_repo.get_branch.return_value.commit.sha = sha
    page = b"---\ntitle: Example\n---\n## Team & Experience\n[Alice](https://github.com/alice)\n"
    docs_repo.get_contents.side_effect = [
        [SimpleNamespace(path=_PAGE["path"])],
        SimpleNamespace(decoded_content=page),
    ]
    monkeypatch.setattr(command, "get_github_client", lambda: SimpleNamespace(get_repo=lambda _: docs_repo))
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
    monkeypatch.setattr(command, "_MAINTAINER_OUTPUT_PATH", output)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text('{"ledger":123,"first_token_id":1,"next_token_id":1,"members":[]}', encoding="utf-8")
    monkeypatch.setattr(
        "sys.argv", ["draft_pg_proposal_overrides.py", "--source=merged", "--stroopies-snapshot", str(snapshot)]
    )

    assert command.main() == 0
    assert [args.kwargs["ref"] for args in docs_repo.get_contents.call_args_list] == [sha, sha]
    draft = _yaml_load(output.read_text(encoding="utf-8"))
    assert draft[_PROJECT]["metadata"]["maintainers"] == [
        {"github_login": "alice", "evidence": [{**_PAGE, "commit_sha": sha}]},
    ]

    snapshot.write_text('{"ledger":123,"first_token_id":1,"next_token_id":2,"members":[]}', encoding="utf-8")
    valid_output = output.read_bytes()
    assert command.main() == 1
    assert output.read_bytes() == valid_output


def test_rejection_comment_round_trips_and_suppresses_candidate() -> None:
    from pg_atlas.ingestion.declared_maintainers import plan_review
    from scripts.draft_pg_proposal_overrides import _reviewer_rejections

    template = _yaml_load(f"""
$schema: ../schema-override-project.yml
{_PROJECT}:
  metadata:
    # d4-reject: Alice
    maintainers: []
""")
    rejected = _reviewer_rejections(template)
    active = [{"github_login": "alice", "evidence": [_PAGE]}]
    assert plan_review([], active, [], set(), rejected[_PROJECT]) == ([], [], {})
    rendered = _render_sorted_yaml(
        {_PROJECT: {"metadata": {"maintainers": []}}},
        template,
        {},
        {_PROJECT: ["d4-reject: alice"]},
    )
    assert _reviewer_rejections(_yaml_load(rendered)) == {_PROJECT: {"alice"}}


def test_review_only_removes_entries_when_supporting_sources_were_rechecked() -> None:
    from pg_atlas.ingestion.declared_maintainers import plan_review

    old = {"github_login": "alice", "evidence": [_PAGE]}
    assert plan_review([old], [], [], set(), set()) == ([old], [], {})
    entries, _, removals = plan_review([old], [], [], {("project_page", _PAGE["path"])}, set())
    assert entries == []
    assert "no longer declare" in removals["alice"]
    refreshed = {"github_login": "alice", "evidence": [{**_PAGE, "commit_sha": "b" * 40}]}
    assert plan_review([old], [], [refreshed], {("project_page", _PAGE["path"])}, set()) == ([old], [], {})


def test_stroopies_cache_rejects_stale_and_partial_records(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import UTC, datetime, timedelta

    from pg_atlas.ingestion import stroopies

    monkeypatch.setattr(stroopies, "_record", lambda raw: raw)
    settings = {"next_token_id": 1, "ledger": 123}
    cache = {
        "contract_id": stroopies.CONTRACT_ID,
        "read_at": (datetime.now(UTC) - timedelta(days=2)).isoformat(),
        "settings_before": settings,
        "settings_after": settings,
        "members": [],
    }
    with pytest.raises(ValueError, match="stale"):
        stroopies._decode_cache(cache)
    cache["read_at"] = datetime.now(UTC).isoformat()
    with pytest.raises(ValueError, match="incomplete"):
        stroopies._decode_cache(cache)
