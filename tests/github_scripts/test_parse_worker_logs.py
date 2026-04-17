"""
Tests for Procrastinate log parsers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import os
import subprocess
import sys
from pathlib import Path


def test_parse_bootstrap_log(tmp_path: Path) -> None:
    log_content = """
2026-07-17 10:05:00,123 INFO     worker: Starting
Queue opengrants final status counts: todo=0 doing=0 succeeded=5 failed=0 cancelled=0 aborted=0
2026-07-17 10:06:00,456 WARNING  task: Something odd happened
Queue package-deps final status counts: todo=1 doing=0 succeeded=3 failed=1 cancelled=0 aborted=0
2026-07-17 10:07:00,789 ERROR    task: Critical failure
"""
    log_file = tmp_path / "bootstrap.log"
    log_file.write_text(log_content)

    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "parse-bootstrap-log.py"

    # emulate non-GitHub Actions environment
    env = os.environ.copy()
    env.pop("GITHUB_OUTPUT", None)

    result = subprocess.run(
        [sys.executable, str(script_path), str(log_file)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )

    stdout = result.stdout
    assert "opengrants_succeeded=5" in stdout
    assert "package_deps_failed=1" in stdout
    assert "warning_count=1" in stdout
    assert "error_count=1" in stdout
    assert "warnings=Something odd happened" in stdout
    assert "errors=Critical failure" in stdout


def test_parse_sbom_log(tmp_path: Path) -> None:
    log_content = """
2026-07-17 10:05:00,123 INFO     worker: Starting
2026-07-17 10:05:00,456 INFO  pg_atlas.ingestion.spdx: SPDX document parsed OK: name='foo' packages=43
Queue sbom final status counts: todo=1 doing=0 succeeded=1 failed=0 cancelled=0 aborted=0
"""
    log_file = tmp_path / "sbom.log"
    log_file.write_text(log_content)

    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "parse-sbom-log.py"

    # emulate non-GitHub Actions environment
    env = os.environ.copy()
    env.pop("GITHUB_OUTPUT", None)

    result = subprocess.run(
        [sys.executable, str(script_path), str(log_file)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )

    stdout = result.stdout
    assert "sbom_succeeded=1" in stdout
    assert "error_count=0" in stdout
    assert "warnings=" not in stdout
    assert "spdx_details=- name='foo' packages=43" in stdout


def test_parse_gitlog_log(tmp_path: Path) -> None:
    worker_log = """
2026-07-17 10:05:00,123 INFO     worker: Starting
Queue gitlog final status counts: todo=0 doing=0 succeeded=4 failed=1 cancelled=0 aborted=0
2026-07-17 10:06:00,456 WARNING  task: Clone warning
Gitlog rate-limit stats: first_rate_limit_hit_after_n_repos=2 total_rate_limit_hits=5
Gitlog terminal failures marked private: https://github.com/org/private-repo
"""
    auth_log = "gh auth status: logged in"
    worker_file = tmp_path / "gitlog.log"
    auth_file = tmp_path / "gh-auth.log"
    worker_file.write_text(worker_log)
    auth_file.write_text(auth_log)

    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "parse-gitlog-log.py"

    env = os.environ.copy()
    env.pop("GITHUB_OUTPUT", None)

    result = subprocess.run(
        [sys.executable, str(script_path), str(worker_file), str(auth_file)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )

    stdout = result.stdout
    assert "gitlog_succeeded=4" in stdout
    assert "gitlog_failed=1" in stdout
    assert "first_rate_limit_hit_after_n_repos=2" in stdout
    assert "total_rate_limit_hits=5" in stdout
    assert "terminal_failures_marked_private=https://github.com/org/private-repo" in stdout
    assert "gh_auth_status=gh auth status: logged in" in stdout


def test_parse_adoption_log(tmp_path: Path) -> None:
    log_content = (
        "2026-07-17 10:05:00,123 INFO     __main__: materialize_adoption_scores: "
        "repos_seen=144 repo_composites_computed=144 projects_seen=611 "
        "projects_scored=26 duration_seconds=0.158\n"
        "2026-07-17 10:05:00,456 INFO     __main__: project adoption materialization finished: "
        "repos_seen=144 repo_composites_computed=144 projects_seen=611 "
        "projects_scored=26 duration_seconds=0.158\n"
    )
    log_file = tmp_path / "adoption.log"
    log_file.write_text(log_content)

    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "parse-materialize-adoption-log.py"

    env = os.environ.copy()
    env.pop("GITHUB_OUTPUT", None)

    result = subprocess.run(
        [sys.executable, str(script_path), str(log_file)],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )

    stdout = result.stdout
    assert "adoption_repos_seen=144" in stdout
    assert "adoption_repo_composites_computed=144" in stdout
    assert "adoption_projects_seen=611" in stdout
    assert "adoption_projects_scored=26" in stdout
    assert "adoption_duration_seconds=0.158" in stdout
