"""
Tests for Procrastinate log parsers.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

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

    result = subprocess.run(
        [sys.executable, str(script_path), str(log_file)],
        capture_output=True,
        text=True,
        check=True,
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

    result = subprocess.run(
        [sys.executable, str(script_path), str(log_file)],
        capture_output=True,
        text=True,
        check=True,
    )

    stdout = result.stdout
    assert "sbom_succeeded=1" in stdout
    assert "error_count=0" in stdout
    assert "warnings=" not in stdout
    assert "spdx_details=- name='foo' packages=43" in stdout
