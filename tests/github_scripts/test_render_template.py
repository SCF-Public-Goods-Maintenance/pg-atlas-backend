"""
Tests for the template renderer script.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import subprocess
import sys
from pathlib import Path


def test_render_template(tmp_path: Path) -> None:
    template_path = tmp_path / "test-template.md"
    template_path.write_text("Header\nName: {name}\nMissing: {missing}\nMultiline: {multiline}\n")

    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "render-template.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            str(template_path),
            "name=PG Atlas",
            "multiline=Line 1\nLine 2",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert "Name: PG Atlas" in result.stdout
    assert "Missing: {missing}" in result.stdout
    assert "Multiline: Line 1\nLine 2" in result.stdout


def test_render_template_missing_args() -> None:
    script_path = Path(__file__).parent.parent.parent / ".github" / "scripts" / "render-template.py"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert "Usage:" in result.stderr
