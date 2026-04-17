"""
Unit tests for ``pg_atlas.instruments.tee``.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

import logging
import sys
from pathlib import Path

import pytest

from pg_atlas.instruments.tee import run_with_tee


def test_run_with_tee_writes_stdout_and_stderr(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    tee_file = tmp_path / "worker.log"

    def _emit() -> None:
        print("stdout-line")
        print("stderr-line", file=sys.stderr)

    run_with_tee(tee_file, _emit)

    captured = capsys.readouterr()
    log_content = tee_file.read_text(encoding="utf-8")
    assert "stdout-line" in captured.out
    assert "stderr-line" in captured.err
    assert "stdout-line" in log_content
    assert "stderr-line" in log_content


def test_run_with_tee_routes_existing_logging_handlers_to_file(tmp_path: Path) -> None:
    logger = logging.getLogger("test_run_with_tee_routes_existing_logging_handlers_to_file")
    old_propagate = logger.propagate
    logger.propagate = False
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

    tee_file = tmp_path / "worker.log"
    try:

        def _emit() -> None:
            logger.info("logging-line")

        run_with_tee(tee_file, _emit)
    finally:
        logger.propagate = old_propagate
        logger.removeHandler(handler)
        handler.close()

    assert handler.stream is sys.stderr
    assert "logging-line" in tee_file.read_text(encoding="utf-8")
