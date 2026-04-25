"""
Python-level stdout/stderr tee for GitHub Actions log capture.

Use ``run_with_tee`` instead of the shell ``tee`` command.  Shell ``tee``
masks the exit code of the command to its left (``cmd | tee file`` always
exits 0); Python-level tee keeps the original exit code because the
exception propagates normally.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

import io
import logging
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Protocol, TextIO, TypeAlias, TypeGuard


class _StreamTee(io.TextIOBase):
    """
    Mirror writes to an original stream and a tee file stream.
    """

    def __init__(self, primary: TextIO | io.TextIOBase, tee_file: TextIO | io.TextIOBase) -> None:
        self._primary = primary
        self._tee_file = tee_file

    def writable(self) -> bool:
        return self._primary.writable()

    def write(self, data: str) -> int:
        written = self._primary.write(data)
        self._tee_file.write(data)

        return written

    def flush(self) -> None:
        self._primary.flush()

        try:
            self._tee_file.flush()
        except ValueError:
            pass

    def isatty(self) -> bool:
        return self._primary.isatty()

    def fileno(self) -> int:
        return self._primary.fileno()

    def __getattr__(self, name: str) -> object:
        return getattr(self._primary, name)


class _HasTextStream(Protocol):
    stream: io.TextIOBase


_StreamHandlerPatch: TypeAlias = tuple[_HasTextStream, io.TextIOBase]


def _iter_loggers() -> Iterator[logging.Logger]:
    yield logging.root

    for logger in logging.root.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            yield logger


def _is_text_stream_handler(handler: object) -> TypeGuard[_HasTextStream]:
    if not isinstance(handler, logging.StreamHandler):
        return False

    stream = getattr(handler, "stream", None)  # pyright: ignore[reportUnknownArgumentType]
    return isinstance(stream, io.TextIOBase)


def _patch_stream_handlers(original_stream: TextIO | io.TextIOBase, replacement: io.TextIOBase) -> list[_StreamHandlerPatch]:
    patched: list[_StreamHandlerPatch] = []

    for logger in _iter_loggers():
        for handler in logger.handlers:
            if not _is_text_stream_handler(handler):
                continue

            if handler.stream is original_stream:
                patched.append((handler, handler.stream))
                handler.stream = replacement

    return patched


def _restore_stream_handlers(patched_handlers: list[_StreamHandlerPatch]) -> None:
    for handler, original_stream in patched_handlers:
        handler.stream = original_stream


def run_with_tee(tee_path: Path | None, run: Callable[[], None]) -> None:
    """
    Run a callback while optionally mirroring stdout/stderr to a file.

    The original streams remain active so output still appears in GitHub
    Actions logs while also being persisted to ``tee_path``.

    Unlike shell ``tee``, exceptions raised inside ``run`` propagate
    normally so the process exits with a non-zero code on failure.
    """

    if tee_path is None:
        run()

        return

    tee_path.parent.mkdir(parents=True, exist_ok=True)
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    with tee_path.open("w", encoding="utf-8") as tee_file:
        stdout_wrapper = _StreamTee(original_stdout, tee_file)
        stderr_wrapper = _StreamTee(original_stderr, tee_file)

        sys.stdout = stdout_wrapper
        sys.stderr = stderr_wrapper

        patched_handlers: list[_StreamHandlerPatch] = []
        patched_handlers.extend(_patch_stream_handlers(original_stdout, stdout_wrapper))
        patched_handlers.extend(_patch_stream_handlers(original_stderr, stderr_wrapper))

        try:
            run()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            _restore_stream_handlers(patched_handlers)
            tee_file.flush()
