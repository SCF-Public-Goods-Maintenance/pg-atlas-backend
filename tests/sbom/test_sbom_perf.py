"""
Performance-oriented latency tests for SBOM ingestion admission path.

These tests intentionally run in a dedicated lane (``-m flaky``) and focus on
request-thread latency for ``handle_sbom_submission`` with database disabled
(``session=None``). This isolates the parser/hash hot path from DB and queue
variance so regressions are easier to detect.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from pathlib import Path
from time import perf_counter_ns

import pytest

from pg_atlas.ingestion.persist import SbomAcceptedResponse, handle_sbom_submission

FIXTURES = Path(__file__).parent / "data_fixtures"

pytestmark = [
    pytest.mark.flaky,
]

# Post-optimization benchmark on 2026-04-09:
# baseline_avg_latency_ms ~= 12.445353
# optimized_avg_latency_ms ~= 8.262758 (mean of 5 measured runs)
OPTIMIZED_AVG_LATENCY_MS = 8.262758
MAX_LATENCY_FACTOR = 1.5
WARMUP_RUNS = 3
MEASURED_RUNS = 9


async def _measure_avg_latency_ms(raw_body: bytes) -> float:
    """
    Measure average latency of ``handle_sbom_submission`` in milliseconds.

    The function performs a short warmup to reduce first-run effects, then
    measures a fixed number of iterations and returns the arithmetic mean.
    """

    claims = {
        "repository": "test-org/test-repo",
        "actor": "test-user",
    }

    for _ in range(WARMUP_RUNS):
        result = await handle_sbom_submission(None, raw_body, claims)
        assert isinstance(result, SbomAcceptedResponse)

    timings_ms: list[float] = []
    for _ in range(MEASURED_RUNS):
        started_ns = perf_counter_ns()
        result = await handle_sbom_submission(None, raw_body, claims)
        elapsed_ms = (perf_counter_ns() - started_ns) / 1_000_000
        assert isinstance(result, SbomAcceptedResponse)
        timings_ms.append(elapsed_ms)

    return sum(timings_ms) / len(timings_ms)


async def test_handle_sbom_submission_latency_py_stellar_sdk_baseline() -> None:
    """
    Baseline the request-thread latency for the py-stellar fixture payload.

    This test is informative and validates the benchmark harness itself.
    """

    raw_body = (FIXTURES / "py-stellar-sdk-a9b110.spdx.json").read_bytes()
    avg_latency_ms = await _measure_avg_latency_ms(raw_body)

    assert avg_latency_ms > 0.0


async def test_handle_sbom_submission_latency_regression_guardrail() -> None:
    """
    Ensure average latency stays within 1.2x of optimized baseline latency.
    Pay attention: could you have caused this test to fail? If not: ignore.
    """

    raw_body = (FIXTURES / "py-stellar-sdk-a9b110.spdx.json").read_bytes()
    avg_latency_ms = await _measure_avg_latency_ms(raw_body)

    threshold_ms = OPTIMIZED_AVG_LATENCY_MS * MAX_LATENCY_FACTOR

    assert avg_latency_ms < threshold_ms, (
        f"Average latency {avg_latency_ms:.3f} ms exceeded threshold "
        f"{threshold_ms:.3f} ms ({MAX_LATENCY_FACTOR}x optimized baseline {OPTIMIZED_AVG_LATENCY_MS:.3f} ms)."
    )
