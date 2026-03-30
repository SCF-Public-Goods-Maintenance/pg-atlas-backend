"""
All tunable metric thresholds. No magic numbers in any other metrics module.

SPDX-FileCopyrightText: 2026 PG Atlas contributors
SPDX-License-Identifier: MPL-2.0
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricsConfig:
    """Immutable configuration for all ecosystem health metrics."""

    pony_factor_threshold: float = 0.50
    """Single-contributor share >= this value flags pony risk."""

    hhi_moderate: int = 1500
    """HHI above this -> moderately concentrated contributor base."""

    hhi_concentrated: int = 2500
    """HHI above this -> highly concentrated contributor base."""

    hhi_critical: int = 5000
    """HHI above this -> critically concentrated (matches standard economic definition)."""

    decay_halflife_days: float = 30.0
    """Half-life in days for temporal decay weighting of contributor activity."""


METRICS_CONFIG = MetricsConfig()
