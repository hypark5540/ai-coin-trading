"""Continuity classification shared by the live shadow consumers.

The archive records a long receive interval as an observation-silence marker.
Public Upbit streams are event driven, so that marker alone does not prove that
messages were lost or that the connection changed.  Runtime consumers still
treat explicit connection, protocol, normalization, ordinal, and monotonic
boundaries as confirmed discontinuities.
"""

from __future__ import annotations

from typing import Any


OBSERVATION_SILENCE_GAP_REASONS = frozenset({"receive_interval"})


def is_confirmed_continuity_gap(
    *,
    gap_before: bool,
    gap_reason: Any,
) -> bool:
    """Return whether an archive gap marker proves a continuity boundary."""

    if not gap_before:
        return False
    reason = gap_reason.strip() if isinstance(gap_reason, str) else gap_reason
    return reason not in OBSERVATION_SILENCE_GAP_REASONS
