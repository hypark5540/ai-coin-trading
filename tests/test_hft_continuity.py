from __future__ import annotations

import pytest

from coinpilot.hft_continuity import is_confirmed_continuity_gap


def test_receive_interval_with_gap_marker_is_observation_silence() -> None:
    assert not is_confirmed_continuity_gap(
        gap_before=True,
        gap_reason="receive_interval",
    )


@pytest.mark.parametrize(
    "gap_reason",
    [None, "receive_interval", "websocket_reconnect", "unknown_reason"],
)
def test_gap_before_false_is_never_a_confirmed_gap(gap_reason: object) -> None:
    assert not is_confirmed_continuity_gap(
        gap_before=False,
        gap_reason=gap_reason,
    )


@pytest.mark.parametrize("gap_reason", [None, "", "unknown_reason"])
def test_missing_or_unknown_reason_fails_closed(gap_reason: object) -> None:
    assert is_confirmed_continuity_gap(
        gap_before=True,
        gap_reason=gap_reason,
    )
