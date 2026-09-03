"""Tests for custom_components.agentic_home.heartbeat module."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from custom_components.agentic_home.const import HEARTBEAT_INTERVAL_SECONDS
from custom_components.agentic_home.frame import SequenceCounter


# ---------------------------------------------------------------------------
# Test HeartbeatGenerator class
# ---------------------------------------------------------------------------

class TestHeartbeatGenerator:
    """Timer logic, auth failure, and lifecycle tests for HeartbeatGenerator."""

    @pytest.fixture
    def mock_hass(self) -> MagicMock:
        """Mock Home Assistant core with a mock event loop."""
        hass = MagicMock()
        hass.loop = MagicMock()
        return hass

    @pytest.fixture
    def mock_pusher(self) -> MagicMock:
        """Mock IngressHTTPPusher."""
        pusher = MagicMock()
        pusher._auth_failed = False
        return pusher

    @pytest.fixture
    def mock_metrics(self) -> MagicMock:
        """Mock RuntimeMetrics."""
        metrics = MagicMock()
        metrics.last_push_time = 0.0
        return metrics

    @pytest.fixture
    def seq_counter(self) -> SequenceCounter:
        """Fresh sequence counter for each test."""
        return SequenceCounter()

    @pytest.fixture
    def heartbeat_generator(self, mock_hass, mock_pusher, mock_metrics, seq_counter):
        """Instantiate HeartbeatGenerator with mocks."""
        from custom_components.agentic_home.heartbeat import HeartbeatGenerator

        return HeartbeatGenerator(
            hass=mock_hass,
            pusher=mock_pusher,
            metrics=mock_metrics,
            seq_counter=seq_counter,
        )

    # -------------------------------------------------------------------------
    # Timer logic tests
    # -------------------------------------------------------------------------

    def test_start_schedules_timer(self, mock_hass, heartbeat_generator):
        """start() calls call_later with HEARTBEAT_INTERVAL_SECONDS."""
        heartbeat_generator.start()
        mock_hass.loop.call_later.assert_called_once_with(
            HEARTBEAT_INTERVAL_SECONDS,
            heartbeat_generator._on_heartbeat_timer,
        )

    def test_timer_sends_heartbeat_when_idle(
        self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator
    ):
        """When last_push_time is 0, timer fires and sends a heartbeat."""
        mock_metrics.last_push_time = 0.0

        heartbeat_generator.start()
        # Grab the scheduled callback and invoke it.
        call_later_calls = mock_hass.loop.call_later.call_args_list
        assert len(call_later_calls) == 1
        _, callback = call_later_calls[0].args
        callback()

        mock_pusher.add_frame.assert_called_once()
        frame = mock_pusher.add_frame.call_args[0][0]
        assert frame["delivery_mode"] == "heartbeat"
        assert frame["event_type"] == "heartbeat"
        assert frame["payload"] == {}

    def test_timer_skips_heartbeat_when_stream_active(
        self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator
    ):
        """When last_push_time is recent, timer skips sending and reschedules."""
        import time

        mock_metrics.last_push_time = time.time()

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        assert len(call_later_calls) == 1
        _, callback = call_later_calls[0].args
        callback()

        # No frame added.
        mock_pusher.add_frame.assert_not_called()

        # Still rescheduled (active stream check doesn't stop timer permanently).
        assert mock_hass.loop.call_later.call_count == 2

    def test_timer_reschedules_after_idle_check(
        self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator
    ):
        """After sending a heartbeat, timer reschedules itself."""
        mock_metrics.last_push_time = 0.0

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        # Two calls: first from start(), second from reschedule in _on_heartbeat_timer.
        assert mock_hass.loop.call_later.call_count == 2
        assert mock_pusher.add_frame.call_count == 1

    def test_timer_reschedules_after_active_skip(
        self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator
    ):
        """When heartbeat is skipped, timer still reschedules."""
        import time

        mock_metrics.last_push_time = time.time()

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        # Two calls: one from start(), one from skip-reschedule.
        assert mock_hass.loop.call_later.call_count == 2
        mock_pusher.add_frame.assert_not_called()

    # -------------------------------------------------------------------------
    # Auth failure tests
    # -------------------------------------------------------------------------

    def test_auth_failure_stops_timer(
        self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator
    ):
        """When pusher._auth_failed is True, no frame is added and timer stops."""
        mock_pusher._auth_failed = True
        mock_metrics.last_push_time = 0.0

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        mock_pusher.add_frame.assert_not_called()

    def test_auth_failure_no_reschedule(self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator):
        """After auth failure, no further call_later calls are made."""
        mock_pusher._auth_failed = True
        mock_metrics.last_push_time = 0.0

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        # Only the initial call; no reschedule after auth failure.
        assert mock_hass.loop.call_later.call_count == 1

    # -------------------------------------------------------------------------
    # Lifecycle tests
    # -------------------------------------------------------------------------

    def test_start_idempotent(self, mock_hass, heartbeat_generator):
        """Calling start() twice schedules only one timer."""
        heartbeat_generator.start()
        first_call_count = mock_hass.loop.call_later.call_count
        heartbeat_generator.start()
        assert mock_hass.loop.call_later.call_count == first_call_count

    def test_stop_cancels_timer(self, mock_hass, heartbeat_generator):
        """stop() cancels the pending timer handle."""
        heartbeat_generator.start()
        timer_handle = mock_hass.loop.call_later.return_value
        heartbeat_generator.stop()

        timer_handle.cancel.assert_called_once()
        assert heartbeat_generator._timer is None

    def test_stop_prevents_heartbeat_after_stop(self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator):
        """After stop(), timer callback is no-op (no frame added)."""
        import time

        mock_metrics.last_push_time = 0.0
        heartbeat_generator.start()
        heartbeat_generator.stop()

        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        mock_pusher.add_frame.assert_not_called()

    def test_stop_sets_stopped_flag(self, heartbeat_generator):
        """_stopped is True after stop() is called."""
        heartbeat_generator.start()
        assert heartbeat_generator._stopped is False
        heartbeat_generator.stop()
        assert heartbeat_generator._stopped is True

    # -------------------------------------------------------------------------
    # Edge case tests
    # -------------------------------------------------------------------------

    def test_first_heartbeat_before_any_push(self, mock_hass, mock_pusher, mock_metrics, heartbeat_generator):
        """last_push_time=0.0 causes a heartbeat to be sent immediately."""
        mock_metrics.last_push_time = 0.0

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        assert mock_pusher.add_frame.call_count == 1

    def test_heartbeat_frame_format(self, mock_hass, mock_pusher, heartbeat_generator):
        """Heartbeat frame dict matches build_heartbeat output in structure and field types."""
        from custom_components.agentic_home.frame import build_heartbeat

        heartbeat_generator.start()
        call_later_calls = mock_hass.loop.call_later.call_args_list
        _, callback = call_later_calls[0].args
        callback()

        frame = mock_pusher.add_frame.call_args[0][0]
        expected = build_heartbeat(heartbeat_generator._seq_counter)

        # Compare all fields except source_sequence (which increments with each call).
        assert frame["delivery_mode"] == expected["delivery_mode"]
        assert frame["source_sequence"] >= expected["source_sequence"] - 1
        assert frame["source_sequence"] <= expected["source_sequence"]
        assert frame["event_type"] == expected["event_type"]
        assert frame["payload"] == expected["payload"]
        assert frame["source_event_id"] == ""

    def test_stop_is_idempotent(self, mock_hass, heartbeat_generator):
        """Calling stop() twice does not double-cancel the timer."""
        heartbeat_generator.start()
        timer_handle = mock_hass.loop.call_later.return_value
        heartbeat_generator.stop()
        heartbeat_generator.stop()
        timer_handle.cancel.assert_called_once()

    def test_start_after_stop_does_nothing(self, mock_hass, heartbeat_generator):
        """start() after stop() does not schedule a new timer."""
        heartbeat_generator.start()
        heartbeat_generator.stop()
        mock_hass.loop.call_later.reset_mock()
        heartbeat_generator.start()
        mock_hass.loop.call_later.assert_not_called()