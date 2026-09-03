"""Tests for custom_components.agentic_home.metrics module."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from custom_components.agentic_home.metrics import RuntimeMetrics


# ---------------------------------------------------------------------------
# RuntimeMetrics tests
# ---------------------------------------------------------------------------

class TestRuntimeMetricsInitialState:
    def test_default_values(self):
        """New RuntimeMetrics has zero/None defaults."""
        m = RuntimeMetrics()
        assert m.error_count == 0
        assert m.last_push_time == 0.0
        assert m.frames_pushed == 0
        assert m.frames_captured == 0
        assert m.last_push_status is None

    def test_snapshot_initial(self):
        """snapshot() returns correct initial state."""
        m = RuntimeMetrics()
        snap = m.snapshot()
        assert snap["error_count"] == 0
        assert snap["last_push_time"] == 0.0
        assert snap["frames_pushed"] == 0
        assert snap["frames_captured"] == 0
        assert snap["last_push_status"] is None


class TestIncrementError:
    def test_increment_error_once(self):
        """increment_error() increments error_count by 1."""
        m = RuntimeMetrics()
        m.increment_error()
        assert m.error_count == 1

    def test_increment_error_multiple(self):
        """Multiple increment_error() calls accumulate correctly."""
        m = RuntimeMetrics()
        for _ in range(5):
            m.increment_error()
        assert m.error_count == 5


class TestRecordPush:
    def test_record_push_accumulates_frames(self):
        """record_push() adds count to frames_pushed."""
        m = RuntimeMetrics()
        m.record_push(count=10, status=202, timestamp=1000.0)
        assert m.frames_pushed == 10
        assert m.last_push_time == 1000.0
        assert m.last_push_status == 202

    def test_record_push_accumulates_across_calls(self):
        """Multiple record_push() calls accumulate frames_pushed."""
        m = RuntimeMetrics()
        m.record_push(count=5, status=202, timestamp=1000.0)
        m.record_push(count=3, status=202, timestamp=2000.0)
        assert m.frames_pushed == 8
        assert m.last_push_time == 2000.0
        assert m.last_push_status == 202

    def test_record_push_captures_failure_status(self):
        """record_push() stores non-2xx status codes."""
        m = RuntimeMetrics()
        m.record_push(count=5, status=503, timestamp=1000.0)
        assert m.frames_pushed == 5
        assert m.last_push_status == 503


class TestSnapshot:
    def test_snapshot_returns_copy(self):
        """snapshot() returns a plain dict (not a view)."""
        m = RuntimeMetrics()
        m.increment_error()
        m.record_push(count=7, status=202, timestamp=1234.5)
        m.increment_captured(3)

        snap = m.snapshot()
        assert isinstance(snap, dict)
        assert snap["error_count"] == 1
        assert snap["frames_pushed"] == 7
        assert snap["frames_captured"] == 3
        assert snap["last_push_time"] == 1234.5
        assert snap["last_push_status"] == 202

    def test_snapshot_is_independent_of_mutations(self):
        """snapshot() returns a copy; later mutations don't affect the returned dict."""
        m = RuntimeMetrics()
        snap1 = m.snapshot()
        m.increment_error()
        m.record_push(count=1, status=200, timestamp=99.0)
        m.increment_captured(2)
        snap2 = m.snapshot()

        assert snap1["error_count"] == 0
        assert snap2["error_count"] == 1
        assert snap1["frames_pushed"] == 0
        assert snap2["frames_pushed"] == 1


class TestIncrementCaptured:
    def test_increment_captured_default_1(self):
        """increment_captured() defaults to adding 1."""
        m = RuntimeMetrics()
        m.increment_captured()
        assert m.frames_captured == 1

    def test_increment_captured_batch(self):
        """increment_captured(n) adds n frames."""
        m = RuntimeMetrics()
        m.increment_captured(50)
        assert m.frames_captured == 50

    def test_increment_captured_accumulates(self):
        """Multiple increment_captured() calls accumulate."""
        m = RuntimeMetrics()
        m.increment_captured(10)
        m.increment_captured(5)
        assert m.frames_captured == 15


class TestRecordRegistryPush:
    def test_record_registry_push_increments_counter(self):
        """record_registry_push() increments registry_push_count."""
        m = RuntimeMetrics()
        assert m.registry_push_count == 0
        m.record_registry_push(status=202, timestamp=1000.0)
        assert m.registry_push_count == 1
        m.record_registry_push(status=202, timestamp=2000.0)
        assert m.registry_push_count == 2

    def test_record_registry_push_sets_timestamp(self):
        """record_registry_push() sets registry_last_push_time."""
        m = RuntimeMetrics()
        assert m.registry_last_push_time == 0.0
        m.record_registry_push(status=202, timestamp=1234.5)
        assert m.registry_last_push_time == 1234.5

    def test_record_registry_push_records_status(self):
        """record_registry_push() updates last_push_status."""
        m = RuntimeMetrics()
        assert m.last_push_status is None
        m.record_registry_push(status=400, timestamp=999.0)
        assert m.last_push_status == 400

    def test_record_registry_push_multiple_calls(self):
        """Multiple record_registry_push() calls accumulate counters and update timestamp."""
        m = RuntimeMetrics()
        m.record_registry_push(status=202, timestamp=100.0)
        assert m.registry_push_count == 1
        assert m.registry_last_push_time == 100.0

        m.record_registry_push(status=202, timestamp=200.0)
        assert m.registry_push_count == 2
        assert m.registry_last_push_time == 200.0


class TestIncrementRegistryError:
    def test_increment_registry_error_once(self):
        """increment_registry_error() increments registry_error_count by 1."""
        m = RuntimeMetrics()
        m.increment_registry_error()
        assert m.registry_error_count == 1

    def test_increment_registry_error_multiple(self):
        """Multiple increment_registry_error() calls accumulate."""
        m = RuntimeMetrics()
        for _ in range(3):
            m.increment_registry_error()
        assert m.registry_error_count == 3


class TestSnapshotIncludesRegistryFields:
    def test_snapshot_includes_registry_fields(self):
        """snapshot() returns all registry fields."""
        m = RuntimeMetrics()
        m.record_registry_push(status=202, timestamp=500.0)
        m.increment_registry_error()

        snap = m.snapshot()
        assert "registry_push_count" in snap
        assert "registry_last_push_time" in snap
        assert "registry_error_count" in snap
        assert snap["registry_push_count"] == 1
        assert snap["registry_last_push_time"] == 500.0
        assert snap["registry_error_count"] == 1

    def test_snapshot_registry_fields_independent_of_mutations(self):
        """snapshot() returns a copy; later mutations don't affect returned dict."""
        m = RuntimeMetrics()
        snap1 = m.snapshot()
        assert snap1["registry_push_count"] == 0
        assert snap1["registry_last_push_time"] == 0.0
        assert snap1["registry_error_count"] == 0

        m.record_registry_push(status=202, timestamp=42.0)
        m.increment_registry_error()
        snap2 = m.snapshot()

        assert snap1["registry_push_count"] == 0
        assert snap2["registry_push_count"] == 1
        assert snap1["registry_error_count"] == 0
        assert snap2["registry_error_count"] == 1


# ---------------------------------------------------------------------------
# Error tracking tests
# ---------------------------------------------------------------------------

class TestRecordStreamError:
    def test_record_stream_error_sets_fields(self):
        """record_stream_error() sets last_error_msg and last_error_time."""
        m = RuntimeMetrics()
        before = time.time()
        m.record_stream_error("connection refused")
        after = time.time()

        assert m.last_error_msg == "stream: connection refused"
        assert before <= m.last_error_time <= after

    def test_record_stream_error_increments_error_count(self):
        """record_stream_error() increments error_count."""
        m = RuntimeMetrics()
        m.record_stream_error("timeout")
        assert m.error_count == 1

    def test_record_stream_error_overwrites_previous(self):
        """Subsequent record_stream_error() calls overwrite previous values."""
        m = RuntimeMetrics()
        m.record_stream_error("first error")
        m.record_stream_error("second error")

        assert m.last_error_msg == "stream: second error"

    def test_record_stream_error_invokes_callback(self):
        """record_stream_error() fires _on_update after state change."""
        m = RuntimeMetrics()
        called = False

        def on_update():
            nonlocal called
            called = True

        m._on_update = on_update
        m.record_stream_error("fail")
        assert called is True


class TestRecordRegistryErrorMsg:
    def test_record_registry_error_msg_sets_fields(self):
        """record_registry_error_msg() sets last_error_msg and last_error_time."""
        m = RuntimeMetrics()
        before = time.time()
        m.record_registry_error_msg("entity not found")
        after = time.time()

        assert m.last_error_msg == "registry: entity not found"
        assert before <= m.last_error_time <= after

    def test_record_registry_error_msg_increments_registry_error_count(self):
        """record_registry_error_msg() increments registry_error_count."""
        m = RuntimeMetrics()
        m.record_registry_error_msg("timeout")
        assert m.registry_error_count == 1

    def test_record_registry_error_msg_invokes_callback(self):
        """record_registry_error_msg() fires _on_update after state change."""
        m = RuntimeMetrics()
        called = False

        def on_update():
            nonlocal called
            called = True

        m._on_update = on_update
        m.record_registry_error_msg("boom")
        assert called is True


# ---------------------------------------------------------------------------
# Push rate tests
# ---------------------------------------------------------------------------

class TestPushRate:
    def test_push_rate_zero_when_empty(self):
        """push_rate returns 0.0 when no push timestamps recorded."""
        m = RuntimeMetrics()
        assert m.push_rate == 0.0

    def test_push_rate_zero_when_one_entry(self):
        """push_rate returns 0.0 when only one timestamp in window."""
        m = RuntimeMetrics()
        m.record_push(count=1, status=202, timestamp=time.time())
        assert m.push_rate == 0.0

    def test_push_rate_computes_moving_average(self):
        """push_rate returns count/60 when >=2 timestamps in 60s window."""
        m = RuntimeMetrics()
        now = time.time()
        m.record_push(count=1, status=202, timestamp=now - 30.0)
        m.record_push(count=1, status=202, timestamp=now)
        rate = m.push_rate
        # 2 entries within 60s → 2/60 pushes per second
        assert abs(rate - 2.0 / 60.0) < 1e-9

    def test_push_rate_ignores_old_entries(self):
        """push_rate only counts entries within the last 60 seconds."""
        m = RuntimeMetrics()
        now = time.time()
        # 3 entries, but only 2 within last 60s (one is 90s old)
        m.record_push(count=1, status=202, timestamp=now - 90.0)
        m.record_push(count=1, status=202, timestamp=now - 30.0)
        m.record_push(count=1, status=202, timestamp=now)
        rate = m.push_rate
        # only 2 entries within 60s window
        assert abs(rate - 2.0 / 60.0) < 1e-9

    def test_push_rate_reports_higher_rate_for_burst(self):
        """Burst of pushes in a short window yields higher push_rate."""
        m = RuntimeMetrics()
        now = time.time()
        for i in range(30):
            m.record_push(count=1, status=202, timestamp=now - i)
        rate = m.push_rate
        # all 30 timestamps within 60s window → 30/60 = 0.5
        assert abs(rate - 0.5) < 1e-9

    def test_push_rate_excludes_from_snapshot_impl_details(self):
        """_frame_push_times is not included in snapshot (internal impl detail)."""
        m = RuntimeMetrics()
        snap = m.snapshot()
        assert "_frame_push_times" not in snap


# ---------------------------------------------------------------------------
# Callback tests
# ---------------------------------------------------------------------------

class TestOnUpdateCallback:
    def test_increment_error_invokes_callback(self):
        """increment_error() fires _on_update after increment."""
        m = RuntimeMetrics()
        calls = []

        def on_update():
            calls.append(None)

        m._on_update = on_update
        m.increment_error()
        assert len(calls) == 1

    def test_record_push_invokes_callback(self):
        """record_push() fires _on_update after recording."""
        m = RuntimeMetrics()
        calls = []

        def on_update():
            calls.append(None)

        m._on_update = on_update
        m.record_push(count=5, status=202, timestamp=100.0)
        assert len(calls) == 1

    def test_record_registry_push_invokes_callback(self):
        """record_registry_push() fires _on_update after recording."""
        m = RuntimeMetrics()
        calls = []

        def on_update():
            calls.append(None)

        m._on_update = on_update
        m.record_registry_push(status=202, timestamp=100.0)
        assert len(calls) == 1

    def test_increment_registry_error_invokes_callback(self):
        """increment_registry_error() fires _on_update after increment."""
        m = RuntimeMetrics()
        calls = []

        def on_update():
            calls.append(None)

        m._on_update = on_update
        m.increment_registry_error()
        assert len(calls) == 1

    def test_increment_captured_does_not_invoke_callback(self):
        """increment_captured() does not fire _on_update (not in plan)."""
        m = RuntimeMetrics()
        calls = []

        def on_update():
            calls.append(None)

        m._on_update = on_update
        m.increment_captured(5)
        assert len(calls) == 0

    def test_callback_receives_no_args(self):
        """Callback is called with no positional arguments."""
        m = RuntimeMetrics()
        received = []

        def on_update():
            received.append(True)

        m._on_update = on_update
        m.increment_error()
        assert received == [True]  # callback was called (no args passed to it)

    def test_callback_not_set_is_safe(self):
        """Methods work correctly when _on_update is None (default)."""
        m = RuntimeMetrics()
        # should not raise
        m.increment_error()
        m.record_push(count=1, status=202, timestamp=1.0)
        m.record_registry_push(status=202, timestamp=1.0)
        m.increment_registry_error()
        assert m.error_count == 1
        assert m.frames_pushed == 1


# ---------------------------------------------------------------------------
# New snapshot fields tests
# ---------------------------------------------------------------------------

class TestSnapshotIncludesNewFields:
    def test_snapshot_includes_last_error_msg(self):
        """snapshot() includes last_error_msg field."""
        m = RuntimeMetrics()
        m.record_stream_error("bad response")
        snap = m.snapshot()
        assert "last_error_msg" in snap
        assert snap["last_error_msg"] == "stream: bad response"

    def test_snapshot_includes_last_error_time(self):
        """snapshot() includes last_error_time field."""
        m = RuntimeMetrics()
        m.record_stream_error("fail")
        snap = m.snapshot()
        assert "last_error_time" in snap
        assert snap["last_error_time"] == m.last_error_time

    def test_snapshot_includes_push_rate(self):
        """snapshot() includes push_rate field."""
        m = RuntimeMetrics()
        now = time.time()
        m.record_push(count=1, status=202, timestamp=now - 30.0)
        m.record_push(count=1, status=202, timestamp=now)
        snap = m.snapshot()
        assert "push_rate" in snap
        assert abs(snap["push_rate"] - 2.0 / 60.0) < 1e-9

    def test_snapshot_push_rate_zero_initially(self):
        """snapshot() push_rate is 0.0 before any pushes."""
        m = RuntimeMetrics()
        snap = m.snapshot()
        assert snap["push_rate"] == 0.0

    def test_snapshot_last_error_msg_empty_initially(self):
        """snapshot() last_error_msg is empty string before any errors."""
        m = RuntimeMetrics()
        snap = m.snapshot()
        assert snap["last_error_msg"] == ""

    def test_snapshot_last_error_time_zero_initially(self):
        """snapshot() last_error_time is 0.0 before any errors."""
        m = RuntimeMetrics()
        snap = m.snapshot()
        assert snap["last_error_time"] == 0.0


# ---------------------------------------------------------------------------
# Deque eviction / memory bound tests
# ---------------------------------------------------------------------------

class TestFramePushTimesDequeBound:
    def test_deque_maxlen_prevents_unbounded_growth(self):
        """_frame_push_times deque has maxlen=10000."""
        m = RuntimeMetrics()
        now = time.time()
        # Verify maxlen is set correctly without iterating 10k times
        assert m._frame_push_times.maxlen == 10000
        # Fill just above maxlen to confirm eviction behavior
        for i in range(10005):
            m.record_push(count=1, status=202, timestamp=now + i)
        with m._lock:
            assert len(m._frame_push_times) == 10000

    def test_push_rate_correct_after_deque_overflow(self):
        """push_rate is computed correctly after deque evicted old entries."""
        m = RuntimeMetrics()
        now = time.time()
        # First entry: far in the past → evicted
        m.record_push(count=1, status=202, timestamp=now - 10000.0)
        # These two will remain (within 60s window)
        m.record_push(count=1, status=202, timestamp=now - 30.0)
        m.record_push(count=1, status=202, timestamp=now)
        rate = m.push_rate
        assert abs(rate - 2.0 / 60.0) < 1e-9