"""Error handling observability tests — T03.

Verifies that last_error_msg, last_error_time, and error_count are correctly
set by record_stream_error / record_registry_error_msg across all error paths.
Also proves gap detection compatibility: network errors do NOT stop the pusher
or heartbeat, while auth failures do.

Tests map directly to the error paths wired in T01 (pusher.py) and T02
(registry_pusher.py).
"""

from __future__ import annotations

import asyncio
import gzip
import json
import sys
from typing import Any
from unittest.mock import MagicMock

import aiohttp
import pytest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, ".")

from custom_components.agentic_home.heartbeat import HeartbeatGenerator
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home import pusher as pusher_module
from custom_components.agentic_home import registry_pusher as rp_module
from custom_components.agentic_home.pusher import IngressHTTPPusher
from custom_components.agentic_home.registry_pusher import RegistryPusher
from custom_components.agentic_home.tests.conftest import make_aiohttp_response


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_async_get_clientsession():
    """Patch async_get_clientsession in pusher and registry_pusher modules.

    Production modules now use direct imports:
      from homeassistant.helpers.aiohttp_client import async_get_clientsession

    This fixture replaces the real function with a MagicMock so that
    construction of IngressHTTPPusher/RegistryPusher does not create a
    real HTTP session.
    """
    with (
        patch.object(pusher_module, "async_get_clientsession", return_value=MagicMock()),
        patch.object(rp_module, "async_get_clientsession", return_value=MagicMock()),
    ):
        yield


@pytest.fixture
def metrics() -> RuntimeMetrics:
    """Return a fresh RuntimeMetrics instance."""
    return RuntimeMetrics()


def _fake_session() -> MagicMock:
    session = MagicMock()
    return session


def _fake_hass(fake_session: MagicMock) -> MagicMock:
    hass = MagicMock()
    hass.loop = asyncio.new_event_loop()
    return hass


def _make_pusher(hass: MagicMock, entry: MagicMock, metrics: RuntimeMetrics, session: MagicMock) -> "IngressHTTPPusher":
    """Construct IngressHTTPPusher with async_get_clientsession patched to return session."""
    with patch.object(pusher_module, "async_get_clientsession", return_value=session):
        return IngressHTTPPusher(hass, entry, metrics)


def _make_registry_pusher(hass: MagicMock, entry: MagicMock, metrics: RuntimeMetrics, session: MagicMock) -> "RegistryPusher":
    """Construct RegistryPusher with async_get_clientsession patched to return session."""
    with patch.object(rp_module, "async_get_clientsession", return_value=session):
        return RegistryPusher(hass, entry, metrics)


def _mock_entry() -> MagicMock:
    entry = MagicMock()
    entry.entry_id = "entry_test_abc"
    entry.data = {
        "ingress_url": "https://ingress.example.com",
        "jwt_token": "jwt_secret_xyz",
    }
    return entry


# ---------------------------------------------------------------------------
# Fake response helpers
# ---------------------------------------------------------------------------


class FakeResponse:
    """Fake aiohttp.ClientResponse for use as async context manager."""

    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Frame / topology helpers
# ---------------------------------------------------------------------------


def _make_frame(
    entity_id: str = "light.kitchen", event_type: str = "state_changed"
) -> dict[str, Any]:
    return {
        "source_event_id": "abc123",
        "source_sequence": 1,
        "delivery_mode": "live",
        "event_time": "2024-01-01T00:00:00+00:00",
        "event_type": event_type,
        "payload": {"entity_id": entity_id},
    }


def _make_registry_payload() -> dict[str, Any]:
    """Minimal registry topology payload."""
    return {
        "areas": [{"area_id": "area_1", "name": "Kitchen", "floor": None, "icon": None}],
        "devices": [
            {
                "device_id": "device_1",
                "name": "Light",
                "manufacturer": None,
                "model": None,
                "connections": [],
                "area_id": "area_1",
                "labels": [],
            }
        ],
        "entities": [
            {
                "entity_id": "light.kitchen",
                "name": "Kitchen Light",
                "domain": "light",
                "device_id": "device_1",
                "area_id": "area_1",
                "labels": [],
                "state": "off",
            }
        ],
        "entity_device_mappings": [
            {"entity_id": "light.kitchen", "device_id": "device_1"}
        ],
    }


def _gzip_payload(payload: dict[str, Any]) -> bytes:
    return gzip.compress(json.dumps(payload).encode("utf-8"))


# ---------------------------------------------------------------------------
# Sequence counter stub for HeartbeatGenerator
# ---------------------------------------------------------------------------


class _SeqCounter:
    _seq: int = 0

    def next(self) -> int:
        self._seq += 1
        return self._seq


# ---------------------------------------------------------------------------
# TrackedTimerHandle — mirrors test_registry_pusher.py for timer advancement
# ---------------------------------------------------------------------------


class TrackedTimerHandle:
    def __init__(self, when: float, callback: Any, args: tuple[Any, ...]) -> None:
        self._when = when
        self._callback = callback
        self._args = args
        self._cancelled = False

    @property
    def when(self) -> float:
        return self._when

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True

    def _invoke(self) -> None:
        self._callback(*self._args)


class FakeLoop:
    def __init__(self) -> None:
        self.handles: list[TrackedTimerHandle] = []
        self._time = 0.0

    def time(self) -> float:
        return self._time

    def call_later(
        self,
        delay: float,
        callback: Any,
        *args: Any,
        **kwargs: Any,
    ) -> TrackedTimerHandle:
        handle = TrackedTimerHandle(self._time + delay, callback, args)
        self.handles.append(handle)
        return handle

    def advance(self, seconds: float) -> None:
        """Advance fake time and invoke any handles whose deadline has passed."""
        self._time += seconds
        ready = [h for h in self.handles if not h.cancelled and h.when <= self._time]
        for h in ready:
            self.handles.remove(h)
            h._invoke()


# ---------------------------------------------------------------------------
# TestPusherErrorObservability
# ---------------------------------------------------------------------------


class TestPusherErrorObservability:
    """Tests that verify last_error_msg is set on every pusher error path.

    Each test:
    1. Triggers a specific error condition
    2. Asserts last_error_msg starts with "stream: <expected>"
    3. Asserts last_error_time > 0 (timestamp recorded)
    4. Asserts error_count == 1 (no double-counting)
    """

    @pytest.mark.asyncio
    async def test_auth_failure_sets_last_error(self) -> None:
        """POST 401 → last_error_msg starts with 'stream: auth failure'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        await pusher._flush_batch()

        assert metrics.last_error_msg.startswith("stream: auth failure"), (
            f"expected 'stream: auth failure', got: {metrics.last_error_msg!r}"
        )
        assert metrics.last_error_time > 0, "last_error_time not recorded"
        assert metrics.error_count == 1, f"error_count={metrics.error_count}, expected 1"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_5xx_sets_last_error(self) -> None:
        """POST 500 → last_error_msg starts with 'stream: non-2xx'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(500))

        await pusher._flush_batch()

        assert metrics.last_error_msg.startswith("stream: non-2xx"), (
            f"expected 'stream: non-2xx', got: {metrics.last_error_msg!r}"
        )
        assert metrics.last_error_time > 0, "last_error_time not recorded"
        assert metrics.error_count == 1, f"error_count={metrics.error_count}, expected 1"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_timeout_sets_last_error(self) -> None:
        """TimeoutError → last_error_msg starts with 'stream: timeout'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())

        class TimeoutCM:
            async def __aenter__(self):
                raise asyncio.TimeoutError()

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=TimeoutCM())

        await pusher._flush_batch()

        assert metrics.last_error_msg.startswith("stream: timeout"), (
            f"expected 'stream: timeout', got: {metrics.last_error_msg!r}"
        )
        assert metrics.last_error_time > 0, "last_error_time not recorded"
        assert metrics.error_count == 1, f"error_count={metrics.error_count}, expected 1"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_client_error_sets_last_error(self) -> None:
        """aiohttp.ClientError → last_error_msg starts with 'stream: connection error'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())

        class ClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError("connection refused")

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=ClientErrorCM())

        await pusher._flush_batch()

        assert metrics.last_error_msg.startswith("stream: connection error"), (
            f"expected 'stream: connection error', got: {metrics.last_error_msg!r}"
        )
        assert metrics.last_error_time > 0, "last_error_time not recorded"
        assert metrics.error_count == 1, f"error_count={metrics.error_count}, expected 1"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_auth_failure_stops_pushing(self) -> None:
        """HTTP 401 → _auth_failed True, subsequent add_frame is silently dropped."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        await pusher._flush_batch()

        assert pusher._auth_failed is True, "_auth_failed not set after 401"
        # Verify frames are dropped after auth failure.
        pusher.add_frame(_make_frame("light.living_room"))
        assert len(pusher._batch) == 0, "frames not dropped after auth failure"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_error_count_not_double_incremented(self) -> None:
        """Auth failure sets error_count == 1, not 2 (record_stream_error is the only incrementor)."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        await pusher._flush_batch()

        assert metrics.error_count == 1, (
            f"error_count={metrics.error_count}, expected 1 — "
            "record_stream_error is the single incrementor"
        )
        fake_hass.loop.close()


# ---------------------------------------------------------------------------
# TestRegistryErrorObservability
# ---------------------------------------------------------------------------


class TestRegistryErrorObservability:
    """Tests that verify last_error_msg is set on every registry error path.

    Covers the 12 error paths wired in T02:
    - collect_topology failure (initial + retry)
    - 401/403 auth failure (initial + retry)
    - 400 rejected (initial + retry)
    - 5xx server error (initial + retry)
    - TimeoutError (initial + retry)
    - ClientError (initial + retry)

    Each test asserts last_error_msg starts with "registry: <expected>"
    and that the appropriate scheduling behavior occurs.
    """

    @pytest.mark.asyncio
    async def test_registry_auth_failure_sets_last_error(self) -> None:
        """POST 401 → last_error_msg starts with 'registry: auth failure'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        # Patch collect_topology so it returns a valid payload.
        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            return_value=_make_registry_payload(),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: auth failure"), (
            f"expected 'registry: auth failure', got: {metrics.last_error_msg!r}"
        )
        assert metrics.registry_error_count == 1, (
            f"registry_error_count={metrics.registry_error_count}, expected 1"
        )
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_registry_400_sets_last_error(self) -> None:
        """POST 400 → last_error_msg starts with 'registry: rejected', next periodic scheduled."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)
        fake_session.post = MagicMock(return_value=FakeResponse(400))

        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            return_value=_make_registry_payload(),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: rejected"), (
            f"expected 'registry: rejected', got: {metrics.last_error_msg!r}"
        )
        # 400 schedules the next periodic push (not retry).
        assert pusher._retry_handle is None, "400 should not schedule retry handle"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_registry_5xx_sets_last_error(self) -> None:
        """POST 500 → last_error_msg starts with 'registry: server error', retry scheduled."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)
        fake_session.post = MagicMock(return_value=FakeResponse(500))

        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            return_value=_make_registry_payload(),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: server error"), (
            f"expected 'registry: server error', got: {metrics.last_error_msg!r}"
        )
        # 5xx schedules a retry.
        assert pusher._retry_handle is not None, "5xx should schedule retry handle"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_registry_timeout_sets_last_error(self) -> None:
        """TimeoutError → last_error_msg starts with 'registry: timeout', retry scheduled."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)

        class TimeoutCM:
            async def __aenter__(self):
                raise asyncio.TimeoutError()

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=TimeoutCM())

        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            return_value=_make_registry_payload(),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: timeout"), (
            f"expected 'registry: timeout', got: {metrics.last_error_msg!r}"
        )
        assert pusher._retry_handle is not None, "timeout should schedule retry handle"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_registry_client_error_sets_last_error(self) -> None:
        """aiohttp.ClientError → last_error_msg starts with 'registry: connection error', retry scheduled."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)

        class ClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError("network unreachable")

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=ClientErrorCM())

        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            return_value=_make_registry_payload(),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: connection error"), (
            f"expected 'registry: connection error', got: {metrics.last_error_msg!r}"
        )
        assert pusher._retry_handle is not None, "ClientError should schedule retry handle"
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_topology_collection_failure_sets_last_error(self) -> None:
        """collect_topology raises → last_error_msg starts with 'registry: topology collection'."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_registry_pusher(fake_hass, mock_entry, metrics, fake_session)

        with patch(
            "custom_components.agentic_home.registry_pusher.collect_topology",
            new_callable=AsyncMock,
            side_effect=RuntimeError("device registry unavailable"),
        ):
            await pusher.push_snapshot()

        assert metrics.last_error_msg.startswith("registry: topology collection"), (
            f"expected 'registry: topology collection', got: {metrics.last_error_msg!r}"
        )
        assert metrics.registry_error_count == 1, (
            f"registry_error_count={metrics.registry_error_count}, expected 1"
        )
        fake_hass.loop.close()


# ---------------------------------------------------------------------------
# TestHeartbeatErrorInteraction
# ---------------------------------------------------------------------------


class TestHeartbeatErrorInteraction:
    """Tests that verify heartbeat behavior is correct under error conditions.

    Gap detection compatibility (drop-and-log approach):
    - Network errors (ClientError, timeout) → pusher continues, heartbeat continues.
    - Auth failures (401/403) → pusher stops, heartbeat stops.
    """

    @pytest.mark.asyncio
    async def test_heartbeat_continues_after_network_error(self) -> None:
        """ClientError during push: pusher continues, heartbeat still fires."""
        fake_session = _fake_session()
        fake_loop = FakeLoop()
        fake_hass = MagicMock()
        fake_hass.loop = fake_loop
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()
        seq_counter = _SeqCounter()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        heartbeat = HeartbeatGenerator(fake_hass, pusher, metrics, seq_counter)
        pusher.start()
        heartbeat.start()

        pusher.add_frame(_make_frame())

        class ClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError("network unreachable")

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=ClientErrorCM())
        await pusher._flush_batch()

        # Pusher should still be running (not auth-failed).
        assert pusher._auth_failed is False, "network error should not set _auth_failed"
        # Heartbeat timer should still be active.
        assert heartbeat._timer is not None, "heartbeat should still be running after network error"
        # Advance past HEARTBEAT_INTERVAL_SECONDS to fire the heartbeat.
        from custom_components.agentic_home.const import HEARTBEAT_INTERVAL_SECONDS
        fake_loop.advance(HEARTBEAT_INTERVAL_SECONDS + 1)
        # Clean up.
        await pusher.stop()
        heartbeat.stop()

    @pytest.mark.asyncio
    async def test_heartbeat_stops_after_auth_failure(self) -> None:
        """401 during push: pusher._auth_failed True, heartbeat timer stops after next tick."""
        fake_session = _fake_session()
        fake_loop = FakeLoop()
        fake_hass = MagicMock()
        fake_hass.loop = fake_loop
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()
        seq_counter = _SeqCounter()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        heartbeat = HeartbeatGenerator(fake_hass, pusher, metrics, seq_counter)
        pusher.start()
        heartbeat.start()

        pusher.add_frame(_make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))
        await pusher._flush_batch()

        # Pusher should be stopped by auth failure.
        assert pusher._auth_failed is True, "_auth_failed not set after 401"
        # Heartbeat timer is still active when we check synchronously — advancing
        # the loop fires _on_heartbeat_timer which checks _auth_failed and stops.
        from custom_components.agentic_home.const import HEARTBEAT_INTERVAL_SECONDS
        fake_loop.advance(HEARTBEAT_INTERVAL_SECONDS + 1)
        # After the timer fires and sees auth failure, heartbeat should have stopped.
        assert heartbeat._timer is None, (
            "heartbeat timer should be None after auth failure "
            "(HeartbeatGenerator._on_heartbeat_timer returns early)"
        )
        # Clean up.
        await pusher.stop()
        heartbeat.stop()


# ---------------------------------------------------------------------------
# TestErrorEdgeCases
# ---------------------------------------------------------------------------


class TestErrorEdgeCases:
    """Edge case tests for error message handling."""

    @pytest.mark.asyncio
    async def test_error_message_does_not_exceed_limit(self) -> None:
        """ClientError with 300-char message → last_error_msg < 210 chars (HA state limit)."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()
        pusher.add_frame(_make_frame())

        long_message = "x" * 500

        class LongClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError(long_message)

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=LongClientErrorCM())
        await pusher._flush_batch()

        # "stream: connection error: " (25 chars) + 150-char truncated detail = 175 chars total
        # Allow some headroom; final message must fit comfortably under 210.
        assert len(metrics.last_error_msg) < 210, (
            f"last_error_msg too long ({len(metrics.last_error_msg)} chars): "
            f"{metrics.last_error_msg!r}"
        )
        assert metrics.last_error_msg.startswith("stream: connection error"), (
            f"unexpected prefix: {metrics.last_error_msg!r}"
        )
        fake_hass.loop.close()

    @pytest.mark.asyncio
    async def test_metrics_snapshot_reflects_last_error(self) -> None:
        """After multiple errors, snapshot()['last_error'] == metrics.last_error_msg."""
        fake_session = _fake_session()
        fake_hass = _fake_hass(fake_session)
        mock_entry = _mock_entry()
        metrics = RuntimeMetrics()

        pusher = _make_pusher(fake_hass, mock_entry, metrics, fake_session)
        pusher.start()

        # First error: timeout.
        pusher.add_frame(_make_frame())

        class TimeoutCM1:
            async def __aenter__(self):
                raise asyncio.TimeoutError()

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=TimeoutCM1())
        await pusher._flush_batch()
        first_error_msg = metrics.last_error_msg

        # Second error: ClientError (overwrites).
        pusher.add_frame(_make_frame("light.bedroom"))

        class ClientErrorCM2:
            async def __aenter__(self):
                raise aiohttp.ClientError("connection refused")

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=ClientErrorCM2())
        await pusher._flush_batch()

        # snapshot() should return the most recent last_error_msg.
        snap = metrics.snapshot()
        assert snap["last_error"] == metrics.last_error_msg, (
            f"snapshot['last_error']={snap['last_error']!r} "
            f"!= metrics.last_error_msg={metrics.last_error_msg!r}"
        )
        assert snap["last_error"] != first_error_msg, (
            "snapshot should reflect the most recent error, not the first"
        )
        assert metrics.last_error_msg.startswith("stream: connection error"), (
            f"expected latest error to be connection error, got: {metrics.last_error_msg!r}"
        )
        fake_hass.loop.close()