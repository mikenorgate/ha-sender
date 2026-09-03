"""Tests for registry_pusher.py."""

from __future__ import annotations

import asyncio
import gzip
import json
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp

sys.path.insert(0, ".")

from custom_components.agentic_home import registry_pusher


# ---------------------------------------------------------------------------
# Fake event loop helper (avoids creating a real loop in tests)
# ---------------------------------------------------------------------------


class TrackedTimerHandle:
    """Records a deferred callback for later synchronous inspection."""

    def __init__(self, when: float, callback: Any, args: tuple[Any, ...]) -> None:
        self._when = when
        self._callback = callback
        self._args = args
        self._cancelled = False

    @property
    def when(self) -> float:
        return self._when

    @property  # type: ignore[misc]
    def cancelled(self) -> bool:  # type: ignore[misc]
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True

    def _invoke(self) -> None:
        self._callback(*self._args)


class FakeLoop:
    """Fake asyncio event loop that tracks call_later handles."""

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


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_hass() -> MagicMock:
    hass = MagicMock()
    hass.loop = FakeLoop()
    return hass


def _make_entry(
    ingress_url: str = "http://localhost:8080",
    jwt_token: str = "test-jwt-token",
) -> MagicMock:
    entry = MagicMock()
    entry.entry_id = "entry_pusher_test"
    entry.data = {
        "ingress_url": ingress_url,
        "jwt_token": jwt_token,
        "integration_id": "hh_test",
    }
    return entry


def _make_metrics() -> MagicMock:
    from custom_components.agentic_home.metrics import RuntimeMetrics

    return RuntimeMetrics()


def _make_response(
    status: int,
    body: bytes | str = b"",
) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.read = AsyncMock(return_value=body)
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=None)
    return response


# ---------------------------------------------------------------------------
# Scenario 1: Successful push (202 response)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_push_records_metrics():
    """A 202 response records push count and last push time in metrics."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    # Mock topology response
    fake_snapshot = {
        "areas": [{"id": "a1", "name": "Kitchen"}],
        "devices": [{"id": "d1", "name": "Oven"}],
        "entity_device_mappings": [],
        "floors": [],
        "labels": [],
        "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(
        return_value=_make_response(202),
    )

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    assert metrics.registry_push_count == 1
    assert metrics.registry_last_push_time > 0
    assert pusher._auth_failed is False


# ---------------------------------------------------------------------------
# Scenario 2: Auth failure (401) — permanent stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auth_failure_stops_pusher_permanently():
    """A 401 response sets _auth_failed = True and does not retry."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(401))

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    assert pusher._auth_failed is True
    assert metrics.registry_error_count == 1
    # No handles scheduled (no retry, no periodic)
    assert len(hass.loop.handles) == 0


# ---------------------------------------------------------------------------
# Scenario 3: Server error (503) — retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_503_triggers_retry_with_backoff():
    """A 503 response schedules a retry at the first backoff delay."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(503))

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    # One retry handle scheduled with 30s delay (first backoff step)
    assert len(hass.loop.handles) == 1
    handle = hass.loop.handles[0]
    assert handle.when == pytest.approx(30.0, abs=1.0)
    assert metrics.registry_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 4: Network error — retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_network_error_triggers_retry():
    """A ClientError exception schedules a retry."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    # Simulate a client error on post
    async def raise_client_error(*args: Any, **kwargs: Any) -> MagicMock:
        raise aiohttp.ClientError("connection refused")

    response = MagicMock()
    response.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
    response.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=response)

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    assert len(hass.loop.handles) == 1
    assert metrics.registry_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 5: All retries exhausted — schedule next periodic push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_retries_exhausted_schedules_periodic():
    """After _MAX_RETRIES failures, the next periodic push is scheduled."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(503))

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        # Exhaust retries by calling _schedule_retry with attempt=3 (MAX_RETRIES=3)
        pusher._schedule_retry(attempt=3)

    # After max retries, periodic handle is scheduled (3600 ± jitter)
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    # Interval should be in range [3480, 3720]
    assert 3480 <= handles[0].when <= 3720


# ---------------------------------------------------------------------------
# Scenario 6: Unload while retry in progress — timers cancelled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_cancels_timers():
    """stop() cancels both periodic and retry handles."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)

    # Create handles and assign them to the pusher
    periodic = hass.loop.call_later(3600.0, lambda: None)
    retry = hass.loop.call_later(30.0, lambda: None)
    pusher._periodic_handle = periodic
    pusher._retry_handle = retry

    # Verify handles are not cancelled yet
    assert not periodic.cancelled
    assert not retry.cancelled

    await pusher.stop()

    # Handles should be cancelled
    assert periodic.cancelled
    assert retry.cancelled
    assert pusher._stopped is True
    # Handles cleared from pusher
    assert pusher._periodic_handle is None
    assert pusher._retry_handle is None


# ---------------------------------------------------------------------------
# Scenario 7: Verify gzip body encoding in POST request mock
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_sends_gzip_encoded_body():
    """The POST body is gzip-compressed JSON, and Content-Encoding is gzip."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [{"id": "area_1", "name": "Office"}],
        "devices": [{"id": "dev_1", "name": "Desk Lamp"}],
        "entity_device_mappings": [],
        "floors": [],
        "labels": [],
        "label_assignments": [],
    }

    captured_request: dict[str, Any] = {}

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_request["url"] = url
        captured_request["headers"] = kwargs.get("headers", {})
        captured_request["data"] = kwargs.get("data", b"")
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    # Verify Content-Encoding header
    assert captured_request["headers"].get("Content-Encoding") == "gzip"

    # Verify body is valid gzip-compressed JSON
    gzipped_data = captured_request["data"]
    decompressed = gzip.decompress(gzipped_data)
    parsed = json.loads(decompressed.decode("utf-8"))
    assert parsed["areas"][0]["name"] == "Office"


# ---------------------------------------------------------------------------
# Scenario 8: start() pushes immediately and schedules periodic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_pushes_immediately_and_schedules_periodic():
    """start() fires an immediate push and schedules the next periodic run."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(202))

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        pusher.start()

    # _inflight_task is scheduled
    assert pusher._inflight_task is not None
    # Run the task to ensure push completes
    await pusher._inflight_task

    assert metrics.registry_push_count == 1


# ---------------------------------------------------------------------------
# Scenario 9: Topology collection failure schedules retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_topology_failure_schedules_retry():
    """An exception from collect_topology schedules a retry, not a periodic push."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.side_effect = RuntimeError("registry unavailable")
        await pusher.push_snapshot()

    # One retry handle scheduled
    assert len(hass.loop.handles) == 1
    assert metrics.registry_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 10: 400 response schedules next periodic, not retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_400_schedules_periodic_not_retry():
    """A 400 response schedules the next periodic push (no retry)."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    fake_snapshot = {
        "areas": [], "devices": [], "entity_device_mappings": [],
        "floors": [], "labels": [], "label_assignments": [],
    }

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(400))

    pusher = registry_pusher.RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    with patch.object(registry_pusher, "collect_topology", new_callable=AsyncMock) as mock_collect:
        mock_collect.return_value = fake_snapshot
        await pusher.push_snapshot()

    # One periodic handle scheduled (jitter range)
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    assert 3480 <= handles[0].when <= 3720
    assert metrics.registry_error_count == 1