"""Tests for catalog_pusher.py."""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp

sys.path.insert(0, ".")

from custom_components.agentic_home import catalog_pusher


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

    # Mock the HA event bus with async_listen (returns an unregister callable).
    mock_bus = MagicMock()
    mock_bus.async_listen = MagicMock(return_value=MagicMock())
    hass.bus = mock_bus

    return hass


def _make_entry(
    ingress_url: str = "http://localhost:8080",
    jwt_token: str = "test-jwt-token",
) -> MagicMock:
    entry = MagicMock()
    entry.entry_id = "entry_catalog_test"
    entry.data = {
        "ingress_url": ingress_url,
        "jwt_token": jwt_token,
        "integration_id": "hh_test",
    }
    return entry


def _make_metrics() -> Any:
    from custom_components.agentic_home.metrics import RuntimeMetrics

    return RuntimeMetrics()


def _make_response(status: int) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=None)
    return response


def _make_hass_with_services(
    services: dict[str, dict[str, object]],
    entity_ids: dict[str, list[str]] | None = None,
) -> MagicMock:
    """Build a hass mock with services.async_services and states.async_entity_ids."""
    hass = _make_hass()

    mock_services_obj = MagicMock()
    mock_services_obj.async_services = MagicMock(return_value=services)
    hass.services = mock_services_obj

    mock_states_obj = MagicMock()
    if entity_ids is not None:
        def _entity_ids(domain: str) -> list[str]:
            return entity_ids.get(domain, [])
        mock_states_obj.async_entity_ids = MagicMock(side_effect=_entity_ids)
    else:
        mock_states_obj.async_entity_ids = MagicMock(return_value=[])
    hass.states = mock_states_obj

    return hass


# ---------------------------------------------------------------------------
# Scenario 1: Successful push (202 response) — single chunk
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_push_records_metrics():
    """A 202 response records catalog push count and schedules periodic."""
    services = {
        "light": {"turn_on": object(), "turn_off": object()},
    }
    entity_ids = {
        "light": ["light.kitchen", "light.living_room"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(202))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # 2 entities × 2 services = 4 frames, 1 chunk
    assert metrics.catalog_push_count == 1
    assert metrics.catalog_last_push_time > 0
    assert pusher._auth_failed is False

    # Periodic handle scheduled (jitter range)
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    assert 3480 <= handles[0].when <= 3720


# ---------------------------------------------------------------------------
# Scenario 2: Auth failure (401) — permanent stop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auth_failure_stops_catalog_pusher_permanently():
    """A 401 response sets _auth_failed = True and does not retry."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(401))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    assert pusher._auth_failed is True
    assert metrics.catalog_error_count == 1
    # No handles scheduled (no retry, no periodic)
    assert len(hass.loop.handles) == 0


# ---------------------------------------------------------------------------
# Scenario 3: 403 also stops permanently
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_403_stops_catalog_pusher():
    """A 403 also sets _auth_failed = True."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(403))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    assert pusher._auth_failed is True
    assert metrics.catalog_error_count == 1
    assert len(hass.loop.handles) == 0


# ---------------------------------------------------------------------------
# Scenario 4: Server error (503) — retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_503_triggers_retry_with_backoff():
    """A 503 response schedules a retry at the first backoff delay."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(503))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # One retry handle scheduled with 30s delay (first backoff step)
    assert len(hass.loop.handles) == 1
    handle = hass.loop.handles[0]
    assert handle.when == pytest.approx(30.0, abs=1.0)
    assert metrics.catalog_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 5: Network error — retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_network_error_triggers_retry():
    """A ClientError exception schedules a retry."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    response = MagicMock()
    response.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
    response.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=response)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    assert len(hass.loop.handles) == 1
    assert metrics.catalog_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 6: Timeout error — retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeout_triggers_retry():
    """An asyncio.TimeoutError schedules a retry."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    response = MagicMock()
    response.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError("timed out"))
    response.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=response)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    assert len(hass.loop.handles) == 1
    assert metrics.catalog_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 7: All retries exhausted — schedule next periodic push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_retries_exhausted_schedules_periodic():
    """After _MAX_RETRIES failures, the next periodic push is scheduled."""
    services = {
        "switch": {"toggle": object()},
    }
    entity_ids = {
        "switch": ["switch.one"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(503))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    # Exhaust retries by calling _schedule_retry with attempt=3 (MAX_RETRIES=3)
    pusher._schedule_retry(attempt=3)

    # After max retries, periodic handle is scheduled (3600 ± jitter)
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    assert 3480 <= handles[0].when <= 3720


# ---------------------------------------------------------------------------
# Scenario 8: stop() cancels timers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_cancels_timers():
    """stop() cancels both periodic and retry handles."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)

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
# Scenario 9: NDJSON body format verification
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ndjson_body_format():
    """The POST body is valid NDJSON with Content-Type application/x-ndjson."""
    services = {
        "light": {"turn_on": object(), "turn_off": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

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

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # Verify Content-Type header (no gzip)
    assert captured_request["headers"].get("Content-Type") == "application/x-ndjson"
    assert "Content-Encoding" not in captured_request["headers"]

    # Verify body is valid NDJSON (each line is valid JSON, separated by \n)
    body = captured_request["data"].decode("utf-8")
    lines = body.strip("\n").split("\n")
    for line in lines:
        parsed = json.loads(line)
        assert parsed["event_type"] == "action_catalog"
        assert parsed["delivery_mode"] == "catalog"
        assert "entity_id" in parsed["payload"]
        assert "domain" in parsed["payload"]
        assert "service_name" in parsed["payload"]

    # 1 entity × 2 services = 2 frames
    assert len(lines) == 2


# ---------------------------------------------------------------------------
# Scenario 10: start() pushes immediately
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_pushes_immediately():
    """start() fires an immediate push."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(202))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    pusher.start()

    # _inflight_task is scheduled
    assert pusher._inflight_task is not None
    # Run the task to ensure push completes
    await pusher._inflight_task

    assert metrics.catalog_push_count == 1


# ---------------------------------------------------------------------------
# Scenario 11: Enumeration failure schedules retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enumeration_failure_schedules_retry():
    """An exception from async_services schedules a retry."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    # services.async_services raises
    mock_services = MagicMock()
    mock_services.async_services = MagicMock(side_effect=RuntimeError("HA services unavailable"))
    hass.services = mock_services

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # One retry handle scheduled
    assert len(hass.loop.handles) == 1
    assert metrics.catalog_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 12: 400 response schedules next periodic, not retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_400_schedules_periodic_not_retry():
    """A 400 response schedules the next periodic push (no retry)."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(400))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # One periodic handle scheduled (jitter range)
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    assert 3480 <= handles[0].when <= 3720
    assert metrics.catalog_error_count == 1


# ---------------------------------------------------------------------------
# Scenario 13: Entity×service cross-product construction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_entity_service_cross_product():
    """Multiple entities × multiple services produce correct frame count."""
    services = {
        "light": {"turn_on": object(), "turn_off": object(), "toggle": object()},
        "switch": {"turn_on": object(), "turn_off": object()},
    }
    entity_ids = {
        "light": ["light.kitchen", "light.living_room"],
        "switch": ["switch.coffee"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    captured_bodies: list[str] = []

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_bodies.append(kwargs.get("data", b"").decode("utf-8"))
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # 2 light entities × 3 services + 1 switch entity × 2 services = 8 frames
    total_frames = 0
    for body in captured_bodies:
        lines = body.strip("\n").split("\n")
        total_frames += len(lines)

    assert total_frames == 8


# ---------------------------------------------------------------------------
# Scenario 14: Domains with no entities are skipped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_domains_skipped():
    """Domains with no entities produce no frames."""
    services = {
        "light": {"turn_on": object()},
        "system_log": {"clear": object()},
        "homeassistant": {"restart": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
        # system_log and homeassistant have no entities
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    captured_bodies: list[str] = []

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_bodies.append(kwargs.get("data", b"").decode("utf-8"))
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # Only light domain — 1 entity × 1 service = 1 frame
    total_frames = 0
    for body in captured_bodies:
        lines = body.strip("\n").split("\n")
        total_frames += len(lines)
    assert total_frames == 1


# ---------------------------------------------------------------------------
# Scenario 15: Chunked posting when frames exceed 500
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chunked_posting_over_500():
    """When there are more than 500 frames, they are split across multiple POSTs."""
    # 3 services × 200 entities = 600 frames (> 500), split into 2 chunks
    services = {
        "light": {"s1": object(), "s2": object(), "s3": object()},
    }
    entity_ids = {
        "light": [f"light.e{i:03d}" for i in range(200)],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    post_count = 0

    def count_post(url: str, **kwargs: Any) -> MockResponse:
        nonlocal post_count
        post_count += 1
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=count_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # 600 frames / 500 per chunk = 2 chunks
    assert post_count == 2
    assert metrics.catalog_push_count == 2


# ---------------------------------------------------------------------------
# Scenario 16: No frames produced (all domains empty) — schedule periodic only
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_frames_schedules_periodic():
    """When no domains have entities, just schedule periodic with no POST."""
    services = {
        "homeassistant": {"restart": object()},
    }
    entity_ids: dict[str, list[str]] = {}
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # No POSTs made
    session.post.assert_not_called()

    # Periodic handle scheduled
    handles = [h for h in hass.loop.handles]
    assert len(handles) == 1
    assert 3480 <= handles[0].when <= 3720


# ---------------------------------------------------------------------------
# Scenario 17: CatalogPusher uses its own SequenceCounter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_own_sequence_counter():
    """CatalogPusher has its own SequenceCounter, not shared."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    captured_bodies: list[str] = []

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_bodies.append(kwargs.get("data", b"").decode("utf-8"))
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # Verify source_sequence begins with the millisecond epoch (counter start value)
    for body in captured_bodies:
        lines = body.strip("\n").split("\n")
        for line in lines:
            frame = json.loads(line)
            assert frame["source_sequence"] > 0
            assert isinstance(frame["source_sequence"], int)


# ---------------------------------------------------------------------------
# Scenario 18: Retry with 202 on second attempt records success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt():
    """When the first POST returns 503, the retry returns 202, metrics record success."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    # First call returns 503, retry sequence calls
    call_count = 0

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status

        async def __aenter__(self) -> "MockResponse":
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

    def varying_response(url: str, **kwargs: Any) -> MockResponse:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return MockResponse(503)
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=varying_response)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    # First push_catalog will fail 503 and schedule retry
    await pusher.push_catalog()

    # Retry handle scheduled
    assert len(hass.loop.handles) == 1
    # Clear handles for the retry push
    hass.loop.handles.clear()

    # Simulate retry: _retry_push with attempt=0
    await pusher._retry_push(attempt=0)

    # Now catalog push count should be 1 from the retry
    assert metrics.catalog_push_count == 1
    assert metrics.catalog_error_count == 1  # One error from initial attempt


# ---------------------------------------------------------------------------
# Scenario 19: auth_failed guards prevent any push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_auth_failed_prevents_push():
    """After auth_failed is set, push_catalog and start() are no-ops."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session
    pusher._auth_failed = True

    await pusher.push_catalog()
    session.post.assert_not_called()

    pusher.start()
    assert pusher._inflight_task is None


# ---------------------------------------------------------------------------
# Scenario 20: start() registers service_registered listener
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_registers_service_registered_listener():
    """start() subscribes to service_registered events on the HA bus."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(202))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    pusher.start()

    # Bus listener was registered.
    hass.bus.async_listen.assert_called_once_with(
        "service_registered", pusher._on_service_registered
    )
    # Unsubscribe function stored.
    assert pusher._unsub is not None

    # Run inflight to avoid orphaned task warnings.
    await pusher._inflight_task


# ---------------------------------------------------------------------------
# Scenario 21: _on_service_registered creates a task for domain push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_service_registered_creates_push_task():
    """_on_service_registered dispatches _push_domain_catalog as a task."""
    services = {
        "light": {"turn_on": object(), "turn_off": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    captured_bodies: list[str] = []

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status
        async def __aenter__(self) -> "MockResponse":
            return self
        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_bodies.append(kwargs.get("data", b"").decode("utf-8"))
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    # Create a mock service_registered event.
    event = MagicMock()
    event.data = {"domain": "light", "service": "turn_on"}

    await pusher._on_service_registered(event)

    # Give the created task a chance to run.
    await asyncio.sleep(0)

    # 1 entity × 2 services = 2 frames posted
    assert metrics.catalog_push_count == 1
    assert len(captured_bodies) == 1
    lines = captured_bodies[0].strip("\n").split("\n")
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert parsed["payload"]["domain"] == "light"


# ---------------------------------------------------------------------------
# Scenario 22: _on_service_registered skips events without domain
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_service_registered_skips_missing_domain():
    """Events without a domain are silently skipped."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    event = MagicMock()
    event.data = {"service": "turn_on"}  # no domain

    await pusher._on_service_registered(event)

    # No POST should be made.
    session.post.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 23: _push_domain_catalog pushes frames for a single domain only
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_domain_catalog_single_domain():
    """_push_domain_catalog only enumerates entities/services for the given domain."""
    services = {
        "light": {"turn_on": object(), "turn_off": object()},
        "switch": {"toggle": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
        "switch": ["switch.coffee"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    captured_bodies: list[str] = []

    class MockResponse:
        def __init__(self, status: int) -> None:
            self.status = status
        async def __aenter__(self) -> "MockResponse":
            return self
        async def __aexit__(self, *args: Any) -> None:
            pass

    def capture_post(url: str, **kwargs: Any) -> MockResponse:
        captured_bodies.append(kwargs.get("data", b"").decode("utf-8"))
        return MockResponse(202)

    session = MagicMock()
    session.post = MagicMock(side_effect=capture_post)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    # Push only the "light" domain.
    await pusher._push_domain_catalog("light")

    # Only light domain frames: 1 entity × 2 services = 2 frames.
    assert metrics.catalog_push_count == 1
    lines = captured_bodies[0].strip("\n").split("\n")
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert parsed["payload"]["domain"] == "light"

    # Verify async_entity_ids was called with the correct domain.
    hass.states.async_entity_ids.assert_called_with("light")

    # Verify async_services was called only once.
    assert hass.services.async_services.call_count == 1


# ---------------------------------------------------------------------------
# Scenario 24: _push_domain_catalog handles async_entity_ids failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_domain_catalog_entity_ids_failure():
    """When async_entity_ids raises, the error is logged and metrics recorded."""
    hass = _make_hass()
    mock_states = MagicMock()
    mock_states.async_entity_ids = MagicMock(
        side_effect=RuntimeError("entity state unavailable")
    )
    hass.states = mock_states

    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher._push_domain_catalog("light")

    # Error recorded, no POST attempted.
    assert metrics.catalog_error_count == 1
    session.post.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 25: _push_domain_catalog handles async_services failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_domain_catalog_services_failure():
    """When async_services raises, the error is logged and metrics recorded."""
    hass = _make_hass()
    mock_states = MagicMock()
    mock_states.async_entity_ids = MagicMock(return_value=["light.kitchen"])
    hass.states = mock_states

    mock_services = MagicMock()
    mock_services.async_services = MagicMock(
        side_effect=RuntimeError("service registry unavailable")
    )
    hass.services = mock_services

    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher._push_domain_catalog("light")

    assert metrics.catalog_error_count == 1
    session.post.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 26: stop() cancels the event subscription
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_cancels_event_subscription():
    """stop() calls the unsub function and sets it to None."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    unsub_fn = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._periodic_handle = None
    pusher._retry_handle = None
    pusher._unsub = unsub_fn

    await pusher.stop()

    unsub_fn.assert_called_once()
    assert pusher._unsub is None


# ---------------------------------------------------------------------------
# Scenario 27: Incremental auth failure sets permanent-stop flag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incremental_auth_failure_sets_auth_failed():
    """A 401 on incremental push sets _auth_failed."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(401))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher._push_domain_catalog("light")

    assert pusher._auth_failed is True
    assert metrics.catalog_error_count == 1
    # No handles scheduled (incremental path doesn't schedule retries).
    assert len(hass.loop.handles) == 0


# ---------------------------------------------------------------------------
# Scenario 28: _push_domain_catalog skips when auth_failed or stopped
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_push_domain_catalog_skips_when_stopped():
    """_push_domain_catalog returns early when _auth_failed or _stopped."""
    hass = _make_hass()
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session
    pusher._auth_failed = True

    await pusher._push_domain_catalog("light")
    session.post.assert_not_called()

    pusher._auth_failed = False
    pusher._stopped = True

    await pusher._push_domain_catalog("light")
    session.post.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 29: Incremental push handles 503 — logs error, no retry scheduling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incremental_503_logs_error_no_retry():
    """A 503 on incremental push records an error but does NOT schedule a retry."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(503))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher._push_domain_catalog("light")

    assert metrics.catalog_error_count == 1
    # No retry or periodic handles.
    assert len(hass.loop.handles) == 0
    assert pusher._auth_failed is False


# ---------------------------------------------------------------------------
# Scenario 30: Incremental push handles ClientError — logs error, no retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incremental_client_error_logs_no_retry():
    """A ClientError on incremental push records error but does not schedule retry."""
    services = {
        "light": {"turn_on": object()},
    }
    entity_ids = {
        "light": ["light.kitchen"],
    }
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    response = MagicMock()
    response.__aenter__ = AsyncMock(
        side_effect=aiohttp.ClientError("connection refused")
    )
    response.__aexit__ = AsyncMock(return_value=None)
    session.post = MagicMock(return_value=response)

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher._push_domain_catalog("light")

    assert metrics.catalog_error_count == 1
    assert len(hass.loop.handles) == 0
    assert pusher._auth_failed is False


# ---------------------------------------------------------------------------
# Scenario 26: _extract_fields_from_schema extracts voluptuous data
# ---------------------------------------------------------------------------


def test_extract_fields_from_schema_with_real_voluptuous():
    """Real voluptuous schemas produce JSON-serializable field dicts."""
    import voluptuous as vol

    schema = vol.Schema({
        vol.Required("brightness", description="Brightness level"): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
        vol.Optional("color_temp"): vol.All(
            vol.Coerce(int), vol.Range(min=153, max=500)
        ),
        vol.Optional("rgb_color"): vol.Any(
            vol.Coerce(tuple), str
        ),
    })

    fields = catalog_pusher.CatalogPusher._extract_fields_from_schema(schema)

    assert "brightness" in fields
    assert fields["brightness"]["required"] is True
    assert fields["brightness"]["type"] == "int"
    assert fields["brightness"]["min"] == 0
    assert fields["brightness"]["max"] == 255
    assert fields["brightness"]["description"] == "Brightness level"

    assert "color_temp" in fields
    assert fields["color_temp"]["required"] is False
    assert fields["color_temp"]["type"] == "int"

    assert "rgb_color" in fields


def test_extract_fields_none_schema_returns_empty():
    """A None schema (services with no params) returns an empty dict."""
    fields = catalog_pusher.CatalogPusher._extract_fields_from_schema(None)
    assert fields == {}


def test_extract_fields_all_wrapped_schema():
    """All-wrapped schemas (common in HA) are unwrapped correctly."""
    import voluptuous as vol

    schema = vol.All(vol.Schema({
        vol.Required("entity_id"): str,
        vol.Optional("area_id"): str,
    }))

    fields = catalog_pusher.CatalogPusher._extract_fields_from_schema(schema)

    assert "entity_id" in fields
    assert fields["entity_id"]["required"] is True
    assert fields["entity_id"]["type"] == "str"
    assert "area_id" in fields


def test_extract_fields_in_validator():
    """In validators expose an options list."""
    import voluptuous as vol

    schema = vol.Schema({
        vol.Required("mode"): vol.In(["auto", "heat", "cool", "off"]),
    })

    fields = catalog_pusher.CatalogPusher._extract_fields_from_schema(schema)
    assert fields["mode"]["options"] == ["auto", "heat", "cool", "off"]


# ---------------------------------------------------------------------------
# Scenario 27: _make_frame includes fields from service schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_make_frame_with_service_object_includes_fields():
    """When a service object with a schema is passed, raw_action_data has fields."""
    import voluptuous as vol

    mock_service = MagicMock()
    mock_service.schema = vol.Schema({
        vol.Required("brightness"): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
    })

    hass = _make_hass_with_services({}, {})
    entry = _make_entry()
    metrics = _make_metrics()
    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)

    frame = pusher._make_frame(
        "light", "light.kitchen", "turn_on", service_obj=mock_service
    )

    raw = frame["payload"]["raw_action_data"]
    assert "fields" in raw
    assert "brightness" in raw["fields"]
    assert raw["fields"]["brightness"]["type"] == "int"
    assert raw["fields"]["brightness"]["required"] is True


@pytest.mark.asyncio
async def test_make_frame_without_service_object_empty_fields():
    """When no service object is passed (backward compat), raw_action_data is {}."""
    hass = _make_hass_with_services({}, {})
    entry = _make_entry()
    metrics = _make_metrics()
    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)

    frame = pusher._make_frame("light", "light.kitchen", "turn_on")

    assert frame["payload"]["raw_action_data"] == {}


# ---------------------------------------------------------------------------
# Scenario 28: full push passes service object to _make_frame
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_full_push_includes_service_schema():
    """Verify the full push pipeline passes HA Service objects through."""
    import voluptuous as vol

    mock_service = MagicMock()
    mock_service.schema = vol.Schema({
        vol.Optional("brightness"): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=255)
        ),
    })

    services = {
        "light": {"turn_on": mock_service},
    }
    entity_ids = {"light": ["light.kitchen"]}
    hass = _make_hass_with_services(services, entity_ids)
    entry = _make_entry()
    metrics = _make_metrics()

    session = MagicMock()
    session.post = MagicMock(return_value=_make_response(202))

    pusher = catalog_pusher.CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    pusher._session = session

    await pusher.push_catalog()

    # Capture the NDJSON body sent to session.post
    call_kwargs = session.post.call_args
    body = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
    lines = body.decode("utf-8").strip().split("\n")
    frame = json.loads(lines[0])
    raw = frame["payload"]["raw_action_data"]
    assert "fields" in raw
    assert "brightness" in raw["fields"]


# ---------------------------------------------------------------------------
# Scenario 29: non-serializable In container is handled safely
# ---------------------------------------------------------------------------


def test_extract_fields_non_serializable_in_container():
    """vol.In with non-JSON-serializable container values is handled safely."""
    import voluptuous as vol
    import uuid

    # In with UUID objects (non-JSON-serializable) — must not crash
    bad_in = vol.In([uuid.uuid4(), uuid.uuid4()])
    schema = vol.Schema({
        vol.Optional('device_id'): bad_in,
    })

    fields = catalog_pusher.CatalogPusher._extract_fields_from_schema(schema)

    # Should return a dict (possibly with options as strings, possibly without)
    # but MUST be JSON-serializable.
    body = json.dumps(fields, sort_keys=True)
    assert "device_id" in body or fields == {}


def test_extract_fields_safe_json_filter():
    """_safe_json_fields strips non-serializable values and stringifies fallbacks."""
    import uuid

    raw_fields = {
        "color": {
            "type": "str",
            "options": [uuid.uuid4(), uuid.uuid4()],  # non-serializable
            "required": False,
        },
        "brightness": {
            "type": "int",
            "required": True,
            "min": 0,
        },
    }

    safe = catalog_pusher.CatalogPusher._safe_json_fields(raw_fields)

    # brightness field is preserved intact
    assert safe["brightness"] == {"type": "int", "required": True, "min": 0}
    # color.options should be stringified (UUID → str)
    assert isinstance(safe["color"]["options"], list)
    # Final output must be JSON-serializable
    json.dumps(safe, sort_keys=True)
