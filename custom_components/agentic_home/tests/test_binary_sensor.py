"""Tests for custom_components.agentic_home.binary_sensor platform."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from custom_components.agentic_home.binary_sensor import (
    AgenticHomeConnectionSensor,
    _CONNECTION_TIMEOUT_SECONDS,
    async_setup_entry,
)
from custom_components.agentic_home import pusher as pusher_module
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home.pusher import IngressHTTPPusher


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def metrics() -> RuntimeMetrics:
    return RuntimeMetrics()


@pytest.fixture
def mock_hass():
    """Minimal hass mock with a real event loop for call_later."""
    hass = MagicMock()
    hass.loop = asyncio.new_event_loop()
    hass.data = {}
    yield hass
    hass.loop.close()


@pytest.fixture
def mock_entry():
    entry = MagicMock()
    entry.entry_id = "bs_test_entry_xyz"
    entry.data = {
        "ingress_url": "https://ingress.example.com",
        "jwt_token": "jwt_test",
        "integration_id": "integration-test-bs-001",
    }
    entry.options = {}
    return entry


@pytest.fixture
def mock_pusher(mock_hass, mock_entry, metrics):
    """IngressHTTPPusher with _auth_failed defaulting to False."""
    mock_session = MagicMock()
    with patch.object(pusher_module, "async_get_clientsession", return_value=mock_session):
        p = IngressHTTPPusher(mock_hass, mock_entry, metrics)
    # Default: not auth-failed.
    p._auth_failed = False
    return p


@pytest.fixture
def connection_sensor(mock_hass, mock_entry, metrics, mock_pusher):
    """A connection sensor ready for use."""
    return AgenticHomeConnectionSensor(
        hass=mock_hass,
        entry=mock_entry,
        metrics=metrics,
        pusher=mock_pusher,
        integration_id=mock_entry.data["integration_id"],
    )


# ---------------------------------------------------------------------------
# Test connection state logic
# ---------------------------------------------------------------------------

class TestConnectionState:
    """is_on reflects auth state and push staleness."""

    def test_connection_initially_off(self, connection_sensor):
        """With no push ever, is_on returns None (unknown/unavailable)."""
        assert connection_sensor.is_on is None

    def test_connection_on_after_push(self, connection_sensor, metrics):
        """With a recent push (within 60s) and auth_ok, is_on is True."""
        now = time.time()
        metrics.record_push(count=1, status=200, timestamp=now)
        assert connection_sensor.is_on is True

    def test_connection_on_near_staleness_boundary(self, connection_sensor, metrics):
        """Just inside the 60s window, connection is still ON."""
        past = time.time() - (_CONNECTION_TIMEOUT_SECONDS - 1)
        metrics.record_push(count=1, status=200, timestamp=past)
        assert connection_sensor.is_on is True

    def test_connection_off_on_staleness(self, connection_sensor, metrics):
        """After 60s with no push, is_on is False."""
        stale = time.time() - (_CONNECTION_TIMEOUT_SECONDS + 5)
        metrics.record_push(count=1, status=200, timestamp=stale)
        assert connection_sensor.is_on is False

    def test_connection_off_on_auth_failure(self, connection_sensor, mock_pusher, metrics):
        """If _auth_failed is True, connection is OFF regardless of push recency."""
        now = time.time()
        metrics.record_push(count=1, status=200, timestamp=now)
        mock_pusher._auth_failed = True
        assert connection_sensor.is_on is False

    def test_connection_off_auth_failure_with_stale_push(self, connection_sensor, mock_pusher, metrics):
        """OFF even when push is stale AND auth has failed."""
        stale = time.time() - (_CONNECTION_TIMEOUT_SECONDS + 10)
        metrics.record_push(count=1, status=200, timestamp=stale)
        mock_pusher._auth_failed = True
        assert connection_sensor.is_on is False


# ---------------------------------------------------------------------------
# Test entity attributes
# ---------------------------------------------------------------------------

class TestEntityAttributes:
    """Entity-level attribute verification."""

    def test_device_class_connectivity(self, connection_sensor):
        """device_class is CONNECTIVITY so HA shows the right icon and colour."""
        from homeassistant.components.binary_sensor import BinarySensorDeviceClass
        assert connection_sensor.device_class == BinarySensorDeviceClass.CONNECTIVITY

    def test_no_entity_category(self, connection_sensor):
        """Connection sensor has no entity_category — shown in default state card."""
        # None means "config" or unset; connection sensors are visible by default.
        assert connection_sensor.entity_category is None

    def test_has_entity_name_true(self, connection_sensor):
        """has_entity_name is True."""
        assert connection_sensor.has_entity_name is True

    def test_unique_id_format(self, mock_hass, mock_entry, metrics, mock_pusher):
        """unique_id is {entry_id}_connection."""
        entity = AgenticHomeConnectionSensor(
            hass=mock_hass, entry=mock_entry,
            metrics=metrics, pusher=mock_pusher,
            integration_id="iid-001",
        )
        assert entity.unique_id == f"{mock_entry.entry_id}_connection"

    def test_should_poll_false(self, connection_sensor):
        """The sensor is reactive; no polling required."""
        assert connection_sensor.should_poll is False


# ---------------------------------------------------------------------------
# Test staleness timer
# ---------------------------------------------------------------------------

class TestStalenessTimer:
    """Periodic timer checks for staleness even when no push events fire."""

    def test_staleness_timer_schedules_on_add(self, connection_sensor, mock_hass):
        """async_added_to_hass schedules the first staleness check."""
        mock_hass.loop.call_later = MagicMock(return_value=MagicMock())

        # async_added_to_hass is async; run its sync part directly.
        connection_sensor.hass = mock_hass
        # Call the sync timer-start method directly.
        connection_sensor._start_staleness_timer()

        mock_hass.loop.call_later.assert_called()
        call_args = mock_hass.loop.call_later.call_args
        assert call_args[0][0] > 0  # delay is positive
        assert callable(call_args[0][1])  # callback is callable

    def test_staleness_timer_cancels_on_remove(self, connection_sensor, mock_hass):
        """async_will_remove_from_hass cancels the timer."""
        timer_handle = MagicMock()
        connection_sensor._staleness_timer_handle = timer_handle

        connection_sensor._cancel_staleness_timer()

        timer_handle.cancel.assert_called_once()
        assert connection_sensor._staleness_timer_handle is None

    def test_staleness_timer_no_crash_after_hass_none(self, mock_hass, mock_entry, metrics, mock_pusher):
        """_on_staleness_timer is safe when hass is None (entity removed mid-callback)."""
        sensor = AgenticHomeConnectionSensor(
            hass=mock_hass, entry=mock_entry,
            metrics=metrics, pusher=mock_pusher,
            integration_id="iid-001",
        )
        # Simulate post-removal: hass is None, timer still fires.
        sensor.hass = None
        sensor._on_staleness_timer()  # must not raise

    def test_staleness_timer_reschedules_itself(self, mock_hass, mock_entry, metrics, mock_pusher):
        """After writing state, _on_staleness_timer reschedules the next check."""
        sensor = AgenticHomeConnectionSensor(
            hass=mock_hass, entry=mock_entry,
            metrics=metrics, pusher=mock_pusher,
            integration_id="iid-001",
        )
        mock_hass.loop.call_later = MagicMock(return_value=MagicMock())
        sensor.async_write_ha_state = MagicMock()

        sensor._on_staleness_timer()

        sensor.async_write_ha_state.assert_called_once()
        # The timer reschedules itself with the same callback.
        assert mock_hass.loop.call_later.call_count == 1
        call_args = mock_hass.loop.call_later.call_args
        assert call_args[0][0] > 0  # delay is positive
        assert call_args[0][1] == sensor._on_staleness_timer  # same callback


# ---------------------------------------------------------------------------
# Test async_setup_entry integration
# ---------------------------------------------------------------------------

class TestAsyncSetupEntry:
    """Verify async_setup_entry wires the binary sensor correctly."""

    @pytest.mark.asyncio
    async def test_async_setup_entry_creates_connection_sensor(
        self, mock_hass, mock_entry, metrics, mock_pusher
    ):
        """async_setup_entry adds exactly one binary_sensor entity."""
        added_entities = []

        def capture(entities):
            added_entities.extend(entities)

        mock_hass.data.setdefault("agentic_home", {})
        mock_hass.data["agentic_home"][mock_entry.entry_id] = {
            "metrics": metrics,
            "pusher": mock_pusher,
        }

        await async_setup_entry(mock_hass, mock_entry, capture)

        assert len(added_entities) == 1
        entity = added_entities[0]
        assert isinstance(entity, AgenticHomeConnectionSensor)
        assert entity.unique_id == f"{mock_entry.entry_id}_connection"

    @pytest.mark.asyncio
    async def test_async_setup_entry_wires_metrics_callback(
        self, mock_hass, mock_entry, metrics, mock_pusher
    ):
        """After setup, metrics._on_update triggers async_write_ha_state on the entity."""
        added_entities = []
        mock_hass.data.setdefault("agentic_home", {})
        mock_hass.data["agentic_home"][mock_entry.entry_id] = {
            "metrics": metrics,
            "pusher": mock_pusher,
        }

        await async_setup_entry(mock_hass, mock_entry, added_entities.extend)

        entity = added_entities[0]
        # HA calls async_added_to_hass after adding the entity to the registry;
        # simulate that here so _on_metrics_update gets wired.
        await entity.async_added_to_hass()
        entity.async_write_ha_state = MagicMock()

        metrics._on_update()

        entity.async_write_ha_state.assert_called_once()