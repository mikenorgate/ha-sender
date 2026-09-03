"""Tests for custom_components.agentic_home.sensor platform."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from homeassistant.helpers.entity import EntityCategory

from custom_components.agentic_home import pusher as pusher_module
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home.pusher import IngressHTTPPusher
from custom_components.agentic_home.sensor import AgenticHomeSensorEntity, SENSOR_KEYS, async_setup_entry


# ---------------------------------------------------------------------------
# Mock async_write_ha_state — SensorEntity is instantiated outside
# HA's EntityComponent so self.entity_id is None and async_write_ha_state()
# raises NoEntitySpecifiedError. We replace it with a plain sync no-op.
# ---------------------------------------------------------------------------

def _noop_write_ha_state(self) -> None:
    pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def metrics() -> RuntimeMetrics:
    return RuntimeMetrics()


@pytest.fixture
def mock_hass():
    """Minimal hass mock with event loop for async callbacks."""
    hass = MagicMock()
    hass.loop = asyncio.new_event_loop()
    hass.data = {}
    # HA entity constructor may access hass.data["integrations"].
    hass.data["integrations"] = {}
    hass.data["agentic_home"] = {}
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config_entries.async_unload_platforms = AsyncMock()
    yield hass
    # Cancel any pending handles before closing the loop.
    hass.loop.close()


@pytest.fixture(autouse=True)
def _suppress_ha_entity_validation():
    """Suppress HA's async_write_ha_state validation during every test.

    AgenticHomeSensorEntity is instantiated outside HA's EntityComponent, so
    self.entity_id is None. async_write_ha_state() raises NoEntitySpecifiedError
    without a valid entity_id. The patch replaces the method with a plain
    no-op so entity construction and schedule_update() calls are safe.
    """
    with patch(
        "homeassistant.helpers.entity.Entity.async_write_ha_state",
        _noop_write_ha_state,
    ):
        yield


@pytest.fixture
def mock_entry():
    entry = MagicMock()
    entry.entry_id = "sensor_test_entry_abc"
    entry.data = {
        "ingress_url": "https://ingress.example.com",
        "jwt_token": "jwt_test",
        "integration_id": "integration-test-001",
    }
    entry.options = {}
    return entry


@pytest.fixture
def mock_pusher(mock_hass, mock_entry, metrics):
    """IngressHTTPPusher with aiohttp session stubbed."""
    mock_session = MagicMock()
    with patch.object(pusher_module, "async_get_clientsession", return_value=mock_session):
        return IngressHTTPPusher(mock_hass, mock_entry, metrics)


@pytest.fixture
def sensor_entity(mock_hass, mock_entry, metrics, mock_pusher):
    """Return the first sensor entity (error_count) as a usable fixture."""
    entity = AgenticHomeSensorEntity(
        hass=mock_hass,
        entry=mock_entry,
        metrics=metrics,
        sensor_key="error_count",
        integration_id=mock_entry.data["integration_id"],
    )
    return entity


# ---------------------------------------------------------------------------
# Test native_value reads from metrics snapshot
# ---------------------------------------------------------------------------

class TestNativeValueFromMetrics:
    """Each sensor's native_value reflects the in-memory metrics snapshot."""

    # --- error_count ---

    def test_error_count_initial_returns_0(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="iid-001",
        )
        assert entity.native_value == 0

    def test_error_count_after_increment(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.increment_error()
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="iid-001",
        )
        assert entity.native_value == 1

    # --- frames_pushed ---

    def test_frames_pushed_initial(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="frames_pushed", integration_id="iid-001",
        )
        assert entity.native_value == 0

    def test_frames_pushed_after_record(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_push(count=7, status=202, timestamp=1000.0)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="frames_pushed", integration_id="iid-001",
        )
        assert entity.native_value == 7

    # --- frames_captured ---

    def test_frames_captured_initial(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="frames_captured", integration_id="iid-001",
        )
        assert entity.native_value == 0

    def test_frames_captured_after_increment(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.increment_captured(3)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="frames_captured", integration_id="iid-001",
        )
        assert entity.native_value == 3

    # --- last_push_status ---

    def test_last_push_status_initial_none(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_push_status", integration_id="iid-001",
        )
        assert entity.native_value is None

    def test_last_push_status_after_push(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_push(count=1, status=202, timestamp=1000.0)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_push_status", integration_id="iid-001",
        )
        assert entity.native_value == 202

    # --- last_push_time ---

    def test_last_push_time_initial_none(self, mock_hass, mock_entry, metrics, mock_pusher):
        """0.0 maps to None so HA shows unavailable instead of epoch 0."""
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_push_time", integration_id="iid-001",
        )
        assert entity.native_value is None

    def test_last_push_time_converts_unix_to_datetime(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_push(count=1, status=200, timestamp=1700000000.0)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_push_time", integration_id="iid-001",
        )
        assert isinstance(entity.native_value, datetime)
        assert entity.native_value == datetime.fromtimestamp(1700000000.0, tz=UTC)

    # --- registry_push_count ---

    def test_registry_push_count_initial(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_push_count", integration_id="iid-001",
        )
        assert entity.native_value == 0

    def test_registry_push_count_after_push(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_registry_push(status=200, timestamp=1000.0)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_push_count", integration_id="iid-001",
        )
        assert entity.native_value == 1

    # --- registry_error_count ---

    def test_registry_error_count_initial(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_error_count", integration_id="iid-001",
        )
        assert entity.native_value == 0

    def test_registry_error_count_after_increment(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.increment_registry_error()
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_error_count", integration_id="iid-001",
        )
        assert entity.native_value == 1

    # --- registry_last_push_time ---

    def test_registry_last_push_time_initial_none(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_last_push_time", integration_id="iid-001",
        )
        assert entity.native_value is None

    def test_registry_last_push_time_converts(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_registry_push(status=200, timestamp=1800000000.0)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="registry_last_push_time", integration_id="iid-001",
        )
        assert isinstance(entity.native_value, datetime)
        assert entity.native_value == datetime.fromtimestamp(1800000000.0, tz=UTC)

    # --- push_rate ---

    def test_push_rate_initial_zero(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="push_rate", integration_id="iid-001",
        )
        assert entity.native_value == 0.0

    def test_push_rate_after_pushes(self, mock_hass, mock_entry, metrics, mock_pusher):
        """push_rate needs ≥2 timestamps within 60s window to be non-zero."""
        now = time.time()
        metrics.record_push(count=1, status=200, timestamp=now)
        metrics.record_push(count=1, status=200, timestamp=now + 1)
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="push_rate", integration_id="iid-001",
        )
        assert entity.native_value > 0.0

    # --- last_error ---

    def test_last_error_initial_empty(self, mock_hass, mock_entry, metrics, mock_pusher):
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_error", integration_id="iid-001",
        )
        assert entity.native_value == ""

    def test_last_error_after_stream_error(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_stream_error("connection refused")
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_error", integration_id="iid-001",
        )
        assert entity.native_value == "stream: connection refused"

    def test_last_error_after_registry_error(self, mock_hass, mock_entry, metrics, mock_pusher):
        metrics.record_registry_error_msg("timeout reaching registry endpoint")
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="last_error", integration_id="iid-001",
        )
        assert entity.native_value == "registry: timeout reaching registry endpoint"


# ---------------------------------------------------------------------------
# Test entity attributes
# ---------------------------------------------------------------------------

class TestEntityAttributes:
    """Verify entity-level attributes on AgenticHomeSensorEntity."""

    def test_entity_category_is_diagnostic(self, sensor_entity):
        """All sensors have entity_category = DIAGNOSTIC."""
        assert sensor_entity.entity_category == EntityCategory.DIAGNOSTIC

    def test_has_entity_name_true(self, sensor_entity):
        """has_entity_name is True so HA uses entity name from translation files."""
        assert sensor_entity.has_entity_name is True

    def test_unique_id_format(self, mock_hass, mock_entry, metrics, mock_pusher):
        """unique_id is {entry_id}_{sensor_key}."""
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="frames_pushed", integration_id="iid-001",
        )
        assert entity.unique_id == f"{mock_entry.entry_id}_frames_pushed"

    def test_device_info_linked(self, mock_hass, mock_entry, metrics, mock_pusher):
        """device_info identifiers match the Agentic Home device registry entry."""
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="integration-test-001",
        )
        assert (("agentic_home", "integration-test-001")) in entity.device_info["identifiers"]

    def test_should_poll_false(self, sensor_entity):
        """Sensors are reactive (not polling)."""
        assert sensor_entity.should_poll is False


# ---------------------------------------------------------------------------
# Test debounce logic
# ---------------------------------------------------------------------------

class TestDebounce:
    """Debounce skips writes within 1 second; deferred writes fire after the interval."""

    def test_debounce_skips_rapid_writes(self, mock_hass, mock_entry, metrics, mock_pusher):
        """A second schedule_update within 1s cancels the previous deferred write."""
        # Set call_later BEFORE entity construction — entity.__init__ calls
        # schedule_update() which may invoke call_later if _last_write_time is set.
        deferred_handle = MagicMock()
        immediate_handle = MagicMock()
        call_count = 0

        def call_later_side_effect(delay, callback):
            nonlocal call_count
            call_count += 1
            return deferred_handle if call_count == 1 else immediate_handle

        mock_hass.loop.call_later = MagicMock(side_effect=call_later_side_effect)

        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="iid-001",
        )
        # Simulate a recent write so schedule_update() enters the debounce path.
        entity._last_write_time = time.monotonic() - 0.2  # 200ms ago

        # First schedule_update: deferred (within 1s), creates deferred_handle.
        entity.schedule_update()
        handle1 = entity._pending_write_handle
        # Second schedule_update: cancels the deferred handle, schedules immediate.
        entity.schedule_update()
        handle2 = entity._pending_write_handle

        # The deferred handle must have been cancelled.
        assert deferred_handle.cancel.called is True
        # handle2 is the immediate write (immediate_handle).
        assert handle2 is immediate_handle

    def test_debounce_deferred_write_fires(self, mock_hass, mock_entry, metrics, mock_pusher):
        """_write_state calls async_write_ha_state directly (no call_later)."""
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="iid-001",
        )
        entity.async_write_ha_state = MagicMock()

        # invoke _write_state directly — it calls async_write_ha_state.
        entity._write_state()

        assert entity.async_write_ha_state.called is True
        assert entity._pending_write_handle is None

    def test_debounce_immediate_write_if_no_recent_write(self, mock_hass, mock_entry, metrics, mock_pusher):
        """If no write occurred in the last second, write_state fires immediately."""
        entity = AgenticHomeSensorEntity(
            hass=mock_hass, entry=mock_entry, metrics=metrics,
            sensor_key="error_count", integration_id="iid-001",
        )
        entity.async_write_ha_state = MagicMock()

        # First write — no previous, so immediate.
        entity.schedule_update()
        assert entity._pending_write_handle is None
        assert entity.async_write_ha_state.called is True


# ---------------------------------------------------------------------------
# Test async_setup_entry integration
# ---------------------------------------------------------------------------

class TestAsyncSetupEntry:
    """Verify async_setup_entry creates all 10 sensor entities."""

    @pytest.mark.asyncio
    async def test_async_setup_entry_creates_all_sensors(
        self, mock_hass, mock_entry, metrics, mock_pusher
    ):
        """All 10 sensor keys produce entities passed to async_add_entities."""
        mock_hass.data = {}

        added_entities: list[AgenticHomeSensorEntity] = []

        def capture_entities(entities):
            added_entities.extend(entities)

        # Pre-populate hass.data as async_setup_entry expects.
        mock_hass.data.setdefault("agentic_home", {})
        mock_hass.data["agentic_home"][mock_entry.entry_id] = {
            "metrics": metrics,
            "pusher": mock_pusher,
        }

        await async_setup_entry(mock_hass, mock_entry, capture_entities)

        assert len(added_entities) == len(SENSOR_KEYS) == 10

        # Verify each sensor key got an entity.
        observed_keys = {e._sensor_key for e in added_entities}
        assert observed_keys == set(SENSOR_KEYS)

    @pytest.mark.asyncio
    async def test_async_setup_entry_wires_metrics_callback(
        self, mock_hass, mock_entry, metrics, mock_pusher
    ):
        """After setup, metrics._on_update triggers schedule_update on all entities."""
        added_entities: list[AgenticHomeSensorEntity] = []
        mock_hass.data.setdefault("agentic_home", {})
        mock_hass.data["agentic_home"][mock_entry.entry_id] = {
            "metrics": metrics,
            "pusher": mock_pusher,
        }

        await async_setup_entry(mock_hass, mock_entry, added_entities.extend)

        # Trigger a metric mutation — this must fire _on_update.
        for entity in added_entities:
            entity.schedule_update = MagicMock()

        metrics._on_update()

        for entity in added_entities:
            entity.schedule_update.assert_called_once()