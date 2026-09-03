"""Tests for custom_components.agentic_home.subscriber module."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from custom_components.agentic_home.const import CONF_EXCLUDE_ENTITIES, DOMAIN
from custom_components.agentic_home.frame import SequenceCounter
from custom_components.agentic_home.sensor import SENSOR_KEYS
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home import pusher as pusher_module
from custom_components.agentic_home.pusher import IngressHTTPPusher
from custom_components.agentic_home.subscriber import EventSubscriber


# ---------------------------------------------------------------------------
# Mock HA fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_hass():
    """Return a MagicMock hass with a real event loop."""
    hass = MagicMock()
    hass.loop = asyncio.new_event_loop()
    # Pre-populate hass.data with empty domain structure.
    hass.data = {DOMAIN: {}}
    yield hass
    hass.loop.close()


@pytest.fixture
def mock_entry():
    """Return a ConfigEntry-like MagicMock."""
    entry = MagicMock()
    entry.entry_id = "entry_test_sub_xyz"
    entry.data = {}
    entry.options = {}
    return entry


@pytest.fixture
def fake_pusher(fake_hass, mock_entry, metrics):
    """Return an IngressHTTPPusher with all deps mocked."""
    hass = fake_hass
    from custom_components.agentic_home.const import (
        BATCH_FLUSH_INTERVAL_MS,
        BATCH_MAX_FRAMES,
        CONF_INGRESS_URL,
        CONF_JWT_TOKEN,
        HTTP_TIMEOUT_SECONDS,
        INGRESS_STREAM_PATH,
    )
    entry = mock_entry
    entry.data = {
        CONF_INGRESS_URL: "https://ingress.example.com",
        CONF_JWT_TOKEN: "jwt_secret",
    }
    metrics = RuntimeMetrics()
    with patch.object(pusher_module, "async_get_clientsession", return_value=MagicMock()):
        return IngressHTTPPusher(hass, entry, metrics)


@pytest.fixture
def metrics():
    return RuntimeMetrics()


@pytest.fixture
def seq_counter():
    return SequenceCounter()


@pytest.fixture
def subscriber(fake_hass, mock_entry, fake_pusher, metrics, seq_counter):
    return EventSubscriber(fake_hass, mock_entry, fake_pusher, metrics, seq_counter)


# ---------------------------------------------------------------------------
# Mock event helpers
# ---------------------------------------------------------------------------


@dataclass
class MockState:
    state: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class MockEvent:
    event_type: str = "state_changed"
    time_fired: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: dict[str, Any] = field(default_factory=dict)
    event_id: str | None = None


def make_state_event(
    entity_id: str,
    new_state: str,
    old_state: str = "",
    new_attrs: dict[str, Any] | None = None,
    time_fired: datetime | None = None,
) -> MockEvent:
    new = MockState(state=new_state, attributes=new_attrs or {})
    old = MockState(state=old_state, attributes={}) if old_state else None
    return MockEvent(
        event_type="state_changed",
        time_fired=time_fired or datetime.now(timezone.utc),
        data={
            "entity_id": entity_id,
            "new_state": new,
            "old_state": old,
        },
    )


def make_non_state_event(
    event_type: str,
    entity_id: str = "",
    domain: str = "",
    service: str = "",
    time_fired: datetime | None = None,
) -> MockEvent:
    data: dict[str, Any] = {}
    if entity_id:
        data["entity_id"] = entity_id
    if domain:
        data["domain"] = domain
    if service:
        data["service"] = service
    return MockEvent(
        event_type=event_type,
        time_fired=time_fired or datetime.now(timezone.utc),
        data=data,
    )


# ---------------------------------------------------------------------------
# TestEventSubscription — start / stop / running property
# ---------------------------------------------------------------------------

class TestEventSubscription:
    def test_start_registers_wildcard_listener(self, subscriber, fake_hass):
        """start() calls hass.bus.async_listen('*', self._on_event)."""
        subscriber.start()
        fake_hass.bus.async_listen.assert_called_once()
        call_args = fake_hass.bus.async_listen.call_args
        assert call_args[0][0] == "*"
        assert call_args[0][1] == subscriber._on_event

    def test_start_sets_running_true(self, subscriber, fake_hass):
        """start() sets the running property to True."""
        assert subscriber.running is False
        subscriber.start()
        assert subscriber.running is True

    def test_start_idempotent(self, subscriber, fake_hass):
        """Multiple start() calls do not register multiple listeners."""
        subscriber.start()
        subscriber.start()
        fake_hass.bus.async_listen.assert_called_once()

    def test_start_loads_excluded_entities_from_entry_options(self, subscriber, mock_entry):
        """start() reads excluded_entities from entry.options and appends self-entity IDs."""
        mock_entry.options = {
            CONF_EXCLUDE_ENTITIES: ["sensor.private", "climate.sensitive"],
        }
        subscriber.start()
        expected = ["sensor.private", "climate.sensitive"]
        expected.extend(f"sensor.agentic_home_{key}" for key in SENSOR_KEYS)
        expected.append("binary_sensor.agentic_home_connection")
        assert subscriber._excluded_entities == expected

    def test_start_allows_empty_excluded_entities(self, subscriber, mock_entry):
        """start() handles missing excluded_entities key and adds self-entity IDs."""
        mock_entry.options = {}
        subscriber.start()
        expected = [f"sensor.agentic_home_{key}" for key in SENSOR_KEYS] + [
            "binary_sensor.agentic_home_connection"
        ]
        assert subscriber._excluded_entities == expected

    def test_stop_unregisters_listener(self, subscriber, fake_hass):
        """stop() calls the stored unregister function."""
        subscriber.start()
        unsub = subscriber._unregister
        subscriber.stop()
        unsub.assert_called_once()

    def test_stop_clears_unregister_ref(self, subscriber, fake_hass):
        """stop() sets _unregister to None."""
        subscriber.start()
        subscriber.stop()
        assert subscriber._unregister is None

    def test_stop_sets_running_false(self, subscriber, fake_hass):
        """stop() sets the running property to False."""
        subscriber.start()
        subscriber.stop()
        assert subscriber.running is False

    def test_stop_idempotent(self, subscriber, fake_hass):
        """Multiple stop() calls are safe."""
        subscriber.start()
        subscriber.stop()
        subscriber.stop()  # must not raise

    def test_stop_without_start_is_noop(self, subscriber, fake_hass):
        """stop() before start() is a no-op (running already False)."""
        subscriber.stop()
        assert subscriber.running is False
        fake_hass.bus.async_listen.assert_not_called()


# ---------------------------------------------------------------------------
# TestEventTranslation — frames built and forwarded correctly
# ---------------------------------------------------------------------------

class TestEventTranslation:
    def test_state_changed_event_produces_frame(self, subscriber, fake_pusher, seq_counter):
        """state_changed events are translated and forwarded to the pusher."""
        subscriber.start()
        event = make_state_event("light.kitchen", "on", "off")
        subscriber._on_event(event)

        assert len(fake_pusher._batch) == 1
        frame = fake_pusher._batch[0]
        assert frame["event_type"] == "state_changed"
        assert frame["payload"]["entity_id"] == "light.kitchen"
        assert frame["payload"]["new_state"]["state"] == "on"
        assert frame["payload"]["old_state"]["state"] == "off"

    def test_non_state_event_produces_frame(self, subscriber, fake_pusher, seq_counter):
        """Non-state_changed events are forwarded to the pusher."""
        subscriber.start()
        event = make_non_state_event("automation.triggered", entity_id="automation.goodnight")
        subscriber._on_event(event)

        assert len(fake_pusher._batch) == 1
        frame = fake_pusher._batch[0]
        assert frame["event_type"] == "automation.triggered"
        assert frame["payload"]["entity_id"] == "automation.goodnight"

    def test_event_without_event_type_skipped(self, subscriber, fake_pusher):
        """Events with no event_type are skipped (not forwarded)."""
        subscriber.start()
        event = MockEvent(event_type="", data={})
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 0

    def test_frames_captured_incremented(self, subscriber, fake_pusher, mock_entry):
        """Each forwarded frame is added to the pusher batch (via add_frame)."""
        subscriber.start()
        mock_entry.options = {}
        subscriber._on_event(make_state_event("light.dining", "on", "off"))
        # The subscriber forwards to subscriber._pusher (== fake_pusher via fixture chain).
        assert len(fake_pusher._batch) == 1


# ---------------------------------------------------------------------------
# TestEntityExclusion — filter hook works
# ---------------------------------------------------------------------------

class TestEntityExclusion:
    def test_excluded_entity_skipped(self, subscriber, mock_entry, fake_pusher):
        """Events from excluded entities are not forwarded to the pusher."""
        mock_entry.options = {CONF_EXCLUDE_ENTITIES: ["sensor.private"]}
        subscriber.start()
        event = make_state_event("sensor.private", "42.0", "41.0")
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 0

    def test_non_excluded_entity_forwarded(self, subscriber, mock_entry, fake_pusher):
        """Events from non-excluded entities are forwarded."""
        mock_entry.options = {CONF_EXCLUDE_ENTITIES: ["sensor.private"]}
        subscriber.start()
        event = make_state_event("sensor.public", "100", "99")
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 1

    def test_empty_excluded_list_forwards_all(self, subscriber, mock_entry, fake_pusher):
        """When excluded_entities is empty, all entities are forwarded."""
        mock_entry.options = {CONF_EXCLUDE_ENTITIES: []}
        subscriber.start()
        event = make_state_event("light.anything", "on", "off")
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 1

    def test_multiple_excluded_entities(self, subscriber, mock_entry, fake_pusher):
        """Multiple excluded entities are all filtered out."""
        mock_entry.options = {
            CONF_EXCLUDE_ENTITIES: ["sensor.a", "sensor.b", "sensor.c"],
        }
        subscriber.start()
        for entity_id in ["sensor.a", "sensor.b", "light.ok", "sensor.c"]:
            event = make_state_event(entity_id, "on", "off")
            subscriber._on_event(event)
        # Only light.ok should be forwarded.
        assert len(fake_pusher._batch) == 1
        assert fake_pusher._batch[0]["payload"]["entity_id"] == "light.ok"

    def test_exclusion_uses_stored_list_not_live(self, subscriber, mock_entry, fake_pusher):
        """Exclusion list is snapshotted at start(); later changes to entry.options are not reflected."""
        mock_entry.options = {CONF_EXCLUDE_ENTITIES: []}
        subscriber.start()
        # Modify entry.options after start.
        mock_entry.options[CONF_EXCLUDE_ENTITIES] = ["sensor.blocked"]
        # Event should still be forwarded (exclusion was snapshotted at start).
        event = make_state_event("sensor.blocked", "1", "0")
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 1


# ---------------------------------------------------------------------------
# TestSubscriberLifecycle — clean start/stop
# ---------------------------------------------------------------------------

class TestSubscriberLifecycle:
    def test_running_false_before_start(self, subscriber):
        """running is False before start()."""
        assert subscriber.running is False

    def test_running_true_after_start(self, subscriber):
        """running is True after start()."""
        subscriber.start()
        assert subscriber.running is True

    def test_running_false_after_stop(self, subscriber):
        """running is False after stop()."""
        subscriber.start()
        subscriber.stop()
        assert subscriber.running is False

    def test_stop_without_start_leaves_running_false(self, subscriber):
        """stop() without prior start() keeps running False."""
        subscriber.stop()
        assert subscriber.running is False

    def test_excluded_entities_loaded_on_start(self, subscriber, mock_entry):
        """_excluded_entities is populated when start() is called, including self-entity IDs."""
        mock_entry.options = {CONF_EXCLUDE_ENTITIES: ["sensor.x"]}
        assert subscriber._excluded_entities == []  # not loaded yet
        subscriber.start()
        expected = ["sensor.x"]
        expected.extend(f"sensor.agentic_home_{key}" for key in SENSOR_KEYS)
        expected.append("binary_sensor.agentic_home_connection")
        assert subscriber._excluded_entities == expected

    def test_error_in_on_event_does_not_crash(self, subscriber, fake_pusher, mock_entry):
        """If build_frame returns None, _on_event handles it gracefully (metrics updated)."""
        mock_entry.options = {}
        subscriber.start()
        # state_changed with no new_state → build_frame returns None.
        event = MockEvent(
            event_type="state_changed",
            data={"entity_id": "light.broken"},  # no new_state
        )
        subscriber._on_event(event)  # must not raise
        assert len(fake_pusher._batch) == 0

    def test_non_state_event_type_empty_string_skipped(self, subscriber, fake_pusher):
        """Event with event_type '' is skipped."""
        subscriber.start()
        event = MockEvent(event_type="", data={})
        subscriber._on_event(event)
        assert len(fake_pusher._batch) == 0

    def test_missing_options_key_defaults_empty(self, subscriber, mock_entry, fake_pusher):
        """entry.options = {} (no CONF_EXCLUDE_ENTITIES key) results in no filtering."""
        mock_entry.options = {}
        subscriber.start()
        # All events should be forwarded.
        event1 = make_state_event("sensor.secret", "1", "0")
        event2 = make_state_event("light.living", "on", "off")
        subscriber._on_event(event1)
        subscriber._on_event(event2)
        assert len(fake_pusher._batch) == 2