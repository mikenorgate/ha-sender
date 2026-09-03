"""Tests for custom_components.agentic_home.frame module."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from custom_components.agentic_home.frame import (
    DELIVERY_MODE_HEARTBEAT,
    DELIVERY_MODE_LIVE,
    SequenceCounter,
    _extract_upstream_context,
    build_frame,
    build_heartbeat,
    build_non_state_frame,
    build_state_frame,
)


# ---------------------------------------------------------------------------
# Mock Home Assistant event helpers
# ---------------------------------------------------------------------------

@dataclass
class MockState:
    """Minimal HA State object for tests."""
    state: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class MockEventData:
    """Minimal HA event data dict for tests."""
    _data: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default=None):
        return self._data.get(key, default)


@dataclass
class MockContext:
    """Minimal HA Context object for testing."""
    id: str = ""
    parent_id: str | None = None
    user_id: str | None = None


@dataclass
class MockEvent:
    """Minimal Home Assistant Event for testing."""
    event_type: str = "state_changed"
    time_fired: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: dict[str, Any] = field(default_factory=dict)
    event_id: str | None = None
    context: MockContext | None = None


def make_state_event(
    entity_id: str,
    new_state: str,
    old_state: str = "",
    new_attrs: dict[str, Any] | None = None,
    old_attrs: dict[str, Any] | None = None,
    time_fired: datetime | None = None,
    event_id: str | None = None,
) -> MockEvent:
    """Factory: build a state_changed MockEvent."""
    new = MockState(state=new_state, attributes=new_attrs or {})
    old = MockState(state=old_state, attributes=old_attrs or {}) if old_state or old_attrs else None
    return MockEvent(
        event_type="state_changed",
        time_fired=time_fired or datetime.now(timezone.utc),
        data={
            "entity_id": entity_id,
            "new_state": new,
            "old_state": old,
        },
        event_id=event_id,
    )


def make_non_state_event(
    event_type: str,
    entity_id: str = "",
    domain: str = "",
    service: str = "",
    service_data: dict[str, Any] | None = None,
    time_fired: datetime | None = None,
) -> MockEvent:
    """Factory: build a non-state_changed MockEvent."""
    data: dict[str, Any] = {}
    if entity_id:
        data["entity_id"] = entity_id
    if domain:
        data["domain"] = domain
    if service:
        data["service"] = service
    if service_data:
        data["service_data"] = service_data
    return MockEvent(
        event_type=event_type,
        time_fired=time_fired or datetime.now(timezone.utc),
        data=data,
    )


# ---------------------------------------------------------------------------
# SequenceCounter tests
# ---------------------------------------------------------------------------

class TestSequenceCounter:
    def test_starts_near_current_time_millis(self):
        """Sequence counter starts near the current Unix millisecond timestamp."""
        before = int(time.time() * 1000)
        counter = SequenceCounter()
        after = int(time.time() * 1000)
        assert before <= counter._value <= after

    def test_next_returns_incrementing_values(self):
        """Each call to next() returns a monotonically increasing integer."""
        counter = SequenceCounter()
        first = counter.next()
        second = counter.next()
        third = counter.next()
        assert second == first + 1
        assert third == second + 1

    def test_next_returns_int(self):
        """next() returns a native int, not a float."""
        counter = SequenceCounter()
        val = counter.next()
        assert isinstance(val, int)


# ---------------------------------------------------------------------------
# build_frame dispatch tests
# ---------------------------------------------------------------------------

class TestBuildFrameDispatch:
    def test_dispatches_state_changed(self):
        """build_frame dispatches to build_state_frame for state_changed."""
        event = make_state_event("light.kitchen", "on", "off")
        counter = SequenceCounter()
        result = build_frame(event, counter)
        assert result is not None
        assert result["event_type"] == "state_changed"
        assert result["payload"]["entity_id"] == "light.kitchen"

    def test_dispatches_non_state_to_non_state_frame(self):
        """build_frame dispatches non-state_changed to build_non_state_frame."""
        event = make_non_state_event("call_service", entity_id="light.kitchen")
        counter = SequenceCounter()
        result = build_frame(event, counter)
        assert result is not None
        assert result["event_type"] == "call_service"
        assert result["payload"]["entity_id"] == "light.kitchen"

    def test_state_changed_missing_new_state_returns_none(self):
        """state_changed with no new_state returns None (logged warning)."""
        event = MockEvent(
            event_type="state_changed",
            data={"entity_id": "light.kitchen"},  # no new_state key
        )
        counter = SequenceCounter()
        result = build_frame(event, counter)
        assert result is None

    def test_state_changed_missing_entity_id_returns_none(self):
        """state_changed with no entity_id returns None (logged warning)."""
        event = MockEvent(
            event_type="state_changed",
            data={"new_state": MockState(state="on")},
        )
        counter = SequenceCounter()
        result = build_frame(event, counter)
        assert result is None


# ---------------------------------------------------------------------------
# build_state_frame tests
# ---------------------------------------------------------------------------

class TestBuildStateFrame:
    def test_basic_frame_structure(self):
        """Frame has all required snake_case fields."""
        event = make_state_event("climate.living_room", "22", "21")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result is not None
        assert "source_event_id" in result
        assert "source_sequence" in result
        assert "delivery_mode" in result
        assert "event_time" in result
        assert "event_type" in result
        assert "payload" in result

    def test_delivery_mode_is_live(self):
        """delivery_mode is set to DELIVERY_MODE_LIVE."""
        event = make_state_event("switch.bedroom", "on", "off")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["delivery_mode"] == DELIVERY_MODE_LIVE

    def test_event_type_is_state_changed(self):
        """event_type is always 'state_changed' for state frames."""
        event = make_state_event("sensor.temp", "25.5", "24.0")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["event_type"] == "state_changed"

    def test_source_event_id_is_valid_uuid(self):
        """source_event_id is a valid hex UUID (32 hex chars)."""
        import uuid as uuid_module
        event = make_state_event("binary_sensor.door", "on", "off")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert len(result["source_event_id"]) == 32
        uuid_module.UUID(result["source_event_id"])

    def test_source_sequence_increments(self):
        """source_sequence increments across multiple frames."""
        counter = SequenceCounter()
        event1 = make_state_event("light.a", "on", "off")
        event2 = make_state_event("light.b", "off", "on")
        frame1 = build_state_frame(event1, counter)
        frame2 = build_state_frame(event2, counter)
        assert frame2["source_sequence"] == frame1["source_sequence"] + 1

    def test_payload_entity_id(self):
        """payload.entity_id matches the event data."""
        event = make_state_event("switch.garage", "on", "off")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["payload"]["entity_id"] == "switch.garage"

    def test_payload_new_state(self):
        """payload.new_state contains state and attributes."""
        event = make_state_event("light.dining", "on", "off", new_attrs={"brightness": 128})
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["payload"]["new_state"]["state"] == "on"
        assert result["payload"]["new_state"]["attributes"] == {"brightness": 128}

    def test_payload_old_state(self):
        """payload.old_state contains state and attributes."""
        event = make_state_event("light.dining", "on", "off", old_attrs={"brightness": 64})
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["payload"]["old_state"]["state"] == "off"
        assert result["payload"]["old_state"]["attributes"] == {"brightness": 64}

    def test_payload_old_state_empty_when_none(self):
        """payload.old_state is empty when old_state is None."""
        event = make_state_event("light.new", "on", "")
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["payload"]["old_state"]["state"] == ""
        assert result["payload"]["old_state"]["attributes"] == {}

    def test_payload_domain_extracted(self):
        """payload.domain is extracted from entity_id before the dot."""
        for entity_id, expected_domain in [
            ("light.kitchen", "light"),
            ("climate.hallway", "climate"),
            ("binary_sensor.front_door", "binary_sensor"),
            ("lock.front_door", "lock"),
        ]:
            event = make_state_event(entity_id, "on", "off")
            counter = SequenceCounter()
            result = build_state_frame(event, counter)
            assert result["payload"]["domain"] == expected_domain, f"entity_id={entity_id}"

    def test_event_time_isoformat(self):
        """event_time is an ISO-8601 string."""
        ts = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        event = make_state_event("sensor.temp", "21", "20", time_fired=ts)
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        # isoformat produces a string; Go can parse it with RFC3339Nano/RFC3339.
        assert isinstance(result["event_time"], str)
        assert "2024-06-15T10:30:00" in result["event_time"]

    def test_naive_datetime_gets_utc(self):
        """Naive datetime is converted to UTC before isoformat."""
        naive_ts = datetime(2024, 6, 15, 10, 30, 0)  # no tzinfo
        event = make_state_event("switch.test", "on", "off", time_fired=naive_ts)
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        # isoformat() of UTC-adjusted naive datetime includes +00:00.
        assert "+00:00" in result["event_time"]

    def test_none_attributes_defaults_empty_dict(self):
        """State with None attributes produces empty {} in the frame."""
        state = MockState(state="on", attributes=None)
        event = MockEvent(
            event_type="state_changed",
            data={
                "entity_id": "light.test",
                "new_state": state,
            },
        )
        counter = SequenceCounter()
        result = build_state_frame(event, counter)
        assert result["payload"]["new_state"]["attributes"] == {}


# ---------------------------------------------------------------------------
# build_non_state_frame tests
# ---------------------------------------------------------------------------

class TestBuildNonStateFrame:
    def test_basic_frame_structure(self):
        """Frame has all required snake_case fields."""
        event = make_non_state_event("call_service", entity_id="light.kitchen")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result is not None
        assert "source_event_id" in result
        assert "source_sequence" in result
        assert "delivery_mode" in result
        assert "event_time" in result
        assert "event_type" in result
        assert "payload" in result

    def test_event_type_preserved(self):
        """event_type is preserved as-is (not forced to 'state_changed')."""
        event = make_non_state_event("automation.triggered")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["event_type"] == "automation.triggered"

    def test_payload_entity_id_from_data(self):
        """payload.entity_id comes from event.data['entity_id']."""
        event = make_non_state_event("service_call", entity_id="climate.main")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["entity_id"] == "climate.main"

    def test_payload_entity_id_falls_back_to_domain(self):
        """entity_id falls back to event.data['domain'] when entity_id is absent."""
        event = make_non_state_event("service_call", domain="homeassistant")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["entity_id"] == "homeassistant"

    def test_payload_domain(self):
        """payload.domain is set from event.data['domain']."""
        event = make_non_state_event("event_test", entity_id="light.dining", domain="light")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["domain"] == "light"

    def test_payload_service_name(self):
        """payload.service_name is set from event.data['service']."""
        event = make_non_state_event("call_service", service="turn_on")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["service_name"] == "turn_on"

    def test_payload_raw_action_data(self):
        """payload.raw_action_data preserves event.data['service_data']."""
        event = make_non_state_event(
            "call_service",
            entity_id="light.kitchen",
            service="turn_on",
            service_data={"entity_id": "light.kitchen", "brightness": 200},
        )
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["raw_action_data"]["brightness"] == 200

    def test_raw_action_data_falls_back_to_full_data(self):
        """When service_data is absent, raw_action_data gets the full event.data."""
        event = make_non_state_event(
            "automation.triggered",
            entity_id="automation.goodnight",
            domain="automation",
        )
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        # Should contain at least the keys from the original data.
        assert "entity_id" in result["payload"]["raw_action_data"]

    def test_new_state_uses_service_name(self):
        """new_state.state uses service name when available."""
        event = make_non_state_event("call_service", service="turn_on")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["new_state"]["state"] == "turn_on"

    def test_new_state_uses_event_type_when_no_service(self):
        """new_state.state falls back to event_type when service is absent."""
        event = make_non_state_event("device_tracker.entered")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["new_state"]["state"] == "device_tracker.entered"

    def test_old_state_is_empty(self):
        """old_state.state is always empty for non-state frames."""
        event = make_non_state_event("call_service", service="turn_off")
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert result["payload"]["old_state"]["state"] == ""

    def test_source_sequence_increments(self):
        """source_sequence increments across multiple non-state frames."""
        counter = SequenceCounter()
        event1 = make_non_state_event("event_type_a")
        event2 = make_non_state_event("event_type_b")
        frame1 = build_non_state_frame(event1, counter)
        frame2 = build_non_state_frame(event2, counter)
        assert frame2["source_sequence"] == frame1["source_sequence"] + 1

    def test_event_time_isoformat(self):
        """event_time is an ISO-8601 string for non-state frames too."""
        ts = datetime(2024, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
        event = make_non_state_event("service_call", time_fired=ts)
        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)
        assert isinstance(result["event_time"], str)
        assert "2024-07-01T12:00:00" in result["event_time"]


# ---------------------------------------------------------------------------
# _extract_upstream_context tests
# ---------------------------------------------------------------------------


class TestExtractUpstreamContext:
    """Tests for _extract_upstream_context helper."""

    def test_context_with_user_id(self):
        """Manual interaction context has user_id populated."""
        ctx = MockContext(id="ctx-abc123", user_id="user_42", parent_id=None)
        event = make_state_event("light.living_room", "on", "off")
        event.context = ctx

        result = _extract_upstream_context(event)

        assert result["context_id"] == "ctx-abc123"
        assert result["user_id"] == "user_42"
        assert result["parent_id"] is None

    def test_context_system_automation_no_user_id(self):
        """Automation/system events have context_id but no user_id."""
        ctx = MockContext(id="ctx-auto-1", user_id=None, parent_id=None)
        event = make_state_event("sensor.temp", "22", "21")
        event.context = ctx

        result = _extract_upstream_context(event)

        assert result["context_id"] == "ctx-auto-1"
        assert result["user_id"] is None
        assert result["parent_id"] is None

    def test_context_with_parent_id(self):
        """Event chain context has parent_id set."""
        ctx = MockContext(
            id="ctx-child-1",
            user_id="user_99",
            parent_id="ctx-parent-0",
        )
        event = make_state_event("switch.hallway", "on", "off")
        event.context = ctx

        result = _extract_upstream_context(event)

        assert result["context_id"] == "ctx-child-1"
        assert result["user_id"] == "user_99"
        assert result["parent_id"] == "ctx-parent-0"

    def test_context_none_returns_empty_dict(self):
        """Event with no .context attribute returns empty dict."""
        event = make_state_event("light.kitchen", "on", "off")
        # context defaults to None in MockEvent
        result = _extract_upstream_context(event)
        assert result == {}

    def test_context_empty_id_returns_context_id_empty_string(self):
        """Empty context id produces empty string, not None."""
        ctx = MockContext(id="", user_id=None, parent_id=None)
        event = make_state_event("sensor.humidity", "50", "48")
        event.context = ctx

        result = _extract_upstream_context(event)

        assert result["context_id"] == ""
        assert result["user_id"] is None
        assert result["parent_id"] is None

    def test_upstream_context_present_in_state_frame_payload(self):
        """build_state_frame includes upstream_context in payload."""
        ctx = MockContext(id="ctx-123", user_id="user_a")
        event = make_state_event("climate.hallway", "21", "20")
        event.context = ctx

        counter = SequenceCounter()
        result = build_state_frame(event, counter)

        assert "upstream_context" in result["payload"]
        uc = result["payload"]["upstream_context"]
        assert uc["context_id"] == "ctx-123"
        assert uc["user_id"] == "user_a"

    def test_upstream_context_present_in_non_state_frame_payload(self):
        """build_non_state_frame includes upstream_context in payload."""
        ctx = MockContext(id="ctx-456", user_id="user_b")
        event = make_non_state_event("call_service", entity_id="light.dining")
        event.context = ctx

        counter = SequenceCounter()
        result = build_non_state_frame(event, counter)

        assert "upstream_context" in result["payload"]
        uc = result["payload"]["upstream_context"]
        assert uc["context_id"] == "ctx-456"
        assert uc["user_id"] == "user_b"

    def test_upstream_context_not_in_heartbeat(self):
        """Heartbeat frames do not include upstream_context (no event context)."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)

        assert "upstream_context" not in result["payload"]


# ---------------------------------------------------------------------------
# build_heartbeat tests
# ---------------------------------------------------------------------------

class TestBuildHeartbeat:
    def test_returns_valid_frame(self):
        """build_heartbeat returns a dict with all 6 required Frame fields."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert isinstance(result, dict)
        assert "source_event_id" in result
        assert "source_sequence" in result
        assert "delivery_mode" in result
        assert "event_time" in result
        assert "event_type" in result
        assert "payload" in result

    def test_all_field_types(self):
        """All fields have correct types (string, int, string, string, string, dict)."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert isinstance(result["source_event_id"], str)
        assert isinstance(result["source_sequence"], int)
        assert isinstance(result["delivery_mode"], str)
        assert isinstance(result["event_time"], str)
        assert isinstance(result["event_type"], str)
        assert isinstance(result["payload"], dict)

    def test_empty_source_event_id(self):
        """source_event_id is the empty string (not a UUID)."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["source_event_id"] == ""

    def test_delivery_mode_is_heartbeat(self):
        """delivery_mode is DELIVERY_MODE_HEARTBEAT."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["delivery_mode"] == DELIVERY_MODE_HEARTBEAT

    def test_delivery_mode_matches_ingress_constant(self):
        """delivery_mode value matches the ingress handler constant."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["delivery_mode"] == "heartbeat"

    def test_event_type_is_heartbeat(self):
        """event_type is the literal string 'heartbeat'."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["event_type"] == "heartbeat"

    def test_uses_sequence_counter(self):
        """source_sequence increments with each call."""
        counter = SequenceCounter()
        frame1 = build_heartbeat(counter)
        frame2 = build_heartbeat(counter)
        assert frame2["source_sequence"] == frame1["source_sequence"] + 1

    def test_event_time_is_valid_iso8601(self):
        """event_time is a valid ISO-8601/RFC3339 string."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        # Format matches what Go time.RFC3339Nano can parse (ends in Z or +-offset).
        event_time = result["event_time"]
        assert isinstance(event_time, str)
        assert "T" in event_time  # ISO-8601 separator
        assert ("Z" in event_time or "+" in event_time or event_time.count("-") >= 3)

    def test_event_time_is_current(self):
        """event_time is close to the current time (within 5 seconds)."""
        counter = SequenceCounter()
        before = datetime.now(timezone.utc)
        result = build_heartbeat(counter)
        after = datetime.now(timezone.utc)
        # Parse the UTC timestamp: "2024-01-01T12:00:00+00:00" or "...Z"
        ts_str = result["event_time"].replace("Z", "+00:00")
        from datetime import timedelta

        ts = datetime.fromisoformat(ts_str)
        assert before - timedelta(seconds=5) <= ts <= after + timedelta(seconds=5)

    def test_payload_is_empty_dict(self):
        """payload is {} not None (ingress validateStreamFrame rejects nil payload)."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["payload"] == {}
        assert result["payload"] is not None

    def test_payload_is_not_none(self):
        """payload is the empty dict, not the None singleton."""
        counter = SequenceCounter()
        result = build_heartbeat(counter)
        assert result["payload"] is not None

    def test_heartbeat_passes_ingress_validation(self):
        """Heartbeat frame passes the same validateStreamFrame contract as live frames.

        That contract (from services/ingress/internal/http/handler.go) requires:
        - delivery_mode in {"live", "replay", "catalog", "heartbeat"}
        - source_sequence is present
        - event_time is valid ISO-8601
        - event_type is non-empty
        - payload is non-nil
        - source_event_id may be empty for heartbeat delivery_mode
        """
        counter = SequenceCounter()
        result = build_heartbeat(counter)

        # delivery_mode check
        assert result["delivery_mode"] == "heartbeat"

        # source_sequence check
        assert "source_sequence" in result
        assert isinstance(result["source_sequence"], int)

        # event_time check — verify ISO-8601 structure (Go parses with RFC3339Nano/RFC3339)
        ts_str = result["event_time"].replace("Z", "+00:00")
        from datetime import datetime as dt

        ts = dt.fromisoformat(ts_str)
        assert ts.tzinfo is not None  # must be timezone-aware

        # event_type check
        assert result["event_type"] == "heartbeat"

        # payload check (nil would fail)
        assert result["payload"] == {}

        # source_event_id may be empty for heartbeat
        assert result["source_event_id"] == ""