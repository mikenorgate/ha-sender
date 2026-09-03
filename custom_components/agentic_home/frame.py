"""Frame builder — translates Home Assistant events into platform-ingress Frame dicts."""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field

from homeassistant.core import Event

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Delivery mode emitted by the HA integration for all frames.
DELIVERY_MODE_LIVE = "live"

# Heartbeat delivery mode — skips source_event_id validation in ingress.
# Heartbeats are sent when the event stream is quiet to prevent false gap detection.
DELIVERY_MODE_HEARTBEAT = "heartbeat"


@dataclass
class SequenceCounter:
    """Atomic-ish monotonic sequence counter.

    Starts at int(time.time() * 1000) so that restarts don't collide with
    sequences from a prior run. Thread-safe via GIL in CPython;
    async-safe because we never share the counter across coroutines within
    a single batch-accumulator context.
    """

    _value: int = field(default_factory=lambda: int(time.time() * 1000))

    def next(self) -> int:
        """Return the next sequence value (increments atomically)."""
        current = self._value
        self._value = current + 1
        return current


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------


def build_frame(event: Event, seq_counter: SequenceCounter) -> dict | None:
    """Build a Frame dict from a Home Assistant event.

    Returns None and logs a WARNING when the event cannot be translated
    (e.g., new_state is missing on a state_changed event).

    Frame dict matches the Go service.Frame struct with snake_case field names
    and types accepted by services/ingress/internal/service/ingest.go validateStreamFrame().
    """
    event_type = getattr(event, "event_type", None)

    if event_type == "state_changed":
        return build_state_frame(event, seq_counter)
    else:
        return build_non_state_frame(event, seq_counter)


def build_state_frame(event: Event, seq_counter: SequenceCounter) -> dict | None:
    """Build a Frame dict for a state_changed event."""
    data = getattr(event, "data", {})

    entity_id = data.get("entity_id")
    if not entity_id:
        _LOGGER.warning("%s[%s] state_changed missing entity_id — skipping",
                        DOMAIN, getattr(event, "event_id", "?"))
        return None

    new_state = data.get("new_state")
    old_state = data.get("old_state")

    if new_state is None:
        _LOGGER.warning("%s[%s] state_changed with no new_state — skipping entity %s",
                        DOMAIN, getattr(event, "event_id", "?"), entity_id)
        return None

    # Extract state and attributes from State objects.
    new_state_val = getattr(new_state, "state", None) or ""
    new_attr_val = _state_attrs(new_state)

    old_state_val = getattr(old_state, "state", "") if old_state else ""
    old_attr_val = _state_attrs(old_state) if old_state else {}

    event_time = _format_event_time(event)
    upstream_context = _extract_upstream_context(event)

    return {
        "source_event_id": uuid.uuid4().hex,
        "source_sequence": seq_counter.next(),
        "delivery_mode": DELIVERY_MODE_LIVE,
        "event_time": event_time,
        "event_type": "state_changed",
        "payload": {
            "entity_id": entity_id,
            "new_state": {
                "state": str(new_state_val),
                "attributes": new_attr_val,
            },
            "old_state": {
                "state": str(old_state_val),
                "attributes": old_attr_val,
            },
            "domain": entity_id.split(".", 1)[0] if "." in entity_id else entity_id,
            "upstream_context": upstream_context,
        },
    }


def build_non_state_frame(event: Event, seq_counter: SequenceCounter) -> dict | None:
    """Build a Frame dict for any event that is not state_changed.

    entity_id is taken from event.data['entity_id'] or falling back to
    event.data['domain']. service_name comes from event.data['service'].
    raw_action_data is the full event.data for maximum fidelity.
    """
    data = getattr(event, "data", {})
    event_type = getattr(event, "event_type", "") or ""

    # entity_id: prefer explicit entity_id, fall back to domain.
    entity_id = data.get("entity_id") or data.get("domain") or ""
    domain = data.get("domain") or ""
    service_name = data.get("service") or ""

    # raw_action_data: preserve the full data payload for service calls, etc.
    raw_action_data = data.get("service_data", dict(data))

    # new_state.state: show the service name or event_type as a proxy state.
    new_state_state = data.get("service", event_type) or event_type

    event_time = _format_event_time(event)
    upstream_context = _extract_upstream_context(event)

    return {
        "source_event_id": uuid.uuid4().hex,
        "source_sequence": seq_counter.next(),
        "delivery_mode": DELIVERY_MODE_LIVE,
        "event_time": event_time,
        "event_type": event_type,
        "payload": {
            "entity_id": entity_id,
            "domain": domain,
            "service_name": service_name,
            "raw_action_data": raw_action_data,
            "new_state": {
                "state": new_state_state,
            },
            "old_state": {
                "state": "",
            },
            "upstream_context": upstream_context,
        },
    }


# ---------------------------------------------------------------------------
# Heartbeat frame builder
# ---------------------------------------------------------------------------


def build_heartbeat(seq_counter: SequenceCounter) -> dict:
    """Build a heartbeat Frame dict.

    Heartbeats are sent periodically when the event stream is quiet to prevent
    false gap detection in ingress (15s heartbeat timeout).  They use
    DELIVERY_MODE_HEARTBEAT so the Go side skips the source_event_id check
    (heartbeats have no source event to reference).  payload is always {}
    (not None) because ingress validateStreamFrame requires a non-nil payload.
    """
    from datetime import datetime, timezone as tz

    return {
        "source_event_id": "",  # Empty: ingress skips this check for heartbeat delivery_mode.
        "source_sequence": seq_counter.next(),
        "delivery_mode": DELIVERY_MODE_HEARTBEAT,
        "event_time": datetime.now(tz.utc).isoformat(),
        "event_type": "heartbeat",
        "payload": {},          # Non-nil: ingress validateStreamFrame rejects nil payload.
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _state_attrs(state_obj: object) -> dict:
    """Safely extract attributes dict from a HA State object."""
    if state_obj is None:
        return {}
    attrs = getattr(state_obj, "attributes", None)
    if attrs is None:
        return {}
    return dict(attrs)


def _extract_upstream_context(event: Event) -> dict:
    """Extract HA event.context (id, parent_id, user_id) into a dict.

    Home Assistant's Event.context is a Context object with:
      - id: str — unique context ID
      - parent_id: str | None — parent context ID (for event chains)
      - user_id: str | None — user who triggered the event (None for automation/system)

    Returns a dict with context_id, user_id, and parent_id keys.
    user_id and parent_id are None when absent.
    """
    ctx = getattr(event, "context", None)
    if ctx is None:
        return {}

    result: dict = {
        "context_id": getattr(ctx, "id", "") or "",
    }

    user_id = getattr(ctx, "user_id", None)
    result["user_id"] = user_id if user_id else None

    parent_id = getattr(ctx, "parent_id", None)
    result["parent_id"] = parent_id if parent_id else None

    return result


def _format_event_time(event: Event) -> str:
    """Return event.time_fired as an ISO-8601/RFC3339 string.

    If the datetime is naive (no tz info), attach UTC.
    This guarantees the Go side can parse it with time.RFC3339 or time.RFC3339Nano.
    """
    time_fired = getattr(event, "time_fired", None)
    if time_fired is None:
        return ""

    # If tz-aware, return isoformat directly (includes tz offset → Go parses with RFC3339Nano).
    # If naive, attach UTC so isoformat emits "+00:00".
    if time_fired.tzinfo is None:
        from datetime import timezone
        time_fired = time_fired.replace(tzinfo=timezone.utc)

    return time_fired.isoformat()