"""Sensor platform for Agentic Home integration runtime metrics.

Ten AgenticHomeSensorEntity instances surface RuntimeMetrics data to Home Assistant
users without polling. Each sensor reads from metrics.snapshot() on every state update.
"""

from __future__ import annotations

import time
from datetime import datetime, UTC

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import EntityCategory
from homeassistant.components.sensor import SensorEntity

from .const import DOMAIN, STALENESS_CHECK_SECONDS
from .metrics import RuntimeMetrics
from .pusher import IngressHTTPPusher


# Sensor keys exposed by AgenticHomeSensorEntity.
# Kept at module level so subscriber.py can import them for self-exclusion.
SENSOR_KEYS = [
    "error_count",
    "frames_pushed",
    "frames_captured",
    "last_push_status",
    "last_push_time",
    "registry_push_count",
    "registry_error_count",
    "registry_last_push_time",
    "push_rate",
    "last_error",
]

# Human-readable names for each sensor entity.
# Used as _attr_name so entities have a meaningful name even if the
# translation_key doesn't resolve (e.g. missing translations/en.json).
_SENSOR_NAMES: dict[str, str] = {
    "error_count": "Error Count",
    "frames_pushed": "Frames Pushed",
    "frames_captured": "Frames Captured",
    "last_push_status": "Last Push Status",
    "last_push_time": "Last Push Time",
    "registry_push_count": "Registry Push Count",
    "registry_error_count": "Registry Error Count",
    "registry_last_push_time": "Registry Last Push Time",
    "push_rate": "Push Rate",
    "last_error": "Last Error",
}


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> None:
    """Set up Agentic Home sensor entities for a config entry.

    Creates one AgenticHomeSensorEntity per sensor key in SENSOR_KEYS.
    All entities are non-polling and update reactively via metrics._on_update.
    """
    entry_data = hass.data[DOMAIN][entry.entry_id]
    metrics: RuntimeMetrics = entry_data["metrics"]

    integration_id: str = entry.data.get("integration_id", "")

    entities = [
        AgenticHomeSensorEntity(
            hass=hass,
            entry=entry,
            metrics=metrics,
            sensor_key=key,
            integration_id=integration_id,
        )
        for key in SENSOR_KEYS
    ]

    # Wire metrics._on_update to trigger schedule_update on every entity.
    @callback
    def _on_metrics_update() -> None:
        for entity in entities:
            entity.schedule_update()

    metrics._on_update = _on_metrics_update

    async_add_entities(entities)


class AgenticHomeSensorEntity(SensorEntity):
    """Non-polling sensor entity that reads RuntimeMetrics state reactively.

    Every state mutation on the shared RuntimeMetrics fires _on_update, which
    calls schedule_update() on each entity. native_value reads from the
    in-memory snapshot with no I/O.
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_should_poll = False

    _last_write_time: float | None = None
    _pending_write_handle: HomeAssistant.loop | None = None

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        metrics: RuntimeMetrics,
        sensor_key: str,
        integration_id: str,
    ) -> None:
        self.hass = hass
        self._entry = entry
        self._metrics = metrics
        self._sensor_key = sensor_key

        self._attr_unique_id = f"{entry.entry_id}_{sensor_key}"
        self._attr_translation_key = f"metric_{sensor_key}"
        self._attr_name = _SENSOR_NAMES.get(sensor_key, sensor_key)

        self._attr_device_info = {
            "identifiers": {(DOMAIN, integration_id)},
        }

    @property
    def native_value(self):
        """Return the current metric value from the in-memory snapshot.

        Timestamp fields (last_push_time, registry_last_push_time) convert the
        Unix float to a UTC datetime. A value of 0.0 means "never occurred" and
        returns None so HA displays an unavailable-like state.
        """
        snap = self._metrics.snapshot()
        value = snap[self._sensor_key]

        # Timestamp sensors: convert Unix float → datetime(UTC), None if unset.
        if self._sensor_key in ("last_push_time", "registry_last_push_time"):
            if value == 0.0:
                return None
            return datetime.fromtimestamp(value, tz=UTC)

        return value

    def schedule_update(self) -> None:
        """Debounced write: skip if a write occurred within the last 1 second.

        Schedules a deferred write for the remaining interval to avoid flooding
        HA's state persistence layer during burst metric updates.
        """
        now = time.monotonic()

        # Cancel any pending deferred write.
        if self._pending_write_handle is not None:
            self._pending_write_handle.cancel()
            self._pending_write_handle = None

        if self._last_write_time is not None:
            elapsed = now - self._last_write_time
            if elapsed < 1.0:
                # Defer the write.
                remaining = 1.0 - elapsed
                self._pending_write_handle = self.hass.loop.call_later(
                    remaining,
                    self._write_state,
                )
                return

        self._write_state()

    def _write_state(self) -> None:
        """Write the current state to Home Assistant."""
        self._pending_write_handle = None
        self._last_write_time = time.monotonic()
        self.async_write_ha_state()