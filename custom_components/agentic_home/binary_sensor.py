"""Binary sensor platform for Agentic Home connection health.

AgenticHomeConnectionSensor exposes at-a-glance integration health:
- ON  = not auth-failed AND last push within 60 seconds
- OFF = auth-failed OR last push >60 seconds ago (staleness)

A periodic staleness timer (every 15s) ensures OFF transitions are detected
promptly even when no push events are firing.
"""

from __future__ import annotations

import time

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    BinarySensorDeviceClass,
)

from .const import DOMAIN, STALENESS_CHECK_SECONDS
from .metrics import RuntimeMetrics
from .pusher import IngressHTTPPusher


# Staleness threshold in seconds — connection goes OFF after this gap.
_CONNECTION_TIMEOUT_SECONDS = 60


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> None:
    """Set up the Agentic Home connection binary sensor for a config entry."""
    entry_data = hass.data[DOMAIN][entry.entry_id]
    metrics: RuntimeMetrics = entry_data["metrics"]
    pusher: IngressHTTPPusher = entry_data["pusher"]

    integration_id: str = entry.data.get("integration_id", "")

    async_add_entities([
        AgenticHomeConnectionSensor(
            hass=hass,
            entry=entry,
            metrics=metrics,
            pusher=pusher,
            integration_id=integration_id,
        )
    ])


class AgenticHomeConnectionSensor(BinarySensorEntity):
    """Binary sensor for Agentic Home integration connection health.

    State is ON when:
      - auth has not failed (pusher._auth_failed is False), AND
      - last push occurred within _CONNECTION_TIMEOUT_SECONDS seconds.

    State transitions to OFF promptly via a periodic staleness timer.
    """

    _attr_has_entity_name = True
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_should_poll = False

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        metrics: RuntimeMetrics,
        pusher: IngressHTTPPusher,
        integration_id: str,
    ) -> None:
        self.hass = hass
        self._entry = entry
        self._metrics = metrics
        self._pusher = pusher

        self._attr_name = "Connection"
        self._attr_unique_id = f"{entry.entry_id}_connection"
        self._attr_translation_key = "connection"

        self._attr_device_info = {
            "identifiers": {(DOMAIN, integration_id)},
        }

        self._staleness_timer_handle: HomeAssistant.loop | None = None

    @property
    def is_on(self) -> bool | None:
        """Return True (ON) when connected, False (OFF) when stale or auth-failed.

        Returns None when no push has ever occurred (last_push_time == 0.0),
        representing an unavailable/unknown state.
        """
        if self._pusher._auth_failed:
            return False

        snap = self._metrics.snapshot()
        last_push_time = snap["last_push_time"]

        if last_push_time == 0.0:
            # Never pushed — treat as unknown (None) so HA shows unavailable.
            return None

        age = time.time() - last_push_time
        return age <= _CONNECTION_TIMEOUT_SECONDS

    async def async_added_to_hass(self) -> None:
        """Start the staleness timer when the entity is added to HA."""
        await super().async_added_to_hass()
        self._start_staleness_timer()

        # Also react to metrics updates for immediate ON transitions.
        @callback
        def _on_metrics_update() -> None:
            if self.hass is None:
                return
            self.async_write_ha_state()

        self._metrics._on_update = _on_metrics_update

    async def async_will_remove_from_hass(self) -> None:
        """Cancel the staleness timer when the entity is removed."""
        self._cancel_staleness_timer()
        await super().async_will_remove_from_hass()

    def _start_staleness_timer(self) -> None:
        """Schedule periodic staleness checks."""
        self._staleness_timer_handle = self.hass.loop.call_later(
            STALENESS_CHECK_SECONDS,
            self._on_staleness_timer,
        )

    def _cancel_staleness_timer(self) -> None:
        """Cancel any pending staleness timer."""
        if self._staleness_timer_handle is not None:
            self._staleness_timer_handle.cancel()
            self._staleness_timer_handle = None

    def _on_staleness_timer(self) -> None:
        """Periodic check: write state if the connection status changed.

        This ensures that even without push events, HA transitions the binary
        sensor to OFF within STALENESS_CHECK_SECONDS after the 60s threshold
        is breached.
        """
        # Guard against entity removal while timer fires.
        if self.hass is None:
            return

        # Evaluate the current is_on state and write if changed.
        self.async_write_ha_state()

        # Reschedule the timer.
        self._staleness_timer_handle = self.hass.loop.call_later(
            STALENESS_CHECK_SECONDS,
            self._on_staleness_timer,
        )