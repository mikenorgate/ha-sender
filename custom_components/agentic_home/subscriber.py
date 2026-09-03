"""EventSubscriber — listens to the Home Assistant event bus and forwards frames to the ingress pusher."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import Event

from .const import CONF_EXCLUDE_ENTITIES, DOMAIN
from .frame import SequenceCounter, build_frame
from .metrics import RuntimeMetrics
from .pusher import IngressHTTPPusher
from .sensor import SENSOR_KEYS

_LOGGER = logging.getLogger(__name__)


class EventSubscriber:
    """Subscribes to the HA event bus, translates events to frames, and pushes them.

    Parameters
    ----------
    hass : Any
        The Home Assistant hass object.
    entry : Any
        The config entry for this integration (entry_id used as key in hass.data).
    pusher : IngressHTTPPusher
        The batch pusher that sends frames to the ingress HTTP endpoint.
    metrics : RuntimeMetrics
        Shared runtime metrics (frames_captured, error_count, etc.).
    seq_counter : SequenceCounter
        Shared monotonic sequence counter across all subscribers.
    """

    def __init__(
        self,
        hass: Any,
        entry: Any,
        pusher: IngressHTTPPusher,
        metrics: RuntimeMetrics,
        seq_counter: SequenceCounter,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._pusher = pusher
        self._metrics = metrics
        self._seq_counter = seq_counter

        # Entity IDs that should not produce frames.
        self._excluded_entities: list[str] = []
        self._unregister: Any = None
        self._running = False

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Register the '*' event listener on hass.bus.

        Initializes excluded_entities from entry.options before registering.
        Idempotent: subsequent calls to start() are no-ops while running.
        """
        if self._running:
            return

        # Load excluded entities from OptionsFlow-sourced entry.options.
        self._excluded_entities = list(
            self._entry.options.get(CONF_EXCLUDE_ENTITIES, [])
        )

        # Self-exclusion: prevent this integration's own sensor entities from
        # generating frames that would be reported to the integration itself.
        self_entity_ids = [
            f"sensor.agentic_home_{key}" for key in SENSOR_KEYS
        ] + ["binary_sensor.agentic_home_connection"]
        self._excluded_entities.extend(self_entity_ids)

        # Register the wildcard listener; store the cancel function.
        self._unregister = self._hass.bus.async_listen("*", self._on_event)
        self._running = True

        _LOGGER.debug(
            "%s[%s] subscriber started, excluded_entities=%s",
            DOMAIN,
            self._entry.entry_id,
            self._excluded_entities,
        )

    def stop(self) -> None:
        """Cancel the event listener and mark the subscriber as stopped.

        Idempotent: multiple calls are safe.
        """
        if not self._running:
            return

        if self._unregister is not None:
            self._unregister()
            self._unregister = None

        self._running = False

        _LOGGER.debug(
            "%s[%s] subscriber stopped",
            DOMAIN,
            self._entry.entry_id,
        )

    @property
    def running(self) -> bool:
        """Return True if the subscriber is currently listening for events."""
        return self._running

    # -------------------------------------------------------------------------
    # Internal: event handler
    # -------------------------------------------------------------------------

    def _on_event(self, event: Event) -> None:
        """Translate a HA event to a frame and add it to the pusher.

        Skips events with no event_type. Skips events whose entity_id is in
        the excluded_entities list. Increments frames_captured on the shared
        metrics for every frame successfully forwarded to the pusher.
        """
        event_type = getattr(event, "event_type", None)
        if not event_type:
            _LOGGER.debug(
                "%s[%s] event with no event_type — skipping",
                DOMAIN,
                getattr(event, "event_id", "?"),
            )
            return

        # Entity exclusion filter.
        data: dict[str, Any] = getattr(event, "data", {})
        entity_id: str = data.get("entity_id", "") if isinstance(data, dict) else ""
        if entity_id and entity_id in self._excluded_entities:
            _LOGGER.debug(
                "%s[%s] entity %s is excluded — skipping",
                DOMAIN,
                self._entry.entry_id,
                entity_id,
            )
            return

        # Build the frame.
        frame = build_frame(event, self._seq_counter)
        if frame is None:
            # build_frame already logged a warning.
            self._metrics.increment_error()
            return

        # Forward to the batch pusher.
        self._pusher.add_frame(frame)