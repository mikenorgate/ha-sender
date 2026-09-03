"""HeartbeatGenerator — sends periodic heartbeat frames when the event stream is quiet.

S04: prevents false gap detection in ingress (15s heartbeat timeout).
Periodic inventory resync is handled by RegistryPusher (S03).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from .const import HEARTBEAT_INTERVAL_SECONDS
from .frame import build_heartbeat

if TYPE_CHECKING:
    from .pusher import IngressHTTPPusher
    from .metrics import RuntimeMetrics

_LOGGER = logging.getLogger(__name__)


class HeartbeatGenerator:
    """Sends heartbeat frames on a timer when no events have been pushed recently.

    Stops permanently when pusher._auth_failed is set. Skips sending when the
    stream is active (last push was within HEARTBEAT_INTERVAL_SECONDS).

    Parameters
    ----------
    hass : Any
        Home Assistant core object (provides loop.call_later).
    pusher : IngressHTTPPusher
        IngressHTTPPusher instance used to call add_frame.
    metrics : RuntimeMetrics
        RuntimeMetrics instance used to read last_push_time.
    seq_counter : SequenceCounter
        Sequence counter for heartbeat source_sequence values.
    """

    def __init__(
        self,
        hass: object,
        pusher: IngressHTTPPusher,
        metrics: RuntimeMetrics,
        seq_counter: object,
    ) -> None:
        self._hass = hass
        self._pusher = pusher
        self._metrics = metrics
        self._seq_counter = seq_counter

        self._timer: object | None = None
        self._stopped = False

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the heartbeat timer if not already running and not stopped."""
        if self._stopped or self._timer is not None:
            return
        self._schedule_heartbeat()

    def stop(self) -> None:
        """Stop the heartbeat timer and prevent further heartbeats."""
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    # -------------------------------------------------------------------------
    # Internal: timer
    # -------------------------------------------------------------------------

    def _schedule_heartbeat(self) -> None:
        """Schedule _on_heartbeat_timer after HEARTBEAT_INTERVAL_SECONDS."""
        self._timer = self._hass.loop.call_later(
            HEARTBEAT_INTERVAL_SECONDS,
            self._on_heartbeat_timer,
        )

    def _on_heartbeat_timer(self) -> None:
        """Called by the event loop; checks conditions and sends a heartbeat or skips."""
        if self._stopped:
            return

        # Auth failure — stop permanently, no reschedule.
        if getattr(self._pusher, "_auth_failed", False):
            _LOGGER.debug("%s heartbeat stopped: auth failure", __name__)
            self._timer = None
            return

        # Stream is active — skip this cycle, reschedule.
        now = time.time()
        elapsed = now - self._metrics.last_push_time
        if elapsed < HEARTBEAT_INTERVAL_SECONDS:
            _LOGGER.debug(
                "%s heartbeat skipped: last push %.1fs ago < %ds threshold",
                __name__,
                elapsed,
                HEARTBEAT_INTERVAL_SECONDS,
            )
            self._schedule_heartbeat()
            return

        # Stream is quiet — send heartbeat.
        frame = build_heartbeat(self._seq_counter)
        self._pusher.add_frame(frame)
        _LOGGER.debug("%s heartbeat sent: stream quiet (last push %.1fs ago)", __name__, elapsed)

        # Always reschedule regardless of whether heartbeat was sent.
        self._schedule_heartbeat()