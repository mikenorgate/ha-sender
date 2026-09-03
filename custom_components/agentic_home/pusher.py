"""IngressHTTPPusher — batches HA frames and POSTs them to the ingress HTTP endpoint."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime as _dt
from typing import Any

import aiohttp


def _json_default(obj: Any) -> Any:
    """Default serializer for objects not natively supported by json.dumps.

    Handles datetime (HA event payloads contain these) and falls back to str()
    for anything else.
    """
    if isinstance(obj, _dt):
        return obj.isoformat()
    return str(obj)
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    BATCH_FLUSH_INTERVAL_MS,
    BATCH_MAX_FRAMES,
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
    HTTP_TIMEOUT_SECONDS,
    INGRESS_STREAM_PATH,
)
from .metrics import RuntimeMetrics

_LOGGER = logging.getLogger(__name__)


class IngressHTTPPusher:
    """Batches frames and pushes them to the ingress HTTP endpoint.

    Flushes when either the time window (BATCH_FLUSH_INTERVAL_MS) expires
    or the batch reaches BATCH_MAX_FRAMES. Non-2xx responses use drop-and-log
    semantics. Auth failures (401/403) permanently stop the pusher.
    """

    def __init__(
        self,
        hass: Any,
        entry: Any,
        metrics: RuntimeMetrics,
    ) -> None:
        self._hass = hass
        self._entry = entry
        self._metrics = metrics

        self._ingress_url = entry.data.get(CONF_INGRESS_URL, "").rstrip("/")
        self._jwt_token = entry.data.get(CONF_JWT_TOKEN, "")

        self._session = async_get_clientsession(hass)

        self._batch: list[dict[str, Any]] = []
        self._auth_failed = False
        self._running = False
        self._flush_task: asyncio.TimerHandle | None = None
        self._stopped = False

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def add_frame(self, frame: dict[str, Any]) -> None:
        """Add a frame to the current batch.

        Silently drops if auth has failed. Schedules an immediate flush
        when the batch cap is reached.
        """
        if self._auth_failed or self._stopped:
            return

        self._batch.append(frame)
        self._metrics.increment_captured(1)

        if len(self._batch) >= BATCH_MAX_FRAMES:
            # Schedule an immediate flush from the event loop.
            self._hass.loop.call_soon(self._schedule_flush_soon)
        else:
            self._ensure_flush_timer()

    def start(self) -> None:
        """Start the periodic flush timer."""
        if self._running or self._stopped:
            return
        self._running = True
        self._schedule_flush()

    async def stop(self) -> None:
        """Stop the pusher: cancel the timer and drain any pending frames."""
        if self._stopped:
            return
        self._stopped = True

        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None

        self._running = False

        # Drain the buffer.
        if self._batch:
            await self._flush_batch()

    async def force_flush(self) -> None:
        """Public API for unload drain — flushes immediately."""
        if self._stopped:
            return
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        if self._batch:
            await self._flush_batch()

    # -------------------------------------------------------------------------
    # Internal: timer management
    # -------------------------------------------------------------------------

    def _ensure_flush_timer(self) -> None:
        """Start the periodic flush timer if it is not already running."""
        if self._flush_task is not None:
            return  # already scheduled
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        """Schedule _on_flush_timer after BATCH_FLUSH_INTERVAL_MS."""
        self._flush_task = self._hass.loop.call_later(
            BATCH_FLUSH_INTERVAL_MS / 1000,
            self._on_flush_timer,
        )

    def _schedule_flush_soon(self) -> None:
        """Schedule an immediate flush from call_soon context."""
        # Use call_later(0) to flush on the next event loop iteration.
        self._hass.loop.call_later(0, self._on_flush_timer)

    def _on_flush_timer(self) -> None:
        """Called by the event loop after the flush interval; performs async flush."""
        self._flush_task = None
        if not self._running or self._stopped:
            return

        # Schedule the async flush without blocking the timer thread.
        asyncio.create_task(self._flush_batch())

        # Reschedule if still running.
        if self._running and not self._stopped:
            self._schedule_flush()

    # -------------------------------------------------------------------------
    # Internal: HTTP push
    # -------------------------------------------------------------------------

    async def _flush_batch(self) -> None:
        """Build NDJSON body and POST the current batch to ingress."""
        if not self._batch:
            return

        batch = self._batch
        self._batch = []
        batch_size = len(batch)

        body = "\n".join(
            json.dumps(frame, default=_json_default) for frame in batch
        )
        url = f"{self._ingress_url}{INGRESS_STREAM_PATH}"

        headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        try:
            async with self._session.post(
                url,
                data=body.encode("utf-8"),
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                status = response.status

                if status == 202:
                    _LOGGER.info(
                        "%s/%s batch push: %d frames, status %d",
                        __name__,
                        self._entry.entry_id,
                        batch_size,
                        status,
                    )
                    self._metrics.record_push(batch_size, status, time.time())
                    return

                if status in (401, 403):
                    _LOGGER.error(
                        "%s/%s auth failure — stopping pusher (status %d)",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._auth_failed = True
                    self._metrics.record_stream_error(f"auth failure (status {status})")
                    self._metrics.record_push(batch_size, status, time.time())
                    return

                # 400, 5xx, etc. — drop and log.
                _LOGGER.warning(
                    "%s/%s batch push failed: %d frames, status %d — dropping",
                    __name__,
                    self._entry.entry_id,
                    batch_size,
                    status,
                )
                self._metrics.record_stream_error(f"non-2xx response (status {status})")
                self._metrics.record_push(batch_size, status, time.time())

        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s/%s batch push timed out after %ds: %d frames — dropping",
                __name__,
                self._entry.entry_id,
                HTTP_TIMEOUT_SECONDS,
                batch_size,
            )
            self._metrics.record_stream_error(f"timeout after {HTTP_TIMEOUT_SECONDS}s")

        except aiohttp.ClientError as exc:
            _LOGGER.warning(
                "%s/%s batch push error: %d frames, %s — dropping",
                __name__,
                self._entry.entry_id,
                batch_size,
                exc,
            )
            self._metrics.record_stream_error(
                f"connection error: {self._truncate_error_detail(str(exc))}"
            )

    # -------------------------------------------------------------------------
    # Internal: helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _truncate_error_detail(detail: str, max_len: int = 150) -> str:
        """Truncate error detail to max_len characters.

        Keeps total error message ("stream: connection error: ...") under the
        HA entity state 255-character limit.
        """
        return detail[:max_len]