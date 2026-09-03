"""RegistryPusher — pushes HA entity/device/area/floor/label topology to the platform."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import random
import time
from datetime import datetime as _dt
from typing import Any

import aiohttp
from homeassistant.helpers.aiohttp_client import async_get_clientsession


def _json_default(obj: Any) -> Any:
    """Default serializer for objects not natively supported by json.dumps."""
    if isinstance(obj, _dt):
        return obj.isoformat()
    return str(obj)

from .const import (
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
    HTTP_TIMEOUT_SECONDS,
    INGRESS_REGISTRY_PATH,
    INVENTORY_INTERVAL_SECONDS,
    INVENTORY_JITTER_SECONDS,
)
from .metrics import RuntimeMetrics
from .topology_builder import collect_topology

_LOGGER = logging.getLogger(__name__)

# Retry backoff schedule (seconds)
_RETRY_DELAYS = (30, 60, 120)
_MAX_RETRIES = 3


class RegistryPusher:
    """Pushes HA registry topology to the platform on a configurable schedule.

    Runs on startup and then hourly with jitter. Gzips the JSON payload.
    Retries transient failures with exponential backoff. Stops permanently
    on auth failure (401/403).
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

        self._periodic_handle: asyncio.TimerHandle | None = None
        self._retry_handle: asyncio.TimerHandle | None = None
        self._auth_failed = False
        self._stopped = False
        self._inflight_task: asyncio.Task[None] | None = None

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def push_snapshot(self) -> None:
        """Collect the HA topology, gzip it, and POST to the registry endpoint.

        Records success/failure in RuntimeMetrics. On auth failure, sets the
        permanent-stop flag. On 5xx/network errors, schedules a retry.
        On success, records metrics and schedules the next periodic push.
        """
        if self._auth_failed or self._stopped:
            return

        try:
            snapshot = await collect_topology(self._hass, self._entry)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s topology collection failed: %s",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_registry_error_msg(
                f"topology collection failed: {str(exc)[:150]}"
            )
            self._schedule_retry(attempt=0)
            return

        json_bytes = json.dumps(snapshot, default=_json_default).encode("utf-8")
        gzipped = gzip.compress(json_bytes)

        url = f"{self._ingress_url}{INGRESS_REGISTRY_PATH}"
        headers = {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        try:
            async with self._session.post(
                url,
                data=gzipped,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                status = response.status
                now = time.time()

                if status == 202:
                    _LOGGER.info(
                        "%s/%s registry push: areas=%d devices=%d entities=%d status %d",
                        __name__,
                        self._entry.entry_id,
                        len(snapshot.get("areas", [])),
                        len(snapshot.get("devices", [])),
                        len(snapshot.get("entity_device_mappings", [])),
                        status,
                    )
                    self._metrics.record_registry_push(status, now)
                    self._schedule_next_inventory()
                    return

                if status in (401, 403):
                    _LOGGER.error(
                        "%s/%s auth failure — registry pusher stopped (status %d)",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._auth_failed = True
                    self._metrics.record_registry_error_msg(f"auth failure (status {status})")
                    self._metrics.record_registry_push(status, now)
                    return

                if status == 400:
                    _LOGGER.warning(
                        "%s/%s registry push rejected: status %d — scheduling next periodic push",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._metrics.record_registry_error_msg(f"rejected (status {status})")
                    self._metrics.record_registry_push(status, now)
                    self._schedule_next_inventory()
                    return

                # 5xx or unexpected — retry with backoff
                _LOGGER.warning(
                    "%s/%s registry push failed: status %d — will retry",
                    __name__,
                    self._entry.entry_id,
                    status,
                )
                self._metrics.record_registry_error_msg(f"server error (status {status})")
                self._schedule_retry(attempt=0)

        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s/%s registry push timed out after %ds — will retry",
                __name__,
                self._entry.entry_id,
                HTTP_TIMEOUT_SECONDS,
            )
            self._metrics.record_registry_error_msg(f"timeout after {HTTP_TIMEOUT_SECONDS}s")
            self._schedule_retry(attempt=0)

        except aiohttp.ClientError as exc:
            _LOGGER.warning(
                "%s/%s registry push error: %s — will retry",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_registry_error_msg(f"connection error: {str(exc)[:150]}")
            self._schedule_retry(attempt=0)

    def schedule_next_inventory(self) -> None:
        """Schedule the next periodic inventory push with jitter.

        Interval = INVENTORY_INTERVAL_SECONDS ± INVENTORY_JITTER_SECONDS,
        i.e.  3600 + random(-120, +120) → range [3480, 3720] seconds.
        """
        if self._auth_failed or self._stopped:
            return

        jitter = random.uniform(
            -INVENTORY_JITTER_SECONDS, INVENTORY_JITTER_SECONDS
        )
        interval = INVENTORY_INTERVAL_SECONDS + jitter

        _LOGGER.debug(
            "%s/%s next inventory push in %.1f seconds",
            __name__,
            self._entry.entry_id,
            interval,
        )

        self._periodic_handle = self._hass.loop.call_later(
            interval,
            self._on_periodic_timer,
        )

    def _schedule_next_inventory(self) -> None:
        """Internal alias — schedules next periodic push.

        Called from retry and error paths; delegates to schedule_next_inventory.
        """
        self.schedule_next_inventory()

    def start(self) -> None:
        """Start the registry pusher: push immediately, then schedule periodic runs.

        Called from __init__.py after the event subscriber starts.
        """
        if self._auth_failed or self._stopped:
            return

        _LOGGER.info(
            "%s/%s registry pusher starting",
            __name__,
            self._entry.entry_id,
        )

        # Immediate first push.
        self._inflight_task = asyncio.create_task(self.push_snapshot())

    async def stop(self) -> None:
        """Stop the registry pusher: cancel pending timers and drain inflight POST."""
        if self._stopped:
            return

        # Cancel any pending handles first.
        if self._periodic_handle is not None:
            self._periodic_handle.cancel()
            self._periodic_handle = None

        if self._retry_handle is not None:
            self._retry_handle.cancel()
            self._retry_handle = None

        self._stopped = True

        if self._inflight_task is not None:
            try:
                await asyncio.wait_for(self._inflight_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._inflight_task.cancel()
            except asyncio.CancelledError:
                pass
            finally:
                self._inflight_task = None

        _LOGGER.debug(
            "%s/%s registry pusher stopped",
            __name__,
            self._entry.entry_id,
        )

    # -------------------------------------------------------------------------
    # Internal: timers
    # -------------------------------------------------------------------------

    def _on_periodic_timer(self) -> None:
        """Called by the event loop when the periodic interval expires."""
        self._periodic_handle = None
        if self._stopped or self._auth_failed:
            return

        self._inflight_task = asyncio.create_task(self.push_snapshot())

    def _schedule_retry(self, attempt: int) -> None:
        """Schedule a retry push after a delay from the backoff schedule.

        After _MAX_RETRIES exhausted, falls back to the periodic schedule.
        """
        if self._stopped or self._auth_failed:
            return

        if attempt >= _MAX_RETRIES:
            _LOGGER.info(
                "%s/%s all retries exhausted — scheduling next periodic push",
                __name__,
                self._entry.entry_id,
            )
            self._schedule_next_inventory()
            return

        delay = _RETRY_DELAYS[attempt]
        _LOGGER.info(
            "%s/%s retry %d/%d in %ds",
            __name__,
            self._entry.entry_id,
            attempt + 1,
            _MAX_RETRIES,
            delay,
        )

        self._retry_handle = self._hass.loop.call_later(
            delay,
            lambda: self._on_retry_timer(attempt),
        )

    def _on_retry_timer(self, attempt: int) -> None:
        """Called by the event loop after the backoff delay; re-attempts the push."""
        self._retry_handle = None
        if self._stopped or self._auth_failed:
            return

        asyncio.create_task(self._retry_push(attempt))

    async def _retry_push(self, attempt: int) -> None:
        """Re-attempt the push; if it fails again, schedule the next backoff step."""
        if self._stopped or self._auth_failed:
            return

        try:
            snapshot = await collect_topology(self._hass, self._entry)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s topology collection failed on retry: %s",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_registry_error_msg(
                f"topology collection failed on retry: {str(exc)[:150]}"
            )
            self._schedule_retry(attempt)
            return

        json_bytes = json.dumps(snapshot, default=_json_default).encode("utf-8")
        gzipped = gzip.compress(json_bytes)

        url = f"{self._ingress_url}{INGRESS_REGISTRY_PATH}"
        headers = {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        try:
            async with self._session.post(
                url,
                data=gzipped,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
            ) as response:
                status = response.status
                now = time.time()

                if status == 202:
                    _LOGGER.info(
                        "%s/%s registry push on retry %d/%d succeeded (status %d)",
                        __name__,
                        self._entry.entry_id,
                        attempt + 1,
                        _MAX_RETRIES,
                        status,
                    )
                    self._metrics.record_registry_push(status, now)
                    self._schedule_next_inventory()
                    return

                if status in (401, 403):
                    _LOGGER.error(
                        "%s/%s auth failure on retry — stopping pusher (status %d)",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._auth_failed = True
                    self._metrics.record_registry_error_msg(f"auth failure on retry (status {status})")
                    self._metrics.record_registry_push(status, now)
                    return

                if status == 400:
                    _LOGGER.warning(
                        "%s/%s registry push rejected on retry: status %d",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._metrics.record_registry_error_msg(f"rejected on retry (status {status})")
                    self._metrics.record_registry_push(status, now)
                    self._schedule_next_inventory()
                    return

                _LOGGER.warning(
                    "%s/%s retry %d/%d failed (status %d) — retrying",
                    __name__,
                    self._entry.entry_id,
                    attempt + 1,
                    _MAX_RETRIES,
                    status,
                )
                self._metrics.record_registry_error_msg(f"server error on retry (status {status})")
                self._schedule_retry(attempt + 1)

        except asyncio.TimeoutError:
            _LOGGER.warning(
                "%s/%s retry %d/%d timed out — retrying",
                __name__,
                self._entry.entry_id,
                attempt + 1,
                _MAX_RETRIES,
            )
            self._metrics.record_registry_error_msg(
                f"timeout on retry {attempt+1}/{_MAX_RETRIES}"
            )
            self._schedule_retry(attempt + 1)

        except aiohttp.ClientError as exc:
            _LOGGER.warning(
                "%s/%s retry %d/%d error: %s — retrying",
                __name__,
                self._entry.entry_id,
                attempt + 1,
                _MAX_RETRIES,
                exc,
            )
            self._metrics.record_registry_error_msg(
                f"connection error on retry: {str(exc)[:150]}"
            )
            self._schedule_retry(attempt + 1)