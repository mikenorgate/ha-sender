"""CatalogPusher — pushes HA service×entity catalog frames to the platform."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import aiohttp
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
    HTTP_TIMEOUT_SECONDS,
    INGRESS_STREAM_PATH,
    INVENTORY_INTERVAL_SECONDS,
    INVENTORY_JITTER_SECONDS,
)
from .frame import SequenceCounter
from .metrics import RuntimeMetrics

try:
    import voluptuous as vol

    _HAS_VOLUPTUOUS = True
except ImportError:  # pragma: no cover
    _HAS_VOLUPTUOUS = False

_LOGGER = logging.getLogger(__name__)

# Retry backoff schedule (seconds)
_RETRY_DELAYS = (30, 60, 120)
_MAX_RETRIES = 3
# Max frames per NDJSON POST chunk
_MAX_CHUNK_FRAMES = 500


class CatalogPusher:
    """Pushes the HA action catalog (entity×service matrix) to the platform.

    Enumerates all HA services, cross-references with entities for each
    domain, and POSTs action_catalog frames as NDJSON to the stream endpoint.
    Runs on startup and then hourly with jitter.

    Differences from RegistryPusher:
    - NDJSON body format (not gzip JSON)
    - Stream endpoint /api/v1/ingress/stream
    - Content-Type application/x-ndjson
    - No Content-Encoding header
    - Entity×service cross-product enumeration
    - Chunked posting (max 500 frames per POST)
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
        self._seq_counter = SequenceCounter()

        self._periodic_handle: asyncio.TimerHandle | None = None
        self._retry_handle: asyncio.TimerHandle | None = None
        self._unsub: Any = None
        self._auth_failed = False
        self._stopped = False
        self._inflight_task: asyncio.Task[None] | None = None

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    async def push_catalog(self) -> None:
        """Enumerate services×entities, build catalog frames, and POST as NDJSON.

        Records success/failure in RuntimeMetrics. On auth failure, sets the
        permanent-stop flag. On 5xx/network errors, schedules a retry.
        On success, records metrics and schedules the next periodic push.
        """
        if self._auth_failed or self._stopped:
            return

        try:
            frames = await self._build_catalog_frames()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s catalog enumeration failed: %s",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_catalog_error_msg(
                f"enumeration failed: {str(exc)[:150]}"
            )
            self._schedule_retry(attempt=0)
            return

        if not frames:
            _LOGGER.info(
                "%s/%s no catalog frames to push — scheduling next periodic push",
                __name__,
                self._entry.entry_id,
            )
            self._schedule_next_inventory()
            return

        await self._post_chunks(frames)

    async def _build_catalog_frames(self) -> list[dict]:
        """Build the list of action_catalog frame dicts.

        Returns an empty list when no services have entities (all domains skipped).
        """
        services = self._hass.services.async_services()

        frames: list[dict] = []
        for domain in sorted(services):
            # Skip domains with no entities.
            entity_ids = self._hass.states.async_entity_ids(domain)
            if not entity_ids:
                continue

            domain_services = services[domain]
            for entity_id in entity_ids:
                for service_name in sorted(domain_services):
                    frame = self._make_frame(
                        domain, entity_id, service_name,
                        service_obj=domain_services[service_name],
                    )
                    frames.append(frame)

        return frames

    def _make_frame(
        self,
        domain: str,
        entity_id: str,
        service_name: str,
        service_obj: Any | None = None,
    ) -> dict:
        """Build a single action_catalog frame for one entity×service pair.

        If *service_obj* is the HA ``Service`` object, its voluptuous schema
        is introspected to populate ``raw_action_data.fields`` with parameter
        metadata (name, type, required, min/max). When ``service_obj`` is
        ``None`` (e.g. in tests) the frame is still valid with empty fields.
        """
        fields = self._extract_fields_from_schema(
            getattr(service_obj, "schema", None) if service_obj else None
        )

        return {
            "source_event_id": uuid.uuid4().hex,
            "source_sequence": self._seq_counter.next(),
            "delivery_mode": "catalog",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "event_type": "action_catalog",
            "payload": {
                "entity_id": entity_id,
                "domain": domain,
                "service_name": service_name,
                "raw_action_data": {"fields": fields} if fields else {},
            },
        }

    @staticmethod
    def _extract_fields_from_schema(
        service_schema: Any,
    ) -> dict[str, dict[str, Any]]:
        """Extract parameter schema from a HA service object into a serializable dict.

        Input:``Service.schema`` — a voluptuous ``Schema`` / ``All`` / ``None``.
        Output: ``{param_name: {type, description, required, min, max}}``

        Returns an empty dict when the schema is None, non-voluptuous, or
        unparseable. Failures are silent — the frame is still valid without
        fields.
        """
        if not _HAS_VOLUPTUOUS or service_schema is None:
            return {}
        try:
            # vol.All wraps one or more inner validators.
            if isinstance(service_schema, vol.All):
                for inner in service_schema.validators:
                    if isinstance(inner, vol.Schema):
                        fields = CatalogPusher._schema_dict_to_fields(
                            inner.schema
                        )
                        return CatalogPusher._safe_json_fields(fields)
                return {}

            if isinstance(service_schema, vol.Schema):
                fields = CatalogPusher._schema_dict_to_fields(
                    service_schema.schema
                )
                return CatalogPusher._safe_json_fields(fields)

            return {}
        except Exception:  # noqa: BLE001
            # Any introspection failure → empty fields; frame is still valid.
            return {}

    @staticmethod
    def _schema_dict_to_fields(
        schema_dict: Any,
    ) -> dict[str, dict[str, Any]]:
        """Convert a voluptuous schema dict to JSON-serializable field metadata."""
        if not isinstance(schema_dict, dict):
            return {}

        fields: dict[str, dict[str, Any]] = {}
        for marker, validator in schema_dict.items():
            name = getattr(marker, "schema", None)
            if not isinstance(name, str):
                continue

            field: dict[str, Any] = {}

            # Required vs Optional.
            if _HAS_VOLUPTUOUS:
                if isinstance(marker, vol.Required):
                    field["required"] = True
                elif isinstance(marker, vol.Optional):
                    field["required"] = False

            # Description from marker.
            desc = getattr(marker, "description", None)
            if isinstance(desc, str) and desc:
                field["description"] = desc

            # Extract type from validator chain.
            type_label, constraints = CatalogPusher._describe_validator(
                validator
            )
            if type_label:
                field["type"] = type_label
            field.update(constraints)

            fields[name] = field

        return fields

    @staticmethod
    def _safe_json_fields(
        fields: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Strip or stringify any non-JSON-serializable values from *fields*.

        Acts as a defensive filter after voluptuous introspection: if a value
        inside a field dict is not a standard JSON type, convert it to its
        ``str()`` representation. The whole fields dict is dropped if the
        conversion still fails.
        """
        safe: dict[str, dict[str, Any]] = {}
        for field_name, field_dict in fields.items():
            safe_dict: dict[str, Any] = {}
            for key, value in field_dict.items():
                try:
                    json.dumps(value, sort_keys=True)
                    safe_dict[key] = value
                except (TypeError, ValueError):
                    # For lists, try stringifying individual elements.
                    if isinstance(value, (list, tuple)):
                        safe_list = [str(v) for v in value]
                        safe_dict[key] = safe_list
                    else:
                        safe_dict[key] = str(value)
            safe[field_name] = safe_dict

        # Final verification: the entire output must be JSON-serializable.
        try:
            json.dumps(safe, sort_keys=True)
            return safe
        except (TypeError, ValueError):
            return {}

    @staticmethod
    def _describe_validator(
        validator: Any,
    ) -> tuple[str, dict[str, Any]]:
        """Return (type_label, constraints_dict) from a voluptuous validator."""
        type_label = ""
        constraints: dict[str, Any] = {}

        if not _HAS_VOLUPTUOUS:
            return type_label, constraints

        if isinstance(validator, vol.All):
            # Walk children to find Coerce type + Range bounds.
            for inner in validator.validators:
                if isinstance(inner, vol.Coerce):
                    type_label = getattr(
                        inner.type, "__name__", str(inner.type)
                    )
                elif isinstance(inner, vol.Range):
                    if inner.min is not None:
                        constraints["min"] = inner.min
                    if inner.max is not None:
                        constraints["max"] = inner.max
                elif isinstance(inner, (list, tuple)):
                    try:
                        opts = list(inner)
                        json.dumps(opts, sort_keys=True)
                        constraints["options"] = opts
                    except (TypeError, ValueError):
                        pass
                elif isinstance(inner, vol.In):
                    try:
                        opts = list(inner.container)
                        json.dumps(opts, sort_keys=True)
                        constraints["options"] = opts
                    except (TypeError, ValueError):
                        pass
                elif isinstance(inner, vol.Length):
                    if inner.min is not None:
                        constraints["min_length"] = inner.min
                    if inner.max is not None:
                        constraints["max_length"] = inner.max
            return type_label, constraints

        if isinstance(validator, vol.Coerce):
            return getattr(validator.type, "__name__", str(validator.type)), {}

        if isinstance(validator, vol.Any):
            # Collect type options.
            types = []
            for inner in validator.validators:
                if isinstance(inner, vol.Coerce):
                    types.append(
                        getattr(inner.type, "__name__", str(inner.type))
                    )
                elif isinstance(inner, type):
                    types.append(inner.__name__)
            if types:
                return "/".join(types), {}
            return "", {}

        if isinstance(validator, vol.In):
            try:
                opts = [v for v in validator.container]
                # Verify every option is JSON-serializable; drop if not.
                json.dumps(opts, sort_keys=True)
                return "", {"options": opts}
            except (TypeError, ValueError):
                return "", {}

        if isinstance(validator, type):
            return validator.__name__, {}

        return type_label, constraints

    async def _post_chunks(self, frames: list[dict]) -> None:
        """POST frames in NDJSON chunks to the stream endpoint.

        Splits frames into batches of _MAX_CHUNK_FRAMES.
        Each batch is sent as a single NDJSON POST.
        Response handling mirrors RegistryPusher: 202=success, 401/403=stop,
        400=skip, 5xx/network=retry.
        """
        url = f"{self._ingress_url}{INGRESS_STREAM_PATH}"
        headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        for i in range(0, len(frames), _MAX_CHUNK_FRAMES):
            chunk = frames[i : i + _MAX_CHUNK_FRAMES]
            body = "\n".join(json.dumps(f, sort_keys=True) for f in chunk) + "\n"

            try:
                async with self._session.post(
                    url,
                    data=body.encode("utf-8"),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
                ) as response:
                    status = response.status
                    now = time.time()

                    if status == 202:
                        _LOGGER.info(
                            "%s/%s catalog push: %d frames status %d",
                            __name__,
                            self._entry.entry_id,
                            len(chunk),
                            status,
                        )
                        self._metrics.record_catalog_push(status, now)
                        # Continue to next chunk
                        continue

                    if status in (401, 403):
                        _LOGGER.error(
                            "%s/%s auth failure — catalog pusher stopped (status %d)",
                            __name__,
                            self._entry.entry_id,
                            status,
                        )
                        self._auth_failed = True
                        self._metrics.record_catalog_error_msg(
                            f"auth failure (status {status})"
                        )
                        return  # Stop all chunks on auth failure

                    if status == 400:
                        _LOGGER.warning(
                            "%s/%s catalog push rejected: status %d — skipping remaining chunks",
                            __name__,
                            self._entry.entry_id,
                            status,
                        )
                        self._metrics.record_catalog_error_msg(
                            f"rejected (status {status})"
                        )
                        self._schedule_next_inventory()
                        return  # Stop all chunks on 400

                    # 5xx or unexpected — retry
                    _LOGGER.warning(
                        "%s/%s catalog push failed: status %d — will retry",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._metrics.record_catalog_error_msg(
                        f"server error (status {status})"
                    )
                    self._schedule_retry(attempt=0)
                    return

            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "%s/%s catalog push timed out after %ds — will retry",
                    __name__,
                    self._entry.entry_id,
                    HTTP_TIMEOUT_SECONDS,
                )
                self._metrics.record_catalog_error_msg(
                    f"timeout after {HTTP_TIMEOUT_SECONDS}s"
                )
                self._schedule_retry(attempt=0)
                return

            except aiohttp.ClientError as exc:
                _LOGGER.warning(
                    "%s/%s catalog push error: %s — will retry",
                    __name__,
                    self._entry.entry_id,
                    exc,
                )
                self._metrics.record_catalog_error_msg(
                    f"connection error: {str(exc)[:150]}"
                )
                self._schedule_retry(attempt=0)
                return

        # All chunks posted successfully — schedule next periodic.
        _LOGGER.info(
            "%s/%s catalog push complete: %d total frames",
            __name__,
            self._entry.entry_id,
            len(frames),
        )
        self._schedule_next_inventory()

    def schedule_next_inventory(self) -> None:
        """Schedule the next periodic catalog push with jitter.

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
            "%s/%s next catalog push in %.1f seconds",
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
        """Start the catalog pusher: push immediately, then schedule periodic runs.

        Called from __init__.py after the event subscriber starts.
        """
        if self._auth_failed or self._stopped:
            return

        _LOGGER.info(
            "%s/%s catalog pusher starting",
            __name__,
            self._entry.entry_id,
        )

        # Immediate first push.
        self._inflight_task = asyncio.create_task(self.push_catalog())

        # Subscribe to service_registered events for incremental catalog updates.
        self._unsub = self._hass.bus.async_listen(
            "service_registered", self._on_service_registered
        )

    async def stop(self) -> None:
        """Stop the catalog pusher: cancel pending timers and drain inflight POST."""
        if self._stopped:
            return

        # Cancel any pending handles first.
        if self._periodic_handle is not None:
            self._periodic_handle.cancel()
            self._periodic_handle = None

        if self._retry_handle is not None:
            self._retry_handle.cancel()
            self._retry_handle = None

        # Cancel the service_registered event subscription.
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

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
            "%s/%s catalog pusher stopped",
            __name__,
            self._entry.entry_id,
        )

    # -------------------------------------------------------------------------
    # Internal: incremental catalog updates (service_registered events)
    # -------------------------------------------------------------------------

    async def _on_service_registered(self, event: Any) -> None:
        """Handle a service_registered event from the HA event bus.

        Extracts the domain from the event data and pushes an incremental
        catalog update for that domain only.

        Must be async so HA dispatches it in the event loop rather than a
        thread pool — asyncio.create_task requires a running event loop.
        """
        domain = event.data.get("domain")
        service = event.data.get("service")

        if not domain:
            _LOGGER.debug(
                "%s/%s service_registered event missing domain — skipping",
                __name__,
                self._entry.entry_id,
            )
            return

        _LOGGER.info(
            "%s/%s catalog incremental push: domain=%s service=%s",
            __name__,
            self._entry.entry_id,
            domain,
            service,
        )
        asyncio.create_task(self._push_domain_catalog(domain))

    async def _push_domain_catalog(self, domain: str) -> None:
        """Push catalog frames for a single domain (incremental update).

        Re-enumerates entities for the domain, builds frames for each
        entity×service combination, and POSTs as NDJSON.  Errors are
        logged but do not schedule retries — the next periodic push
        will recover naturally.
        """
        if self._auth_failed or self._stopped:
            return

        # Get entity IDs for this domain.
        try:
            entity_ids = self._hass.states.async_entity_ids(domain)
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s incremental catalog: async_entity_ids(%s) failed: %s",
                __name__,
                self._entry.entry_id,
                domain,
                exc,
            )
            self._metrics.record_catalog_error_msg(
                f"incremental async_entity_ids({domain}) failed: {str(exc)[:120]}"
            )
            return

        if not entity_ids:
            _LOGGER.debug(
                "%s/%s incremental catalog: domain=%s has no entities — skipped",
                __name__,
                self._entry.entry_id,
                domain,
            )
            return

        # Get services for this domain.
        try:
            all_services = self._hass.services.async_services()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s incremental catalog: async_services() failed: %s",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_catalog_error_msg(
                f"incremental async_services() failed: {str(exc)[:150]}"
            )
            return

        domain_services = all_services.get(domain, {})
        if not domain_services:
            _LOGGER.debug(
                "%s/%s incremental catalog: domain=%s has no services — skipped",
                __name__,
                self._entry.entry_id,
                domain,
            )
            return

        # Build frames.
        frames: list[dict] = []
        for entity_id in entity_ids:
            for service_name in sorted(domain_services):
                frames.append(
                    self._make_frame(
                        domain, entity_id, service_name,
                        service_obj=domain_services[service_name],
                    )
                )

        # Push via the same NDJSON transport (no retry scheduling on error).
        await self._push_incremental_chunks(frames)

    async def _push_incremental_chunks(self, frames: list[dict]) -> None:
        """POST incremental catalog frames as NDJSON chunks.

        Same HTTP transport as _post_chunks but on error the function
        logs and returns without scheduling retries or periodic pushes.
        Auth failures still set the permanent-stop flag.
        """
        if not frames:
            return

        url = f"{self._ingress_url}{INGRESS_STREAM_PATH}"
        headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        for i in range(0, len(frames), _MAX_CHUNK_FRAMES):
            chunk = frames[i : i + _MAX_CHUNK_FRAMES]
            body = "\n".join(json.dumps(f, sort_keys=True) for f in chunk) + "\n"

            try:
                async with self._session.post(
                    url,
                    data=body.encode("utf-8"),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
                ) as response:
                    status = response.status
                    now = time.time()

                    if status == 202:
                        _LOGGER.info(
                            "%s/%s incremental catalog push: %d frames status %d",
                            __name__,
                            self._entry.entry_id,
                            len(chunk),
                            status,
                        )
                        self._metrics.record_catalog_push(status, now)
                        continue

                    if status in (401, 403):
                        _LOGGER.error(
                            "%s/%s auth failure on incremental push (status %d)",
                            __name__,
                            self._entry.entry_id,
                            status,
                        )
                        self._auth_failed = True
                        self._metrics.record_catalog_error_msg(
                            f"auth failure on incremental (status {status})"
                        )
                        return

                    _LOGGER.warning(
                        "%s/%s incremental catalog push failed: status %d",
                        __name__,
                        self._entry.entry_id,
                        status,
                    )
                    self._metrics.record_catalog_error_msg(
                        f"incremental push failed (status {status})"
                    )
                    return

            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "%s/%s incremental catalog push timed out after %ds",
                    __name__,
                    self._entry.entry_id,
                    HTTP_TIMEOUT_SECONDS,
                )
                self._metrics.record_catalog_error_msg(
                    f"incremental timeout after {HTTP_TIMEOUT_SECONDS}s"
                )
                return

            except aiohttp.ClientError as exc:
                _LOGGER.warning(
                    "%s/%s incremental catalog push error: %s",
                    __name__,
                    self._entry.entry_id,
                    exc,
                )
                self._metrics.record_catalog_error_msg(
                    f"incremental connection error: {str(exc)[:150]}"
                )
                return

    # -------------------------------------------------------------------------
    # Internal: timers
    # -------------------------------------------------------------------------

    def _on_periodic_timer(self) -> None:
        """Called by the event loop when the periodic interval expires."""
        self._periodic_handle = None
        if self._stopped or self._auth_failed:
            return

        self._inflight_task = asyncio.create_task(self.push_catalog())

    def _schedule_retry(self, attempt: int) -> None:
        """Schedule a retry push after a delay from the backoff schedule.

        After _MAX_RETRIES exhausted, falls back to the periodic schedule.
        """
        if self._stopped or self._auth_failed:
            return

        if attempt >= _MAX_RETRIES:
            _LOGGER.info(
                "%s/%s all catalog retries exhausted — scheduling next periodic push",
                __name__,
                self._entry.entry_id,
            )
            self._schedule_next_inventory()
            return

        delay = _RETRY_DELAYS[attempt]
        _LOGGER.info(
            "%s/%s catalog retry %d/%d in %ds",
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
            frames = await self._build_catalog_frames()
        except Exception as exc:  # noqa: BLE001
            _LOGGER.warning(
                "%s/%s catalog enumeration failed on retry: %s",
                __name__,
                self._entry.entry_id,
                exc,
            )
            self._metrics.record_catalog_error_msg(
                f"enumeration failed on retry: {str(exc)[:150]}"
            )
            self._schedule_retry(attempt)
            return

        if not frames:
            _LOGGER.info(
                "%s/%s no catalog frames on retry — scheduling next periodic push",
                __name__,
                self._entry.entry_id,
            )
            self._schedule_next_inventory()
            return

        await self._retry_post_chunks(frames, attempt)

    async def _retry_post_chunks(
        self, frames: list[dict], attempt: int
    ) -> None:
        """POST frames in NDJSON chunks on a retry attempt.

        Same chunked logic as _post_chunks but with retry-aware logging
        and attempt tracking.
        """
        url = f"{self._ingress_url}{INGRESS_STREAM_PATH}"
        headers = {
            "Content-Type": "application/x-ndjson",
            "Authorization": f"Bearer {self._jwt_token}",
        }

        for i in range(0, len(frames), _MAX_CHUNK_FRAMES):
            chunk = frames[i : i + _MAX_CHUNK_FRAMES]
            body = "\n".join(json.dumps(f, sort_keys=True) for f in chunk) + "\n"

            try:
                async with self._session.post(
                    url,
                    data=body.encode("utf-8"),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
                ) as response:
                    status = response.status
                    now = time.time()

                    if status == 202:
                        _LOGGER.info(
                            "%s/%s catalog push on retry %d/%d: %d frames status %d",
                            __name__,
                            self._entry.entry_id,
                            attempt + 1,
                            _MAX_RETRIES,
                            len(chunk),
                            status,
                        )
                        self._metrics.record_catalog_push(status, now)
                        continue

                    if status in (401, 403):
                        _LOGGER.error(
                            "%s/%s auth failure on catalog retry — stopping (status %d)",
                            __name__,
                            self._entry.entry_id,
                            status,
                        )
                        self._auth_failed = True
                        self._metrics.record_catalog_error_msg(
                            f"auth failure on retry (status {status})"
                        )
                        return

                    if status == 400:
                        _LOGGER.warning(
                            "%s/%s catalog push rejected on retry: status %d",
                            __name__,
                            self._entry.entry_id,
                            status,
                        )
                        self._metrics.record_catalog_error_msg(
                            f"rejected on retry (status {status})"
                        )
                        self._schedule_next_inventory()
                        return

                    _LOGGER.warning(
                        "%s/%s catalog retry %d/%d failed (status %d) — retrying",
                        __name__,
                        self._entry.entry_id,
                        attempt + 1,
                        _MAX_RETRIES,
                        status,
                    )
                    self._metrics.record_catalog_error_msg(
                        f"server error on retry (status {status})"
                    )
                    self._schedule_retry(attempt + 1)
                    return

            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "%s/%s catalog retry %d/%d timed out — retrying",
                    __name__,
                    self._entry.entry_id,
                    attempt + 1,
                    _MAX_RETRIES,
                )
                self._metrics.record_catalog_error_msg(
                    f"timeout on retry {attempt+1}/{_MAX_RETRIES}"
                )
                self._schedule_retry(attempt + 1)
                return

            except aiohttp.ClientError as exc:
                _LOGGER.warning(
                    "%s/%s catalog retry %d/%d error: %s — retrying",
                    __name__,
                    self._entry.entry_id,
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                )
                self._metrics.record_catalog_error_msg(
                    f"connection error on retry: {str(exc)[:150]}"
                )
                self._schedule_retry(attempt + 1)
                return

        # All chunks posted successfully on retry.
        _LOGGER.info(
            "%s/%s catalog push on retry %d/%d complete: %d total frames",
            __name__,
            self._entry.entry_id,
            attempt + 1,
            _MAX_RETRIES,
            len(frames),
        )
        self._schedule_next_inventory()
