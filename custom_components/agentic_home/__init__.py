"""Agentic Home integration for Home Assistant."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry

from .const import DOMAIN
from .heartbeat import HeartbeatGenerator
from .frame import SequenceCounter
from .metrics import RuntimeMetrics
from .pusher import IngressHTTPPusher
from .catalog_pusher import CatalogPusher
from .registry_pusher import RegistryPusher
from .subscriber import EventSubscriber

_LOGGER = logging.getLogger(__name__)

# Pipeline component keys stored under hass.data[DOMAIN][entry_id].
KEY_SEQUENCE_COUNTER = "sequence_counter"
KEY_METRICS = "metrics"
KEY_PUSHER = "pusher"
KEY_REGISTRY_PUSHER = "registry_pusher"
KEY_SUBSCRIBER = "subscriber"
KEY_HEARTBEAT_GEN = "heartbeat_gen"
KEY_CATALOG_PUSHER = "catalog_pusher"


# Manifest version is read once at import time — manifest.json is static for
# the process lifetime. Avoids a synchronous open() inside async_setup_entry.
def _get_manifest_version() -> str:
    """Return the integration version, cached at module load."""
    return _MANIFEST_VERSION


try:
    _manifest_path = Path(__file__).parent / "manifest.json"
    with open(_manifest_path, "r", encoding="utf-8") as _f:
        _MANIFEST_VERSION = json.load(_f).get("version", "unknown")
except Exception:  # noqa: BLE001
    _MANIFEST_VERSION = "unknown"


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Agentic Home from a config entry.

    Creates and starts the full event-ingestion pipeline:
    SequenceCounter → RuntimeMetrics → IngressHTTPPusher → EventSubscriber.
    All components are stored in hass.data[DOMAIN][entry.entry_id] so that
    async_unload_entry can cleanly shut them down.
    """
    hass.data.setdefault(DOMAIN, {})

    # 1. Core pipeline components.
    seq_counter = SequenceCounter()
    metrics = RuntimeMetrics()
    pusher = IngressHTTPPusher(hass=hass, entry=entry, metrics=metrics)
    registry_pusher = RegistryPusher(hass=hass, entry=entry, metrics=metrics)
    catalog_pusher = CatalogPusher(hass=hass, entry=entry, metrics=metrics)
    heartbeat_gen = HeartbeatGenerator(hass=hass, pusher=pusher, metrics=metrics, seq_counter=seq_counter)
    subscriber = EventSubscriber(
        hass=hass,
        entry=entry,
        pusher=pusher,
        metrics=metrics,
        seq_counter=seq_counter,
    )

    # 2. Store entry data copy and pipeline refs.
    entry_data: dict[str, Any] = dict(entry.data)
    entry_data[KEY_SEQUENCE_COUNTER] = seq_counter
    entry_data[KEY_METRICS] = metrics
    entry_data[KEY_PUSHER] = pusher
    entry_data[KEY_REGISTRY_PUSHER] = registry_pusher
    entry_data[KEY_CATALOG_PUSHER] = catalog_pusher
    entry_data[KEY_HEARTBEAT_GEN] = heartbeat_gen
    entry_data[KEY_SUBSCRIBER] = subscriber
    hass.data[DOMAIN][entry.entry_id] = entry_data

    # 3. Create device registry entry.
    integration_id: str = entry.data.get("integration_id", "")
    sw_version = _get_manifest_version()
    try:
        dev_reg = device_registry.async_get(hass)
        dev_reg.async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, integration_id)},
            name="Agentic Home",
            manufacturer="Agentic Home",
            model="HA Integration",
            sw_version=sw_version,
        )
    except Exception:  # noqa: BLE001
        # In test environments the mock may not support all DeviceRegistry internals.
        pass

    # 4. Forward entry setups to sensor and binary_sensor platforms.
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor", "binary_sensor"])

    # 5. Start the pipeline.
    pusher.start()
    subscriber.start()
    heartbeat_gen.start()

    # Start the registry pusher: immediate startup push then schedule periodic runs.
    await registry_pusher.push_snapshot()
    registry_pusher.schedule_next_inventory()

    # Start the catalog pusher: immediate startup push, subscribe to service_registered
    # events for incremental catalog updates, then schedule periodic runs.
    await catalog_pusher.push_catalog()
    catalog_pusher._unsub = hass.bus.async_listen(
        "service_registered", catalog_pusher._on_service_registered
    )
    catalog_pusher.schedule_next_inventory()

    _LOGGER.info(
        "AgenticHome(%s) pipeline started: integration_id=%s",
        entry.entry_id,
        integration_id,
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload an Agentic Home config entry.

    Stops the event subscriber (deregisters the HA bus listener) and drains
    the ingress pusher (flushes any pending frames) before removing the entry
    data from hass.data.
    """
    domain_data: dict[str, Any] = hass.data.get(DOMAIN, {})
    entry_data: dict[str, Any] = domain_data.get(entry.entry_id, {})

    # Unload sensor and binary_sensor platforms BEFORE pipeline teardown.
    await hass.config_entries.async_unload_platforms(entry, ["sensor", "binary_sensor"])

    # Stop heartbeat first (no more frames will be generated).
    heartbeat_gen: HeartbeatGenerator | None = entry_data.get(KEY_HEARTBEAT_GEN)
    if heartbeat_gen is not None:
        heartbeat_gen.stop()

    # Stop subscriber (no new frames will be forwarded).
    subscriber: EventSubscriber | None = entry_data.get(KEY_SUBSCRIBER)
    if subscriber is not None:
        subscriber.stop()

    # Stop the catalog pusher (cancel timers and drain inflight POST).
    catalog_pusher: CatalogPusher | None = entry_data.get(KEY_CATALOG_PUSHER)
    if catalog_pusher is not None:
        await catalog_pusher.stop()

    # Stop the registry pusher (cancel timers and drain inflight POST).
    registry_pusher: RegistryPusher | None = entry_data.get(KEY_REGISTRY_PUSHER)
    if registry_pusher is not None:
        await registry_pusher.stop()

    # Drain the pusher (flush any buffered frames before shutting down).
    pusher: IngressHTTPPusher | None = entry_data.get(KEY_PUSHER)
    if pusher is not None:
        await pusher.stop()

    # Remove entry data.
    hass.data[DOMAIN].pop(entry.entry_id, None)
    _LOGGER.debug("AgenticHome(%s) pipeline stopped and unloaded", entry.entry_id)
    return True