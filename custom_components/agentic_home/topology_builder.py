"""Topology builder: reads all 5 HA registries and assembles a RegistrySnapshotBody dict."""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    label_registry as lr,
)

from .const import CONF_INGRESS_URL, DOMAIN

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONF_EXCLUDE_ENTITIES = "exclude_entities"

# UUID v5 namespace for synthetic virtual device IDs.
# Every device-less entity gets a deterministic virtual device ID derived from its domain.
AGENTIC_HOME_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "agentic-home.virtual-device")

# Domains in which entities never have a device_id in the HA device registry.
# These 15 domains cover all device-less entity types in Home Assistant.
DEVICE_LESS_DOMAINS: frozenset[str] = frozenset(
    {
        "automation",
        "script",
        "input_boolean",
        "input_number",
        "input_text",
        "input_select",
        "input_datetime",
        "timer",
        "counter",
        "weather",
        "sun",
        "person",
        "zone",
        "image_processing",
        "calendar",
    }
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def virtual_device_id(domain: str) -> str:
    """Return a deterministic UUID v5 string for a domain's virtual device.

    Every entity in the same device-less domain maps to the same virtual device ID.
    Different domains produce different IDs.
    """
    return str(uuid.uuid5(AGENTIC_HOME_NAMESPACE, domain))


# ---------------------------------------------------------------------------
# Topology collection
# ---------------------------------------------------------------------------

async def collect_topology(hass: HomeAssistant, entry: ConfigEntry) -> dict[str, Any]:
    """Read all 5 HA registries and return a RegistrySnapshotBody-compatible dict.

    Args:
        hass:      HomeAssistant instance.
        entry:     ConfigEntry for this integration.

    Returns:
        A dict with keys: floors, areas, devices, entity_device_mappings,
        labels, label_assignments.  All timestamps are ISO 8601 UTC strings.
        household_id is NOT included in individual entries — it is carried
        in the JWT token and assigned by the platform on the server side.
    """
    now_iso = _iso_now()
    excluded_entities: set[str] = set(entry.options.get(CONF_EXCLUDE_ENTITIES, []))

    # ------------------------------------------------------------------
    # 1. Read registries
    # ------------------------------------------------------------------
    entity_reg = er.async_get(hass)
    device_reg = dr.async_get(hass)
    area_reg = ar.async_get(hass)
    label_reg = lr.async_get(hass)
    floor_reg = _get_floor_registry(hass)

    _LOGGER.debug(
        "Topology build starting: entry=%s entities=%d devices=%d areas=%d labels=%d floors=%d",
        entry.entry_id,
        len(entity_reg.entities),
        len(device_reg.devices),
        len(area_reg.areas),
        len(label_reg.labels),
        len(floor_reg.floors) if floor_reg else 0,
    )

    # ------------------------------------------------------------------
    # 2. Apply exclusion rules and build filtered entity list
    # ------------------------------------------------------------------
    # An entity is skipped if:
    #   - disabled_by is not None  OR
    #   - entity_id is in the excluded list
    surviving_entities: list[er.RegistryEntry] = []
    for entity in entity_reg.entities.values():
        if entity.disabled_by is not None:
            continue
        if entity.entity_id in excluded_entities:
            continue
        surviving_entities.append(entity)

    surviving_entity_ids: set[str] = {e.entity_id for e in surviving_entities}

    # ------------------------------------------------------------------
    # 3. Compute surviving devices
    #    A device survives if:
    #      - ANY non-excluded, non-disabled entity maps to it  OR
    #      - it has no entity mappings at all (true orphan)
    # ------------------------------------------------------------------
    entity_device_pairs: list[tuple[str, str]] = []  # (entity_id, device_id)
    for entity in surviving_entities:
        if entity.device_id is not None:
            entity_device_pairs.append((entity.entity_id, entity.device_id))

    device_ids_with_entities: set[str] = {dev_id for _, dev_id in entity_device_pairs}
    surviving_device_ids: set[str] = set(device_ids_with_entities)
    # Devices with zero entity mappings (true orphans) also survive.
    for dev_id, dev in device_reg.devices.items():
        if dev_id not in device_ids_with_entities:
            surviving_device_ids.add(dev_id)

    # ------------------------------------------------------------------
    # 4. Virtual devices for device-less entities
    #    For every surviving entity whose domain has no real devices,
    #    assign it to a deterministic synthetic virtual device so it
    #    flows through the classification pipeline.
    # ------------------------------------------------------------------
    virtual_devices: dict[str, dict[str, Any]] = {}
    for entity in surviving_entities:
        if entity.device_id is not None:
            continue
        domain = entity.entity_id.split(".", 1)[0]
        if domain not in DEVICE_LESS_DOMAINS:
            continue
        vid = virtual_device_id(domain)
        if vid not in virtual_devices:
            virtual_devices[vid] = {
                "id": vid,
                "name": f"Synthetic Device: {domain}",
                "area_id": None,
                "manufacturer": "agentic-home",
                "model": "virtual-device",
            }
        entity_device_pairs.append((entity.entity_id, vid))
        surviving_device_ids.add(vid)

    virtual_device_count = len(virtual_devices)
    virtual_entity_count = sum(
        1 for _, dev_id in entity_device_pairs if dev_id in virtual_devices
    )
    _LOGGER.debug(
        "Virtual devices: %d devices for %d device-less entities",
        virtual_device_count,
        virtual_entity_count,
    )

    # ------------------------------------------------------------------
    # 5. Compute surviving areas
    #    An area survives if it has at least one surviving device OR
    #    at least one surviving entity that references it directly.
    # ------------------------------------------------------------------
    area_ids_with_devices: set[str] = set()
    for dev in device_reg.devices.values():
        if dev.id in surviving_device_ids and dev.area_id is not None:
            area_ids_with_devices.add(dev.area_id)

    area_ids_with_entities: set[str] = set()
    for entity in surviving_entities:
        if entity.area_id is not None:
            area_ids_with_entities.add(entity.area_id)

    surviving_area_ids: set[str] = area_ids_with_devices | area_ids_with_entities
    # Areas with no devices also survive (they may be populated later).
    for area_id in area_reg.areas:
        surviving_area_ids.add(area_id)

    # ------------------------------------------------------------------
    # 6. Collect all labels referenced by surviving entities/devices/areas
    # ------------------------------------------------------------------
    label_ids_referenced: set[str] = set()
    for entity in surviving_entities:
        for label in entity.labels:
            label_ids_referenced.add(label)

    for dev in device_reg.devices.values():
        if dev.id in surviving_device_ids:
            for label in dev.labels:
                label_ids_referenced.add(label)

    for area_id in area_reg.areas:
        area = area_reg.areas[area_id]
        if area.id in surviving_area_ids:
            for label in area.labels:
                label_ids_referenced.add(label)

    # ------------------------------------------------------------------
    # 7. Build the 6 arrays
    # ------------------------------------------------------------------
    # --- floors ---
    floors: list[dict[str, Any]] = []
    if floor_reg is not None:
        for floor in floor_reg.floors.values():
            floors.append(
                {
                    "id": str(floor.floor_id),
                    "name": floor.name,
                    "level": floor.level,
                    "platform_created_at": now_iso,
                    "platform_updated_at": now_iso,
                }
            )

    # --- areas ---
    areas: list[dict[str, Any]] = []
    for area in area_reg.areas.values():
        if area.id not in surviving_area_ids:
            continue
        area_dict: dict[str, Any] = {
            "id": str(area.id),
            "floor_id": str(area.floor_id) if area.floor_id else None,
            "name": area.name,
            "icon": area.icon,
            "platform_created_at": now_iso,
            "platform_updated_at": now_iso,
        }
        areas.append(area_dict)

    # --- devices (physical) ---
    devices: list[dict[str, Any]] = []
    for dev in device_reg.devices.values():
        if dev.id not in surviving_device_ids:
            continue
        device_name = dev.name_by_user if dev.name_by_user else dev.name
        devices.append(
            {
                "id": str(dev.id),
                "area_id": str(dev.area_id) if dev.area_id else None,
                "name": device_name or "",
                "manufacturer": dev.manufacturer,
                "model": dev.model,
                "platform_created_at": now_iso,
                "platform_updated_at": now_iso,
            }
        )

    # --- devices (virtual) ---
    for vdev in virtual_devices.values():
        devices.append(
            {
                "id": vdev["id"],
                "area_id": None,
                "name": vdev["name"],
                "manufacturer": vdev["manufacturer"],
                "model": vdev["model"],
                "platform_created_at": now_iso,
                "platform_updated_at": now_iso,
            }
        )

    # --- entity_device_mappings (device-backed + virtual) ---
    entity_device_mappings: list[dict[str, Any]] = []
    for entity_id, device_id in entity_device_pairs:
        domain = entity_id.split(".", 1)[0]
        device_class = _get_device_class(hass, entity_id)
        entity_device_mappings.append(
            {
                "entity_id": entity_id,
                "device_id": str(device_id),
                "platform_created_at": now_iso,
                "domain": domain,
                "device_class": device_class,
            }
        )

    # --- labels (deduplicated, only referenced) ---
    labels: list[dict[str, Any]] = []
    label_ids_seen: set[str] = set()
    for label_id in label_ids_referenced:
        if label_id in label_ids_seen:
            continue
        label_ids_seen.add(label_id)
        label_obj = label_reg.labels.get(label_id)
        if label_obj is None:
            continue
        labels.append(
            {
                "id": str(label_obj.label_id),
                "name": label_obj.name,
                "color": label_obj.color,
                "created_at": now_iso,
            }
        )

    # --- label_assignments ---
    label_assignments: list[dict[str, Any]] = []
    for entity in surviving_entities:
        for label_id in entity.labels:
            label_assignments.append(
                {
                    "label_id": str(label_id),
                    "target_id": entity.entity_id,
                    "target": "entity",
                    "created_at": now_iso,
                }
            )
    for dev in device_reg.devices.values():
        if dev.id not in surviving_device_ids:
            continue
        for label_id in dev.labels:
            label_assignments.append(
                {
                    "label_id": str(label_id),
                    "target_id": str(dev.id),
                    "target": "device",
                    "created_at": now_iso,
                }
            )
    for area in area_reg.areas.values():
        if area.id not in surviving_area_ids:
            continue
        for label_id in area.labels:
            label_assignments.append(
                {
                    "label_id": str(label_id),
                    "target_id": str(area.id),
                    "target": "area",
                    "created_at": now_iso,
                }
            )

    snapshot: dict[str, Any] = {
        "timezone": hass.config.time_zone,
        "floors": floors,
        "areas": areas,
        "devices": devices,
        "entity_device_mappings": entity_device_mappings,
        "labels": labels,
        "label_assignments": label_assignments,
    }

    _LOGGER.info(
        "Topology built: floors=%d areas=%d devices=%d "
        "entities=%d labels=%d assignments=%d",
        len(floors),
        len(areas),
        len(devices),
        len(surviving_entity_ids),
        len(labels),
        len(label_assignments),
    )

    return snapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_floor_registry(hass: HomeAssistant) -> Any | None:
    """Attempt to load the floor registry; return None if HA version doesn't have it."""
    try:
        # pylint: disable=import-error,import-outside-toplevel
        from homeassistant.helpers import floor_registry as fr

        return fr.async_get(hass)
    except ImportError:
        _LOGGER.debug("Floor registry not available in this HA version")
        return None


def _iso_now() -> str:
    """Return current UTC time as ISO 8601 string."""
    return _fmt_time(time.time())


def _get_device_class(hass: HomeAssistant, entity_id: str) -> str:
    """Extract device_class from HA state attributes, or empty string if unavailable.

    Only sensor and binary_sensor domains typically carry device_class in
    their state attributes. Returns empty string for other domains or when
    the state is unavailable.
    """
    state = hass.states.get(entity_id)
    if state is None or state.attributes is None:
        return ""
    return str(state.attributes.get("device_class", ""))


def _fmt_time(unix_ts: float) -> str:
    """Format a Unix timestamp as an ISO 8601 UTC string."""
    import datetime

    return datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )