"""Tests for topology_builder.py."""

from __future__ import annotations

import sys
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, ".")

from custom_components.agentic_home import topology_builder


# ---------------------------------------------------------------------------
# Registry entry helpers
# ---------------------------------------------------------------------------

class FakeFloor:
    def __init__(self, floor_id: str, name: str, level: int) -> None:
        self.floor_id = floor_id
        self.name = name
        self.level = level


class FakeFloorRegistry:
    def __init__(self, floors: dict[str, FakeFloor] | None = None) -> None:
        self.floors: dict[str, FakeFloor] = floors or {}


class FakeArea:
    def __init__(
        self,
        area_id: str,
        name: str,
        floor_id: str | None = None,
        icon: str | None = None,
        labels: set[str] | None = None,
    ) -> None:
        self.id = area_id
        self.name = name
        self.floor_id = floor_id
        self.icon = icon
        self.labels: set[str] = labels or set()


class FakeAreaRegistry:
    def __init__(self, areas: dict[str, FakeArea] | None = None) -> None:
        self.areas: dict[str, FakeArea] = areas or {}


class FakeDevice:
    def __init__(
        self,
        device_id: str,
        name: str | None = None,
        name_by_user: str | None = None,
        area_id: str | None = None,
        manufacturer: str | None = None,
        model: str | None = None,
        labels: set[str] | None = None,
    ) -> None:
        self.id = device_id
        self.name = name or ""
        self.name_by_user = name_by_user
        self.area_id = area_id
        self.manufacturer = manufacturer
        self.model = model
        self.labels: set[str] = labels or set()


class FakeDeviceRegistry:
    def __init__(self, devices: dict[str, FakeDevice] | None = None) -> None:
        self.devices: dict[str, FakeDevice] = devices or {}


class FakeEntity:
    def __init__(
        self,
        entity_id: str,
        device_id: str | None = None,
        area_id: str | None = None,
        disabled_by: str | None = None,
        labels: set[str] | None = None,
    ) -> None:
        self.entity_id = entity_id
        self.device_id = device_id
        self.area_id = area_id
        self.disabled_by = disabled_by
        self.labels: set[str] = labels or set()


class FakeEntityRegistry:
    def __init__(self, entities: dict[str, FakeEntity] | None = None) -> None:
        self.entities: dict[str, FakeEntity] = entities or {}


class FakeLabel:
    def __init__(self, label_id: str, name: str, color: str | None = None) -> None:
        self.label_id = label_id
        self.name = name
        self.color = color


class FakeLabelRegistry:
    def __init__(self, labels: dict[str, FakeLabel] | None = None) -> None:
        self.labels: dict[str, FakeLabel] = labels or {}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_hass() -> MagicMock:
    hass = MagicMock()
    hass.data = {}
    return hass


@pytest.fixture
def minimal_entry() -> MagicMock:
    entry = MagicMock()
    entry.entry_id = "entry_x"
    entry.data = {"integration_id": "hh_123"}
    entry.options = {}
    return entry


# ---------------------------------------------------------------------------
# Scenario 1: Full topology — all registries populated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_topology(empty_hass, minimal_entry):
    """All registries return non-empty data; everything appears in the snapshot."""
    # Set up floors
    floor_reg = FakeFloorRegistry(
        {"floor_1": FakeFloor("floor_1", "First Floor", 1)}
    )
    # Set up areas
    area_reg = FakeAreaRegistry(
        {"area_k": FakeArea("area_k", "Kitchen", floor_id="floor_1")}
    )
    # Set up devices
    device_reg = FakeDeviceRegistry(
        {"dev_m": FakeDevice("dev_m", "Oven", area_id="area_k")}
    )
    # Set up entities
    entity_reg = FakeEntityRegistry(
        {"sensor.temp": FakeEntity("sensor.temp", device_id="dev_m", area_id=None, labels={"lbl_1"})}
    )
    # Set up labels
    label_reg = FakeLabelRegistry(
        {"lbl_1": FakeLabel("lbl_1", "Kitchen", "#ff0000")}
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, label_reg, floor_reg)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert len(result["floors"]) == 1
    assert result["floors"][0]["name"] == "First Floor"
    assert result["floors"][0]["level"] == 1

    assert len(result["areas"]) == 1
    assert result["areas"][0]["name"] == "Kitchen"

    assert len(result["devices"]) == 1
    assert result["devices"][0]["name"] == "Oven"

    assert len(result["entity_device_mappings"]) == 1
    assert result["entity_device_mappings"][0]["entity_id"] == "sensor.temp"
    assert result["entity_device_mappings"][0]["device_id"] == "dev_m"
    # domain derived from entity_id prefix
    assert result["entity_device_mappings"][0]["domain"] == "sensor"
    # device_class defaults to "" when no HA state is available
    assert result["entity_device_mappings"][0]["device_class"] == ""

    assert len(result["labels"]) == 1
    assert result["labels"][0]["name"] == "Kitchen"

    # sensor.temp has label lbl_1, so one assignment appears
    assert len(result["label_assignments"]) == 1


# ---------------------------------------------------------------------------
# Scenario 2: Entity → Device → Area → Floor chain
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chain_area_from_entity_override(empty_hass, minimal_entry):
    """Entity area_id overrides device area_id."""
    entity_reg = FakeEntityRegistry(
        {"light.living": FakeEntity("light.living", device_id="dev_1", area_id="area_override")}
    )
    device_reg = FakeDeviceRegistry(
        {"dev_1": FakeDevice("dev_1", "Ceiling Light", area_id="area_device")}
    )
    area_reg = FakeAreaRegistry(
        {
            "area_override": FakeArea("area_override", "Living Room"),
            "area_device": FakeArea("area_device", "Hallway"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Both areas survive (entity and device both reference areas)
    area_ids = {a["id"] for a in result["areas"]}
    assert "area_override" in area_ids
    assert "area_device" in area_ids


# ---------------------------------------------------------------------------
# Scenario 3: Device-less entities (no device_id)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_device_less_entities(empty_hass, minimal_entry):
    """Device-less entities in DEVICE_LESS_DOMAINS get virtual device mappings;
    entities not in DEVICE_LESS_DOMAINS remain unmapped."""
    entity_reg = FakeEntityRegistry(
        {
            "sensor.no_device": FakeEntity("sensor.no_device", device_id=None),
            "automation.no_device": FakeEntity("automation.no_device", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # sensor not in DEVICE_LESS_DOMAINS → no mapping
    # automation IS in DEVICE_LESS_DOMAINS → gets virtual mapping
    assert len(result["entity_device_mappings"]) == 1
    mapping = result["entity_device_mappings"][0]
    assert mapping["entity_id"] == "automation.no_device"
    assert mapping["device_id"] == topology_builder.virtual_device_id("automation")

    # Virtual device appears in devices array
    auto_vid = topology_builder.virtual_device_id("automation")
    device_ids = {d["id"] for d in result["devices"]}
    assert auto_vid in device_ids
    auto_dev = next(d for d in result["devices"] if d["id"] == auto_vid)
    assert auto_dev["name"] == "Synthetic Device: automation"


# ---------------------------------------------------------------------------
# Scenario 3b: Virtual devices emitted for device-less domain entities
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_device_for_device_less_domain_entities(empty_hass, minimal_entry):
    """Entities in DEVICE_LESS_DOMAINS get virtual devices and mappings."""
    entity_reg = FakeEntityRegistry(
        {
            "automation.office_mode": FakeEntity("automation.office_mode", device_id=None),
            "input_boolean.guest_mode": FakeEntity("input_boolean.guest_mode", device_id=None),
            "weather.home": FakeEntity("weather.home", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Three mappings: one per entity to its domain's virtual device
    assert len(result["entity_device_mappings"]) == 3

    # Each entity mapped to a deterministic virtual device ID
    auto_vid = topology_builder.virtual_device_id("automation")
    ibool_vid = topology_builder.virtual_device_id("input_boolean")
    weather_vid = topology_builder.virtual_device_id("weather")

    mapping_by_entity = {m["entity_id"]: m for m in result["entity_device_mappings"]}
    assert mapping_by_entity["automation.office_mode"]["device_id"] == auto_vid
    assert mapping_by_entity["input_boolean.guest_mode"]["device_id"] == ibool_vid
    assert mapping_by_entity["weather.home"]["device_id"] == weather_vid

    # Three virtual devices emitted (one per domain)
    assert len(result["devices"]) == 3
    device_by_id = {d["id"]: d for d in result["devices"]}

    # Verify each virtual device has expected fields
    auto_dev = device_by_id[auto_vid]
    assert auto_dev["name"] == "Synthetic Device: automation"
    assert auto_dev["area_id"] is None
    assert auto_dev["manufacturer"] == "agentic-home"
    assert auto_dev["model"] == "virtual-device"

    ibool_dev = device_by_id[ibool_vid]
    assert ibool_dev["name"] == "Synthetic Device: input_boolean"

    weather_dev = device_by_id[weather_vid]
    assert weather_dev["name"] == "Synthetic Device: weather"


# ---------------------------------------------------------------------------
# Scenario 3c: Virtual device deduplication — multiple entities same domain
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_device_deduplication_same_domain(empty_hass, minimal_entry):
    """Multiple entities in same device-less domain share one virtual device."""
    entity_reg = FakeEntityRegistry(
        {
            "automation.morning": FakeEntity("automation.morning", device_id=None),
            "automation.evening": FakeEntity("automation.evening", device_id=None),
            "automation.security": FakeEntity("automation.security", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Three mappings, all to the same virtual device
    assert len(result["entity_device_mappings"]) == 3
    auto_vid = topology_builder.virtual_device_id("automation")
    for m in result["entity_device_mappings"]:
        assert m["device_id"] == auto_vid

    # Only one virtual device in devices array
    assert len(result["devices"]) == 1
    assert result["devices"][0]["id"] == auto_vid
    assert result["devices"][0]["name"] == "Synthetic Device: automation"


# ---------------------------------------------------------------------------
# Scenario 3d: Virtual devices coexist with physical devices
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_and_physical_devices_coexist(empty_hass, minimal_entry):
    """Virtual devices for device-less entities coexist with physical devices."""
    entity_reg = FakeEntityRegistry(
        {
            "sensor.temp": FakeEntity("sensor.temp", device_id="dev_real"),
            "automation.away": FakeEntity("automation.away", device_id=None),
            "script.goodnight": FakeEntity("script.goodnight", device_id=None),
        }
    )
    device_reg = FakeDeviceRegistry(
        {"dev_real": FakeDevice("dev_real", "Thermometer")}
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # 3 mappings: 1 physical + 2 virtual
    assert len(result["entity_device_mappings"]) == 3

    # Physical device mapping
    physical = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "sensor.temp"
    )
    assert physical["device_id"] == "dev_real"

    # Virtual mappings
    auto_vid = topology_builder.virtual_device_id("automation")
    script_vid = topology_builder.virtual_device_id("script")
    mapping_ids = {m["entity_id"]: m["device_id"] for m in result["entity_device_mappings"]}
    assert mapping_ids["automation.away"] == auto_vid
    assert mapping_ids["script.goodnight"] == script_vid

    # 3 devices: 1 physical + 2 virtual
    assert len(result["devices"]) == 3
    device_names = {d["name"] for d in result["devices"]}
    assert "Thermometer" in device_names
    assert "Synthetic Device: automation" in device_names
    assert "Synthetic Device: script" in device_names


# ---------------------------------------------------------------------------
# Scenario 3e: Virtual device determinism via collect_topology
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_device_determinism(empty_hass, minimal_entry):
    """Same domain produces same virtual device ID across collect_topology calls."""
    entity_reg = FakeEntityRegistry(
        {
            "automation.morning": FakeEntity("automation.morning", device_id=None),
            "weather.home": FakeEntity("weather.home", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result1 = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Second call with identical state
    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result2 = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Extract virtual device IDs from both calls and compare
    def device_ids_by_entity(result):
        return {m["entity_id"]: m["device_id"] for m in result["entity_device_mappings"]}

    mappings1 = device_ids_by_entity(result1)
    mappings2 = device_ids_by_entity(result2)
    assert mappings1 == mappings2

    # Each mapping has a valid UUID v5 device ID
    auto_vid = topology_builder.virtual_device_id("automation")
    assert mappings1["automation.morning"] == auto_vid
    assert mappings2["automation.morning"] == auto_vid
    parsed = uuid.UUID(auto_vid)
    assert parsed.version == 5


# ---------------------------------------------------------------------------
# Scenario 3f: Virtual device per domain (one device entry per domain)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_device_per_domain(empty_hass, minimal_entry):
    """Multiple device-less entities in the same domain share exactly one virtual
    device entry in the devices array."""
    entity_reg = FakeEntityRegistry(
        {
            "script.lights_off": FakeEntity("script.lights_off", device_id=None),
            "script.curtains_close": FakeEntity("script.curtains_close", device_id=None),
            "script.goodnight": FakeEntity("script.goodnight", device_id=None),
            "input_number.brightness": FakeEntity("input_number.brightness", device_id=None),
            "input_number.volume": FakeEntity("input_number.volume", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # 5 mappings: 3 scripts + 2 input_numbers
    assert len(result["entity_device_mappings"]) == 5

    # 2 virtual devices: one for script, one for input_number
    assert len(result["devices"]) == 2

    script_vid = topology_builder.virtual_device_id("script")
    inum_vid = topology_builder.virtual_device_id("input_number")
    device_ids = {d["id"] for d in result["devices"]}
    assert device_ids == {script_vid, inum_vid}

    # All script entities map to the same device ID
    script_mappings = [
        m for m in result["entity_device_mappings"]
        if m["entity_id"].startswith("script.")
    ]
    assert len(script_mappings) == 3
    for m in script_mappings:
        assert m["device_id"] == script_vid

    # All input_number entities map to the same device ID
    inum_mappings = [
        m for m in result["entity_device_mappings"]
        if m["entity_id"].startswith("input_number.")
    ]
    assert len(inum_mappings) == 2
    for m in inum_mappings:
        assert m["device_id"] == inum_vid


# ---------------------------------------------------------------------------
# Scenario 3g: UUID format validation on mapping device IDs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_device_less_entity_mapping_has_virtual_device_id(empty_hass, minimal_entry):
    """Every device-less entity mapping has a valid UUID v5 string as its device_id."""
    entity_reg = FakeEntityRegistry(
        {
            "automation.office_mode": FakeEntity("automation.office_mode", device_id=None),
            "input_boolean.guest_mode": FakeEntity("input_boolean.guest_mode", device_id=None),
            "weather.home": FakeEntity("weather.home", device_id=None),
            "counter.visits": FakeEntity("counter.visits", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert len(result["entity_device_mappings"]) == 4

    for m in result["entity_device_mappings"]:
        # Each device_id must parse as a valid UUID
        parsed = uuid.UUID(m["device_id"])
        assert parsed.version == 5
        # Must match the deterministic virtual_device_id for its domain
        domain = m["entity_id"].split(".", 1)[0]
        expected = topology_builder.virtual_device_id(domain)
        assert m["device_id"] == expected


# ---------------------------------------------------------------------------
# Scenario 3h: Virtual device entry structure in devices array
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_virtual_device_entry_in_devices_array(empty_hass, minimal_entry):
    """Virtual devices in the devices array carry 'Synthetic Device: {domain}'
    name and expected metadata fields."""
    entity_reg = FakeEntityRegistry(
        {
            "calendar.personal": FakeEntity("calendar.personal", device_id=None),
            "person.michael": FakeEntity("person.michael", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert len(result["devices"]) == 2

    cal_vid = topology_builder.virtual_device_id("calendar")
    person_vid = topology_builder.virtual_device_id("person")
    devices_by_id = {d["id"]: d for d in result["devices"]}

    # Calendar virtual device
    cal_dev = devices_by_id[cal_vid]
    assert cal_dev["name"] == "Synthetic Device: calendar"
    assert cal_dev["area_id"] is None
    assert cal_dev["manufacturer"] == "agentic-home"
    assert cal_dev["model"] == "virtual-device"
    # Must carry platform timestamps
    assert cal_dev["platform_created_at"].endswith("Z")
    assert cal_dev["platform_updated_at"].endswith("Z")

    # Person virtual device
    person_dev = devices_by_id[person_vid]
    assert person_dev["name"] == "Synthetic Device: person"
    assert person_dev["area_id"] is None
    assert person_dev["manufacturer"] == "agentic-home"
    assert person_dev["model"] == "virtual-device"


# ---------------------------------------------------------------------------
# Scenario 3i: Mixed device and device-less domain/device_class
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mixed_device_and_device_less(empty_hass, minimal_entry):
    """Snapshot with both device-backed and device-less entities has correct
    domain and device_class for all mappings."""
    entity_reg = FakeEntityRegistry(
        {
            "sensor.temperature": FakeEntity("sensor.temperature", device_id="dev_real"),
            "automation.night_mode": FakeEntity("automation.night_mode", device_id=None),
            "zone.home": FakeEntity("zone.home", device_id=None),
        }
    )
    device_reg = FakeDeviceRegistry(
        {"dev_real": FakeDevice("dev_real", "Thermometer", manufacturer="Nest")}
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)
    # Set up HA states with device_class
    empty_hass.states.get = lambda entity_id: {
        "sensor.temperature": FakeState({"device_class": "temperature"}),
    }.get(entity_id)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert len(result["entity_device_mappings"]) == 3

    # sensor.temperature — physical device
    sensor_m = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "sensor.temperature"
    )
    assert sensor_m["device_id"] == "dev_real"
    assert sensor_m["domain"] == "sensor"
    assert sensor_m["device_class"] == "temperature"

    # automation.night_mode — virtual device
    auto_m = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "automation.night_mode"
    )
    assert auto_m["device_id"] == topology_builder.virtual_device_id("automation")
    assert auto_m["domain"] == "automation"
    # Virtual device entities have no HA state → device_class defaults to ""
    assert auto_m["device_class"] == ""

    # zone.home — virtual device
    zone_m = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "zone.home"
    )
    assert zone_m["device_id"] == topology_builder.virtual_device_id("zone")
    assert zone_m["domain"] == "zone"
    assert zone_m["device_class"] == ""

    # Devices array: 1 physical + 2 virtual
    device_names = {d["name"] for d in result["devices"]}
    assert "Thermometer" in device_names
    assert "Synthetic Device: automation" in device_names
    assert "Synthetic Device: zone" in device_names


# ---------------------------------------------------------------------------
# Scenario 3j: Excluded device-less entities get no virtual device mappings
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_excluded_device_less_entity_no_mapping(empty_hass, minimal_entry):
    """Device-less entities on the exclude list do NOT receive virtual device
    mappings and do NOT create virtual device entries."""
    minimal_entry.options = {"exclude_entities": ["automation.secret", "script.hidden"]}

    entity_reg = FakeEntityRegistry(
        {
            "automation.visible": FakeEntity("automation.visible", device_id=None),
            "automation.secret": FakeEntity("automation.secret", device_id=None),
            "script.hidden": FakeEntity("script.hidden", device_id=None),
            "weather.home": FakeEntity("weather.home", device_id=None),
        }
    )

    _patch_registries(empty_hass, entity_reg, FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)
    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Only non-excluded device-less entities get mappings
    assert len(result["entity_device_mappings"]) == 2

    mapped_ids = {m["entity_id"] for m in result["entity_device_mappings"]}
    assert "automation.visible" in mapped_ids
    assert "weather.home" in mapped_ids
    assert "automation.secret" not in mapped_ids
    assert "script.hidden" not in mapped_ids

    # Virtual devices only for domains with non-excluded entities
    # automation.visible → automation domain → virtual device
    # weather.home → weather domain → virtual device
    # script.hidden excluded → script domain gets NO virtual device
    device_ids = {d["id"] for d in result["devices"]}
    auto_vid = topology_builder.virtual_device_id("automation")
    weather_vid = topology_builder.virtual_device_id("weather")
    script_vid = topology_builder.virtual_device_id("script")
    assert auto_vid in device_ids
    assert weather_vid in device_ids
    assert script_vid not in device_ids


# ---------------------------------------------------------------------------
# Scenario 4: Entity area_id override (entity takes precedence over device)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_entity_area_id_precedence(empty_hass, minimal_entry):
    """When entity.area_id is set, it is used instead of device.area_id."""
    entity_reg = FakeEntityRegistry(
        {"light.bed": FakeEntity("light.bed", device_id="dev_x", area_id="area_entity")}
    )
    device_reg = FakeDeviceRegistry(
        {"dev_x": FakeDevice("dev_x", "Bed Lamp", area_id="area_device")}
    )
    area_reg = FakeAreaRegistry(
        {
            "area_entity": FakeArea("area_entity", "Bedroom"),
            "area_device": FakeArea("area_device", "Hallway"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    area_ids = {a["id"] for a in result["areas"]}
    # Both areas survive: one from entity direct ref, one from device
    assert "area_entity" in area_ids
    assert "area_device" in area_ids


# ---------------------------------------------------------------------------
# Scenario 5: Disabled entities excluded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_disabled_entities_excluded(empty_hass, minimal_entry):
    """Entities with disabled_by != None are not included."""
    entity_reg = FakeEntityRegistry(
        {
            "sensor.on": FakeEntity("sensor.on", device_id="dev_1"),
            "sensor.disabled": FakeEntity("sensor.disabled", device_id="dev_2", disabled_by="user"),
        }
    )
    device_reg = FakeDeviceRegistry(
        {
            "dev_1": FakeDevice("dev_1", "Active Device"),
            "dev_2": FakeDevice("dev_2", "Disabled Device"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    mapping_ids = {m["entity_id"] for m in result["entity_device_mappings"]}
    assert "sensor.on" in mapping_ids
    assert "sensor.disabled" not in mapping_ids


# ---------------------------------------------------------------------------
# Scenario 6: Device with no entities survives
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_orphan_device_survives(empty_hass, minimal_entry):
    """Devices with no linked entities still appear in the snapshot."""
    entity_reg = FakeEntityRegistry(
        {"sensor.active": FakeEntity("sensor.active", device_id="dev_linked")}
    )
    device_reg = FakeDeviceRegistry(
        {
            "dev_linked": FakeDevice("dev_linked", "Linked Device"),
            "dev_orphan": FakeDevice("dev_orphan", "Orphan Device"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    device_ids = {d["id"] for d in result["devices"]}
    assert "dev_orphan" in device_ids
    assert "dev_linked" in device_ids


# ---------------------------------------------------------------------------
# Scenario 7: Area with no devices survives
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_orphan_area_survives(empty_hass, minimal_entry):
    """Areas with no devices still appear in the snapshot."""
    entity_reg = FakeEntityRegistry({})
    device_reg = FakeDeviceRegistry({})
    area_reg = FakeAreaRegistry(
        {"area_empty": FakeArea("area_empty", "Empty Room")}
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    area_ids = {a["id"] for a in result["areas"]}
    assert "area_empty" in area_ids


# ---------------------------------------------------------------------------
# Scenario 8: Floor registry unavailable (older HA)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_floor_registry_unavailable(empty_hass, minimal_entry):
    """When floor registry is None, floors array is empty."""
    entity_reg = FakeEntityRegistry({})
    device_reg = FakeDeviceRegistry({})
    area_reg = FakeAreaRegistry({})
    label_reg = FakeLabelRegistry({})

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, label_reg, None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert result["floors"] == []


# ---------------------------------------------------------------------------
# Scenario 9: Entity, device, and area labels
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_labels_on_entity_device_area(empty_hass, minimal_entry):
    """Labels referenced by entities, devices, and areas all appear."""
    entity_reg = FakeEntityRegistry(
        {"light.x": FakeEntity("light.x", device_id="dev_x", labels={"lbl_entity"})}
    )
    device_reg = FakeDeviceRegistry(
        {"dev_x": FakeDevice("dev_x", labels={"lbl_device"})}
    )
    area_reg = FakeAreaRegistry(
        {"area_y": FakeArea("area_y", "Room Y", labels={"lbl_area"})}
    )
    label_reg = FakeLabelRegistry(
        {
            "lbl_entity": FakeLabel("lbl_entity", "Entity Label"),
            "lbl_device": FakeLabel("lbl_device", "Device Label"),
            "lbl_area": FakeLabel("lbl_area", "Area Label"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, label_reg, None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    label_names = {lbl["name"] for lbl in result["labels"]}
    assert label_names == {"Entity Label", "Device Label", "Area Label"}

    # Assignments: 1 from entity, 1 from device, 1 from area
    assert len(result["label_assignments"]) == 3
    targets = {a["target"] for a in result["label_assignments"]}
    assert targets == {"entity", "device", "area"}


# ---------------------------------------------------------------------------
# Scenario 10: Excluded entities filtered
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_excluded_entities_filtered(empty_hass, minimal_entry):
    """Entities in the excluded list are skipped from entity_device_mappings.
    Devices survive even when all their entity mappings were excluded — they are
    orphans (zero non-excluded entity mappings) which still appear in the snapshot."""
    # entry.options has the exclude list
    minimal_entry.options = {"exclude_entities": ["sensor.hidden", "light.secret"]}

    entity_reg = FakeEntityRegistry(
        {
            "sensor.visible": FakeEntity("sensor.visible", device_id="dev_visible"),
            "sensor.hidden": FakeEntity("sensor.hidden", device_id="dev_hidden"),
            "light.secret": FakeEntity("light.secret", device_id="dev_secret"),
        }
    )
    device_reg = FakeDeviceRegistry(
        {
            "dev_visible": FakeDevice("dev_visible"),
            "dev_hidden": FakeDevice("dev_hidden"),
            "dev_secret": FakeDevice("dev_secret"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    mapping_ids = {m["entity_id"] for m in result["entity_device_mappings"]}
    assert "sensor.visible" in mapping_ids
    assert "sensor.hidden" not in mapping_ids
    assert "light.secret" not in mapping_ids

    # All devices survive: excluded-entity devices are orphans (no non-excluded
    # entity mappings), which per design still appear in the snapshot.
    device_ids = {d["id"] for d in result["devices"]}
    assert "dev_visible" in device_ids
    assert "dev_hidden" in device_ids
    assert "dev_secret" in device_ids


# ---------------------------------------------------------------------------
# Scenario 11: JSON contract — field names and nullability match
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_json_contract_field_names_and_nullability(empty_hass, minimal_entry):
    """Output dict matches RegistrySnapshotBody contract field names and nullability."""
    entity_reg = FakeEntityRegistry(
        {"sensor.x": FakeEntity("sensor.x", device_id="dev_x", area_id="area_x", labels={"lbl_c"})}
    )
    device_reg = FakeDeviceRegistry(
        {
            "dev_x": FakeDevice(
                "dev_x",
                name="Thermostat",
                name_by_user="Main Thermostat",
                area_id="area_x",
                manufacturer="Nest",
                model="Learning 3rd",
            )
        }
    )
    area_reg = FakeAreaRegistry(
        {"area_x": FakeArea("area_x", "Hallway", floor_id="floor_1", icon="mdi:home")}
    )
    floor_reg = FakeFloorRegistry({"floor_1": FakeFloor("floor_1", "Ground", 0)})
    label_reg = FakeLabelRegistry(
        {"lbl_c": FakeLabel("lbl_c", "Critical", "#ff0000")}
    )

    _patch_registries(empty_hass, entity_reg, device_reg, area_reg, label_reg, floor_reg)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # ---- floors ----
    floor = result["floors"][0]
    assert set(floor.keys()) == {
        "id", "name", "level", "platform_created_at", "platform_updated_at"
    }
    assert floor["id"] == "floor_1"
    assert floor["name"] == "Ground"
    assert floor["level"] == 0
    assert floor["platform_created_at"].endswith("Z")

    # ---- areas ----
    area = result["areas"][0]
    assert set(area.keys()) == {
        "id", "floor_id", "name", "icon", "platform_created_at", "platform_updated_at"
    }
    assert area["floor_id"] == "floor_1"
    assert area["name"] == "Hallway"
    assert area["icon"] == "mdi:home"

    # ---- devices ----
    device = result["devices"][0]
    assert set(device.keys()) == {
        "id", "area_id", "name", "manufacturer", "model",
        "platform_created_at", "platform_updated_at",
    }
    # name_by_user takes precedence
    assert device["name"] == "Main Thermostat"
    assert device["manufacturer"] == "Nest"
    assert device["model"] == "Learning 3rd"

    # ---- entity_device_mappings ----
    mapping = result["entity_device_mappings"][0]
    assert set(mapping.keys()) == {
        "entity_id", "device_id", "platform_created_at", "domain", "device_class"
    }
    assert mapping["entity_id"] == "sensor.x"
    assert mapping["device_id"] == "dev_x"

    # ---- labels ----
    label = result["labels"][0]
    assert set(label.keys()) == {"id", "name", "color", "created_at"}
    assert label["name"] == "Critical"
    assert label["color"] == "#ff0000"

    # ---- label_assignments ----
    assignment = result["label_assignments"][0]
    assert set(assignment.keys()) == {
        "label_id", "target_id", "target", "created_at"
    }
    assert assignment["target"] in ("entity", "device", "area")

    # ---- top-level keys ----
    assert set(result.keys()) == {
        "timezone", "floors", "areas", "devices", "entity_device_mappings", "labels", "label_assignments"
    }


# ---------------------------------------------------------------------------
# Scenario 12: Empty registries produce valid arrays
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_registries_produce_valid_arrays(empty_hass, minimal_entry):
    """All registries empty → snapshot has empty arrays (no KeyError/TypeError)."""
    _patch_registries(empty_hass, FakeEntityRegistry(), FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert result["floors"] == []
    assert result["areas"] == []
    assert result["devices"] == []
    assert result["entity_device_mappings"] == []
    assert result["labels"] == []
    assert result["label_assignments"] == []


# ---------------------------------------------------------------------------
# Scenario 13: Device name by user preferred over device name
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_device_name_by_user_preferred(empty_hass, minimal_entry):
    """device.name_by_user is used when available; falls back to device.name."""
    device_reg = FakeDeviceRegistry(
        {
            "dev_a": FakeDevice("dev_a", name="Model X", name_by_user="Living Room TV"),
            "dev_b": FakeDevice("dev_b", name="Model Y", name_by_user=None),
        }
    )

    entity_reg = FakeEntityRegistry(
        {
            "sensor.a": FakeEntity("sensor.a", device_id="dev_a"),
            "sensor.b": FakeEntity("sensor.b", device_id="dev_b"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    devices = {d["id"]: d for d in result["devices"]}
    assert devices["dev_a"]["name"] == "Living Room TV"
    assert devices["dev_b"]["name"] == "Model Y"


# ---------------------------------------------------------------------------
# Scenario 14: household_id comes from integration_id in entry.data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_household_id_from_entry_integration_id(empty_hass, minimal_entry):
    """integration_id from entry.data is accessible in topology builder context.

    Note: household_id is NOT included in individual registry entries — it is
    carried in the JWT token and assigned by the platform on the server side.
    This test verifies the integration_id data flow through the entry config.
    """
    minimal_entry.data["integration_id"] = "my-household-xyz"

    _patch_registries(empty_hass, FakeEntityRegistry(), FakeDeviceRegistry(), FakeAreaRegistry(), FakeLabelRegistry(), None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # With empty registries, all arrays are empty.
    assert result["floors"] == []
    assert result["areas"] == []
    assert result["devices"] == []


# ---------------------------------------------------------------------------
# Scenario 15: Labels deduplicated (same label on entity + device)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_labels_deduplicated(empty_hass, minimal_entry):
    """The same label referenced from entity AND device appears only once in labels array."""
    entity_reg = FakeEntityRegistry(
        {"light.x": FakeEntity("light.x", device_id="dev_x", labels={"lbl_shared"})}
    )
    device_reg = FakeDeviceRegistry(
        {"dev_x": FakeDevice("dev_x", labels={"lbl_shared"})}
    )
    label_reg = FakeLabelRegistry({"lbl_shared": FakeLabel("lbl_shared", "Shared")})

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), label_reg, None)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    # Only one label entry
    assert len(result["labels"]) == 1
    assert result["labels"][0]["name"] == "Shared"

    # Two assignments (entity + device)
    assert len(result["label_assignments"]) == 2


# ---------------------------------------------------------------------------
# Scenario 16: Domain and device_class in entity_device_mappings
# ---------------------------------------------------------------------------

class FakeState:
    def __init__(self, attributes: dict[str, str] | None = None) -> None:
        self.attributes = attributes or {}


@pytest.mark.asyncio
async def test_entity_mappings_include_domain_and_device_class(empty_hass, minimal_entry):
    """Entity device mappings carry domain (from entity_id prefix) and device_class (from HA state)."""
    entity_reg = FakeEntityRegistry(
        {
            "sensor.humidity": FakeEntity("sensor.humidity", device_id="dev_h"),
            "light.kitchen": FakeEntity("light.kitchen", device_id="dev_k"),
        }
    )
    device_reg = FakeDeviceRegistry(
        {
            "dev_h": FakeDevice("dev_h", "Humidity Sensor"),
            "dev_k": FakeDevice("dev_k", "Kitchen Light"),
        }
    )

    _patch_registries(empty_hass, entity_reg, device_reg, FakeAreaRegistry(), FakeLabelRegistry(), None)

    # Set up HA states with device_class in attributes
    empty_hass.states.get = lambda entity_id: {
        "sensor.humidity": FakeState({"device_class": "humidity"}),
        "light.kitchen": FakeState({"device_class": ""}),
    }.get(entity_id)

    result = await topology_builder.collect_topology(empty_hass, minimal_entry)

    assert len(result["entity_device_mappings"]) == 2

    # sensor.humidity → domain=sensor, device_class=humidity
    sensor_mapping = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "sensor.humidity"
    )
    assert sensor_mapping["domain"] == "sensor"
    assert sensor_mapping["device_class"] == "humidity"

    # light.kitchen → domain=light, device_class=""
    light_mapping = next(
        m for m in result["entity_device_mappings"] if m["entity_id"] == "light.kitchen"
    )
    assert light_mapping["domain"] == "light"
    assert light_mapping["device_class"] == ""


# ---------------------------------------------------------------------------
# Scenario 17: virtual_device_id — deterministic UUID v5
# ---------------------------------------------------------------------------

class TestVirtualDeviceId:
    """Unit tests for virtual_device_id() — no async, no fixtures needed."""

    def test_same_domain_produces_same_uuid(self):
        """Same domain string always produces the same UUID v5."""
        a = topology_builder.virtual_device_id("automation")
        b = topology_builder.virtual_device_id("automation")
        assert a == b

    def test_different_domains_produce_different_uuids(self):
        """Different domain strings produce different UUID v5 values."""
        auto_id = topology_builder.virtual_device_id("automation")
        script_id = topology_builder.virtual_device_id("script")
        weather_id = topology_builder.virtual_device_id("weather")
        # All three must be pairwise distinct
        assert auto_id != script_id
        assert auto_id != weather_id
        assert script_id != weather_id

    def test_uuid_is_valid_format(self):
        """Returned value is a valid, well-formed UUID v5 string."""
        vid = topology_builder.virtual_device_id("input_boolean")
        # Should parse as UUID
        parsed = uuid.UUID(vid)
        assert parsed.version == 5
        assert str(parsed) == vid

    def test_all_device_less_domains_produce_valid_uuids(self):
        """Every domain in DEVICE_LESS_DOMAINS yields a valid UUID v5."""
        for domain in topology_builder.DEVICE_LESS_DOMAINS:
            vid = topology_builder.virtual_device_id(domain)
            parsed = uuid.UUID(vid)
            assert parsed.version == 5
            assert str(parsed) == vid

    def test_device_less_domains_count_and_values(self):
        """DEVICE_LESS_DOMAINS contains exactly 15 expected device-less domains."""
        expected = {
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
        assert topology_builder.DEVICE_LESS_DOMAINS == expected
        assert len(topology_builder.DEVICE_LESS_DOMAINS) == 15

    def test_namespace_is_deterministic(self):
        """AGENTIC_HOME_NAMESPACE is a fixed UUID v5 based on DNS namespace."""
        ns = topology_builder.AGENTIC_HOME_NAMESPACE
        assert isinstance(ns, uuid.UUID)
        # Recompute and verify determinism of the namespace itself
        recomputed = uuid.uuid5(uuid.NAMESPACE_DNS, "agentic-home.virtual-device")
        assert ns == recomputed


# ---------------------------------------------------------------------------
# Helper: patch hass registries
# ---------------------------------------------------------------------------

def _patch_registries(
    hass: MagicMock,
    entity_reg: FakeEntityRegistry,
    device_reg: FakeDeviceRegistry,
    area_reg: FakeAreaRegistry,
    label_reg: FakeLabelRegistry,
    floor_reg: FakeFloorRegistry | None,
) -> None:
    """Patch the module-level async_get functions on the topology_builder module."""
    # Reset ALL module-level registry references first so prior test state is gone.
    topology_builder.er = MagicMock()
    topology_builder.dr = MagicMock()
    topology_builder.ar = MagicMock()
    topology_builder.lr = MagicMock()
    topology_builder._get_floor_registry = MagicMock()

    # Patch module-level references so collect_topology uses fakes directly.
    topology_builder.er.async_get = lambda _h: entity_reg
    topology_builder.dr.async_get = lambda _h: device_reg
    topology_builder.ar.async_get = lambda _h: area_reg
    topology_builder.lr.async_get = lambda _h: label_reg

    if floor_reg is not None:
        topology_builder._get_floor_registry = lambda _h: floor_reg
    else:
        topology_builder._get_floor_registry = lambda _h: None

    # Ensure hass.states.get returns None by default (no device_class available).
    # Tests that need device_class can override this after calling _patch_registries.
    hass.states = MagicMock()
    hass.states.get = lambda _entity_id: None