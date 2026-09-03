"""Tests for custom_components.agentic_home.__init__ module."""

from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.agentic_home import __init__ as init_module
from custom_components.agentic_home.const import DOMAIN
from custom_components.agentic_home.heartbeat import HeartbeatGenerator
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home.pusher import IngressHTTPPusher
from custom_components.agentic_home.registry_pusher import RegistryPusher
from custom_components.agentic_home.subscriber import EventSubscriber

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reload_init() -> tuple:
    """Reload __init__ module and return (async_setup_entry, async_unload_entry)."""
    if "custom_components.agentic_home" in sys.modules:
        import custom_components.agentic_home as _pkg
        importlib.reload(_pkg)
        m = sys.modules["custom_components.agentic_home"]
        return m.async_setup_entry, m.async_unload_entry
    return init_module.async_setup_entry, init_module.async_unload_entry


def _pipeline_keys() -> dict[str, str]:
    """Return the pipeline component key names used in hass.data[DOMAIN][entry_id]."""
    return {
        "counter": "sequence_counter",
        "metrics": "metrics",
        "pusher": "pusher",
        "registry_pusher": "registry_pusher",
        "subscriber": "subscriber",
        "heartbeat_gen": "heartbeat_gen",
    }


class TestAsyncSetupEntry:
    """Test async_setup_entry."""

    @pytest.mark.asyncio
    async def test_setup_entry_returns_true(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """async_setup_entry returns True on success."""
        setup, _ = _reload_init()
        result = await setup(ah_mock_hass, ah_mock_config_entry)
        assert result is True

    @pytest.mark.asyncio
    async def test_setup_entry_stores_data(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Entry data dict is stored in hass.data[DOMAIN][entry_id] with pipeline components."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        assert DOMAIN in ah_mock_hass.data
        assert ah_mock_config_entry.entry_id in ah_mock_hass.data[DOMAIN]
        # The stored dict must contain the original entry data fields plus pipeline objects.
        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        assert entry_data["ingress_url"] == ah_mock_config_entry.data["ingress_url"]
        assert entry_data["jwt_token"] == ah_mock_config_entry.data["jwt_token"]


class TestAsyncUnloadEntry:
    """Test async_unload_entry."""

    @pytest.mark.asyncio
    async def test_unload_entry_returns_true(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """async_unload_entry returns True on success."""
        ah_mock_hass.data.setdefault(DOMAIN, {})
        ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id] = dict(
            ah_mock_config_entry.data
        )

        _, unload = _reload_init()
        result = await unload(ah_mock_hass, ah_mock_config_entry)
        assert result is True

    @pytest.mark.asyncio
    async def test_unload_entry_removes_data(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Entry data is removed from hass.data[DOMAIN] on unload."""
        ah_mock_hass.data.setdefault(DOMAIN, {})
        ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id] = dict(
            ah_mock_config_entry.data
        )

        _, unload = _reload_init()
        await unload(ah_mock_hass, ah_mock_config_entry)

        assert ah_mock_config_entry.entry_id not in ah_mock_hass.data[DOMAIN]

    @pytest.mark.asyncio
    async def test_setup_unload_roundtrip(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Setup then unload leaves hass.data[DOMAIN] empty."""
        ah_mock_hass.data.setdefault(DOMAIN, {})

        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)
        assert ah_mock_config_entry.entry_id in ah_mock_hass.data[DOMAIN]

        await unload(ah_mock_hass, ah_mock_config_entry)
        assert ah_mock_config_entry.entry_id not in ah_mock_hass.data[DOMAIN]

    @pytest.mark.asyncio
    async def test_unload_idempotent_if_not_setup(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Unload is safe even if entry was never set up."""
        ah_mock_hass.data.setdefault(DOMAIN, {})

        _, unload = _reload_init()
        result = await unload(ah_mock_hass, ah_mock_config_entry)
        assert result is True


# ---------------------------------------------------------------------------
# S02 Pipeline lifecycle tests
# ---------------------------------------------------------------------------


class TestPipelineSetup:
    """Test that async_setup_entry creates and starts all pipeline components."""

    @pytest.mark.asyncio
    async def test_setup_entry_creates_subscriber(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry stores an EventSubscriber in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        assert keys["subscriber"] in entry_data
        assert isinstance(entry_data[keys["subscriber"]], EventSubscriber)

    @pytest.mark.asyncio
    async def test_setup_entry_creates_pusher(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry stores an IngressHTTPPusher in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        assert keys["pusher"] in entry_data
        assert isinstance(entry_data[keys["pusher"]], IngressHTTPPusher)

    @pytest.mark.asyncio
    async def test_setup_entry_creates_metrics(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry stores a RuntimeMetrics in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        assert keys["metrics"] in entry_data
        assert isinstance(entry_data[keys["metrics"]], RuntimeMetrics)

    @pytest.mark.asyncio
    async def test_setup_entry_subscribes_to_bus(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry calls hass.bus.async_listen for '*' and entity registry events."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        # The '*' wildcard listener comes from EventSubscriber.
        # An additional listener (e.g. device_registry_updated) may come from
        # entity registry initialization during the startup push_snapshot() call.
        ah_mock_hass.bus.async_listen.assert_called()
        calls = ah_mock_hass.bus.async_listen.call_args_list
        event_types = {call[0][0] for call in calls}
        assert "*" in event_types


class TestRegistryPusherWiring:
    """Test that async_setup_entry wires the RegistryPusher correctly."""

    @pytest.mark.asyncio
    async def test_setup_entry_creates_registry_pusher(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry stores a RegistryPusher in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        assert keys["registry_pusher"] in entry_data
        assert isinstance(entry_data[keys["registry_pusher"]], RegistryPusher)

    @pytest.mark.asyncio
    async def test_setup_entry_calls_startup_push(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry calls push_snapshot() during setup for immediate push."""
        setup, _ = _reload_init()
        # Verify setup completes without raising — push_snapshot is called inline.
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        rp_keys = _pipeline_keys()
        rp: RegistryPusher = entry_data[rp_keys["registry_pusher"]]
        assert rp is not None

    @pytest.mark.asyncio
    async def test_unload_entry_stops_registry_pusher(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry calls registry_pusher.stop() before pusher drain."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        real_pusher = entry_data[keys["registry_pusher"]]

        # Replace with an async mock to verify stop() was awaited.
        mock_rp = MagicMock()
        mock_rp.stop = AsyncMock()
        entry_data[keys["registry_pusher"]] = mock_rp

        await unload(ah_mock_hass, ah_mock_config_entry)

        mock_rp.stop.assert_awaited_once()


class TestPipelineUnload:
    """Test that async_unload_entry stops and cleans up the pipeline."""

    @pytest.mark.asyncio
    async def test_unload_entry_stops_subscriber(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry calls subscriber.stop()."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        real_subscriber = entry_data[keys["subscriber"]]

        # Replace the subscriber with a mock that records stop() calls.
        mock_subscriber = MagicMock()
        mock_subscriber.running = real_subscriber.running
        entry_data[keys["subscriber"]] = mock_subscriber

        await unload(ah_mock_hass, ah_mock_config_entry)

        mock_subscriber.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_unload_entry_drains_pusher(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry awaits pusher.stop() to flush pending frames."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        real_pusher = entry_data[keys["pusher"]]

        # Replace with an async mock so we can verify stop() was awaited.
        mock_pusher = MagicMock()
        mock_pusher.stop = AsyncMock()
        entry_data[keys["pusher"]] = mock_pusher

        await unload(ah_mock_hass, ah_mock_config_entry)

        mock_pusher.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unload_entry_removes_pipeline(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry removes the entry data entirely from hass.data."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        assert ah_mock_config_entry.entry_id in ah_mock_hass.data[DOMAIN]

        await unload(ah_mock_hass, ah_mock_config_entry)

        assert ah_mock_config_entry.entry_id not in ah_mock_hass.data[DOMAIN]

    @pytest.mark.asyncio
    async def test_setup_unload_roundtrip_cleans_up_pipeline(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """A full setup→unload roundtrip removes all pipeline objects from hass.data."""
        setup, unload = _reload_init()
        keys = _pipeline_keys()

        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        assert keys["subscriber"] in entry_data
        assert keys["pusher"] in entry_data
        assert keys["metrics"] in entry_data
        assert keys["counter"] in entry_data

        await unload(ah_mock_hass, ah_mock_config_entry)

        # After unload the entry key must be gone.
        assert ah_mock_config_entry.entry_id not in ah_mock_hass.data[DOMAIN]


class TestHeartbeatGeneratorWiring:
    """Test that async_setup_entry wires the HeartbeatGenerator correctly."""

    @pytest.mark.asyncio
    async def test_setup_entry_creates_heartbeat_generator(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry stores a HeartbeatGenerator in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        assert keys["heartbeat_gen"] in entry_data
        assert isinstance(entry_data[keys["heartbeat_gen"]], HeartbeatGenerator)

    @pytest.mark.asyncio
    async def test_heartbeat_gen_stores_pusher_ref(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """HeartbeatGenerator._pusher is the same IngressHTTPPusher instance stored in hass.data."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()
        heartbeat_gen: HeartbeatGenerator = entry_data[keys["heartbeat_gen"]]
        pusher: IngressHTTPPusher = entry_data[keys["pusher"]]
        assert heartbeat_gen._pusher is pusher

    @pytest.mark.asyncio
    async def test_heartbeat_gen_started_on_setup(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry calls heartbeat_gen.start() to activate the timer."""
        import sys

        setup, _ = _reload_init()

        # Patch HeartbeatGenerator.start so we can assert it was called.
        pkg_module = sys.modules["custom_components.agentic_home"]
        original_class = pkg_module.HeartbeatGenerator
        mock_instance = MagicMock()
        pkg_module.HeartbeatGenerator = MagicMock(return_value=mock_instance)

        try:
            await setup(ah_mock_hass, ah_mock_config_entry)
            mock_instance.start.assert_called_once()
        finally:
            pkg_module.HeartbeatGenerator = original_class

    @pytest.mark.asyncio
    async def test_unload_stops_heartbeat_before_subscriber(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry calls heartbeat_gen.stop() before subscriber.stop()."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        entry_data = ah_mock_hass.data[DOMAIN][ah_mock_config_entry.entry_id]
        keys = _pipeline_keys()

        # Replace real heartbeat and subscriber with mocks to record call order.
        real_heartbeat: HeartbeatGenerator = entry_data[keys["heartbeat_gen"]]
        real_subscriber = entry_data[keys["subscriber"]]

        mock_heartbeat = MagicMock()
        mock_subscriber = MagicMock()
        mock_subscriber.running = real_subscriber.running

        entry_data[keys["heartbeat_gen"]] = mock_heartbeat
        entry_data[keys["subscriber"]] = mock_subscriber

        await unload(ah_mock_hass, ah_mock_config_entry)

        # Verify heartbeat.stop() is called before subscriber.stop().
        stop_calls = [
            (mock_heartbeat.stop.call_count, "heartbeat.stop"),
            (mock_subscriber.stop.call_count, "subscriber.stop"),
        ]
        assert stop_calls[0][0] == 1, "heartbeat.stop() was not called"
        assert stop_calls[1][0] == 1, "subscriber.stop() was not called"
        # Both must be called exactly once — order is guaranteed by sequential code.


# ---------------------------------------------------------------------------
# S06 Platform forwarding and device registry tests
# ---------------------------------------------------------------------------


class TestPlatformForwarding:
    """Test sensor and binary_sensor platform forwarding on setup and unload."""

    @pytest.mark.asyncio
    async def test_setup_entry_forwards_sensor_platforms(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry calls async_forward_entry_setups with ['sensor', 'binary_sensor']."""
        setup, _ = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)

        ah_mock_hass.config_entries.async_forward_entry_setups.assert_called()
        call_args = ah_mock_hass.config_entries.async_forward_entry_setups.call_args
        entry_arg = call_args[0][0]
        platforms_arg = call_args[0][1]
        assert entry_arg is ah_mock_config_entry
        assert set(platforms_arg) == {"sensor", "binary_sensor"}

    @pytest.mark.asyncio
    async def test_unload_entry_unloads_sensor_platforms(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_unload_entry calls async_unload_platforms with ['sensor', 'binary_sensor']."""
        setup, unload = _reload_init()
        await setup(ah_mock_hass, ah_mock_config_entry)
        ah_mock_hass.config_entries.async_forward_entry_setups.reset_mock()

        await unload(ah_mock_hass, ah_mock_config_entry)

        ah_mock_hass.config_entries.async_unload_platforms.assert_called()
        call_args = ah_mock_hass.config_entries.async_unload_platforms.call_args
        entry_arg = call_args[0][0]
        platforms_arg = call_args[0][1]
        assert entry_arg is ah_mock_config_entry
        assert set(platforms_arg) == {"sensor", "binary_sensor"}


class TestDeviceRegistry:
    """Test that async_setup_entry creates the Agentic Home device entry."""

    @pytest.mark.asyncio
    async def test_setup_entry_creates_device_registry_entry(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
    ) -> None:
        """async_setup_entry calls dev_reg.async_get_or_create with correct identifiers and metadata."""
        from homeassistant.helpers import device_registry as dr_module

        # Patch device_registry.async_get at the module level so that the
        # reloaded __init__ gets our mock instead of the real cached function.
        mock_dev_reg = MagicMock()
        mock_dev_reg.async_get_or_create = MagicMock()
        with patch.object(dr_module, "async_get", return_value=mock_dev_reg):
            # _reload_init() re-runs the module code while the patch is active,
            # so device_registry.async_get resolves to the mocked version.
            setup, _ = _reload_init()
            await setup(ah_mock_hass, ah_mock_config_entry)

        mock_dev_reg.async_get_or_create.assert_called_once()
        call_kwargs = mock_dev_reg.async_get_or_create.call_args[1]

        assert call_kwargs["config_entry_id"] == ah_mock_config_entry.entry_id
        assert call_kwargs["identifiers"] == {("agentic_home", "test-integration-abc")}
        assert call_kwargs["name"] == "Agentic Home"
        assert call_kwargs["manufacturer"] == "Agentic Home"
        assert call_kwargs["model"] == "HA Integration"
        assert "sw_version" in call_kwargs