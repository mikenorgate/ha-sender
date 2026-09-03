"""Shared pytest fixtures for Agentic Home integration tests."""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# Ensure the integration is importable as a top-level package
sys.path.insert(0, ".")

from custom_components.agentic_home.const import CONF_INGRESS_URL, CONF_JWT_TOKEN

# ---------------------------------------------------------------------------
# aiohttp response helpers
# ---------------------------------------------------------------------------


class FakeResponse:
    """Fake aiohttp.ClientResponse used to mock session.get() responses."""

    def __init__(self, status: int, json_data: dict[str, Any] | None = None):
        self.status = status
        self._json_data = json_data or {}

    async def json(self) -> dict[str, Any]:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise Exception(f"HTTP {self.status}")


class FakeContextManagerResponse(FakeResponse):
    """FakeResponse that works as an async context manager (session.get yields this)."""

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


def make_aiohttp_response(
    status: int, json_data: dict[str, Any] | None = None
) -> FakeContextManagerResponse:
    """Factory: return a fake aiohttp response for a given status + JSON body."""
    return FakeContextManagerResponse(status=status, json_data=json_data)


# ---------------------------------------------------------------------------
# HomeAssistant mock fixture  (namespaced to avoid HA plugin shadowing)
# ---------------------------------------------------------------------------


@pytest.fixture
def ah_mock_hass() -> MagicMock:
    """Return a MagicMock hass object with aiohttp_client stubbed and event bus mocked."""
    hass = MagicMock()
    # Use a real dict for .data so hass.data.setdefault() works as expected.
    hass.data = {}
    # async_reload_entry is called with await; async_update_entry is synchronous.
    hass.config_entries.async_reload_entry = AsyncMock()
    # Platform forwarding/unloading (required by sensor and binary_sensor platforms).
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config_entries.async_unload_platforms = AsyncMock()

    # Mock the HA event bus (used by EventSubscriber to register listeners).
    mock_bus = MagicMock()
    # async_listen returns an unregister function.
    mock_bus.async_listen = MagicMock(return_value=MagicMock())
    hass.bus = mock_bus

    return hass


# ---------------------------------------------------------------------------
# Config entry mock fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def ah_mock_config_entry() -> MagicMock:
    """Return a minimal ConfigEntry-like MagicMock."""
    entry = MagicMock()
    entry.entry_id = "test_entry_123"
    entry.data = {
        CONF_INGRESS_URL: "https://ingress.example.com",
        CONF_JWT_TOKEN: "test_jwt_token",
        "integration_id": "test-integration-abc",
    }
    entry.options = {}
    return entry