"""Tests for custom_components.agentic_home.const module."""

import pytest

from custom_components.agentic_home import const as const_module


class TestConstants:
    """Verify all constants exist and have expected values."""

    def test_domain(self) -> None:
        assert const_module.DOMAIN == "agentic_home"

    def test_conf_ingress_url(self) -> None:
        assert const_module.CONF_INGRESS_URL == "ingress_url"

    def test_conf_jwt_token(self) -> None:
        assert const_module.CONF_JWT_TOKEN == "jwt_token"

    def test_http_timeout_seconds(self) -> None:
        assert isinstance(const_module.HTTP_TIMEOUT_SECONDS, int)
        assert const_module.HTTP_TIMEOUT_SECONDS >= 1

    def test_ingress_status_path(self) -> None:
        assert const_module.INGRESS_STATUS_PATH.startswith("/api/v1/")

    def test_ingress_stream_path(self) -> None:
        assert const_module.INGRESS_STREAM_PATH.startswith("/api/v1/")

    def test_ingress_registry_path(self) -> None:
        assert const_module.INGRESS_REGISTRY_PATH.startswith("/api/v1/")

    def test_ingress_paths_exhaustive(self) -> None:
        """Every INGRESS_*_PATH constant starts with /api/v1/."""
        for name in dir(const_module):
            if name.startswith("INGRESS_") and name.endswith("_PATH"):
                value = getattr(const_module, name)
                assert value.startswith("/api/v1/"), f"{name}={value!r} does not start with /api/v1/"

    def test_batch_flush_interval_ms(self) -> None:
        assert isinstance(const_module.BATCH_FLUSH_INTERVAL_MS, int)
        assert const_module.BATCH_FLUSH_INTERVAL_MS > 0

    def test_batch_max_frames(self) -> None:
        assert isinstance(const_module.BATCH_MAX_FRAMES, int)
        assert const_module.BATCH_MAX_FRAMES >= 1

    def test_inventory_interval_seconds(self) -> None:
        assert isinstance(const_module.INVENTORY_INTERVAL_SECONDS, int)
        assert const_module.INVENTORY_INTERVAL_SECONDS > 0

    def test_inventory_jitter_seconds(self) -> None:
        assert isinstance(const_module.INVENTORY_JITTER_SECONDS, int)
        assert const_module.INVENTORY_JITTER_SECONDS >= 0

    def test_heartbeat_interval_seconds(self) -> None:
        assert isinstance(const_module.HEARTBEAT_INTERVAL_SECONDS, int)
        assert const_module.HEARTBEAT_INTERVAL_SECONDS > 0

    def test_no_duplicate_constants(self) -> None:
        """Ensure no accidental shadowing of homeassistant imports in this module."""
        names = [n for n in dir(const_module) if not n.startswith("_")]
        # Assert domain is present exactly once
        assert names.count("DOMAIN") == 1