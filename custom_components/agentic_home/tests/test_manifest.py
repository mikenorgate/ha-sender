"""Tests for custom_components.agentic_home manifest and metadata."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from custom_components.agentic_home import const as const_module

MANIFEST_PATH = "custom_components/agentic_home/manifest.json"
STRINGS_PATH = "custom_components/agentic_home/strings.json"


def _load_json(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class TestManifest:
    """Test manifest.json structural and HACS contract constraints."""

    def test_manifest_valid_json(self) -> None:
        """manifest.json parses as valid JSON."""
        manifest = _load_json(MANIFEST_PATH)
        assert isinstance(manifest, dict)

    def test_domain_matches_const(self) -> None:
        """manifest domain matches const.DOMAIN."""
        manifest = _load_json(MANIFEST_PATH)
        assert manifest["domain"] == const_module.DOMAIN

    def test_config_flow_true(self) -> None:
        """manifest.json declares config_flow: true."""
        manifest = _load_json(MANIFEST_PATH)
        assert manifest.get("config_flow") is True

    def test_no_requirements(self) -> None:
        """manifest.json has no pip requirements (requirements == [])."""
        manifest = _load_json(MANIFEST_PATH)
        assert manifest.get("requirements", []) == []

    def test_version_floor(self) -> None:
        """version is a non-empty string matching PEP 440-ish."""
        manifest = _load_json(MANIFEST_PATH)
        version = manifest.get("version", "")
        assert isinstance(version, str)
        assert len(version) > 0
        # Basic format check: major.minor.patch
        parts = version.split(".")
        assert len(parts) >= 2, f"version {version!r} not in X.Y.Z format"

    def test_iot_class(self) -> None:
        """iot_class is declared and non-empty."""
        manifest = _load_json(MANIFEST_PATH)
        assert isinstance(manifest.get("iot_class"), str)
        assert len(manifest["iot_class"]) > 0

    def test_requires_homeassistant_floor(self) -> None:
        """requires_homeassistant is declared and non-empty."""
        manifest = _load_json(MANIFEST_PATH)
        required = manifest.get("requires_homeassistant", "")
        assert isinstance(required, str)
        assert len(required) > 0


class TestStringsJson:
    """Test strings.json structural correctness."""

    def test_strings_json_valid(self) -> None:
        """strings.json parses as valid JSON."""
        strings = _load_json(STRINGS_PATH)
        assert isinstance(strings, dict)

    def test_strings_json_has_config_step(self) -> None:
        """strings.json has config.step.user."""
        strings = _load_json(STRINGS_PATH)
        assert "config" in strings
        assert "step" in strings["config"]
        assert "user" in strings["config"]["step"]

    def test_strings_json_has_config_error(self) -> None:
        """strings.json has config.error.cannot_connect and invalid_auth."""
        strings = _load_json(STRINGS_PATH)
        errors = strings.get("config", {}).get("error", {})
        assert "cannot_connect" in errors
        assert "invalid_auth" in errors

    def test_strings_json_has_options_step(self) -> None:
        """strings.json has options.step.init."""
        strings = _load_json(STRINGS_PATH)
        assert "options" in strings
        assert "step" in strings["options"]
        assert "init" in strings["options"]["step"]

    def test_strings_json_has_options_error(self) -> None:
        """strings.json has options.error.cannot_connect and invalid_auth."""
        strings = _load_json(STRINGS_PATH)
        errors = strings.get("options", {}).get("error", {})
        assert "cannot_connect" in errors
        assert "invalid_auth" in errors

    def test_error_messages_non_empty(self) -> None:
        """All error messages in strings.json are non-empty strings."""
        strings = _load_json(STRINGS_PATH)
        for section in ("config", "options"):
            for key, value in strings.get(section, {}).get("error", {}).items():
                assert isinstance(value, str), f"strings.json {section}.error.{key} is not a string"
                assert len(value) > 0, f"strings.json {section}.error.{key} is empty"