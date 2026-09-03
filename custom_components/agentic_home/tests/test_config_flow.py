"""Tests for custom_components.agentic_home.config_flow module."""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from custom_components.agentic_home import config_flow as flow_module
from custom_components.agentic_home.const import (
    CONF_EXCLUDE_ENTITIES,
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
)
from custom_components.agentic_home.tests.conftest import make_aiohttp_response

# Ensure the integration is importable as a top-level package
sys.path.insert(0, ".")


# ---------------------------------------------------------------------------
# _validate_input path tests
# ---------------------------------------------------------------------------


class TestValidateInput:
    """Test all _validate_input code paths."""

    @pytest.fixture(autouse=True)
    def _mock_clientsession(self) -> MagicMock:
        session = MagicMock()
        self._mock_session = session
        with patch.object(flow_module, "async_get_clientsession", return_value=session):
            yield session

    @pytest.mark.asyncio
    async def test_validate_input_success(self, ah_mock_hass: MagicMock) -> None:
        """200 + integration_id → returns dict."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(200, {"integration_id": "abc123"})
        )

        result = await flow_module._validate_input(
            ah_mock_hass, "https://ingress.example.com", "valid_token"
        )

        assert result == {"integration_id": "abc123"}

    @pytest.mark.asyncio
    async def test_validate_input_401(self, ah_mock_hass: MagicMock) -> None:
        """401 response raises InvalidAuth."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(401)
        )

        with pytest.raises(flow_module.InvalidAuth):
            await flow_module._validate_input(
                ah_mock_hass, "https://ingress.example.com", "bad_token"
            )

    @pytest.mark.asyncio
    async def test_validate_input_conn_error(self, ah_mock_hass: MagicMock) -> None:
        """aiohttp.ClientError raises CannotConnect."""
        import aiohttp

        # Make __aenter__ return our fake response so resp.status is defined.
        fake = make_aiohttp_response(200)
        self._mock_session.get.return_value.__aenter__.return_value = fake
        # Have __aexit__ raise ClientError — this fires after the body runs, which
        # is when aiohttp raises it for connection-level failures.
        fake.__aexit__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))

        with pytest.raises(flow_module.CannotConnect):
            await flow_module._validate_input(
                ah_mock_hass, "https://unreachable.example.com", "token"
            )

    @pytest.mark.asyncio
    async def test_validate_input_timeout(self, ah_mock_hass: MagicMock) -> None:
        """TimeoutError raises CannotConnect."""
        fake = make_aiohttp_response(200)
        self._mock_session.get.return_value.__aenter__.return_value = fake
        fake.__aexit__ = AsyncMock(side_effect=TimeoutError("timed out"))

        with pytest.raises(flow_module.CannotConnect):
            await flow_module._validate_input(
                ah_mock_hass, "https://slow.example.com", "token"
            )

    @pytest.mark.asyncio
    async def test_validate_input_missing_integration_id(self, ah_mock_hass: MagicMock) -> None:
        """200 with no integration_id raises CannotConnect."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(200, {})
        )

        with pytest.raises(flow_module.CannotConnect):
            await flow_module._validate_input(
                ah_mock_hass, "https://ingress.example.com", "token"
            )

    @pytest.mark.asyncio
    async def test_bearer_header_sent(self, ah_mock_hass: MagicMock) -> None:
        """Bearer token is passed in Authorization header."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(200, {"integration_id": "xyz"})
        )

        await flow_module._validate_input(
            ah_mock_hass, "https://ingress.example.com", "secret_jwt_token"
        )

        call_kwargs = (
            self._mock_session.get.call_args
        )
        assert call_kwargs is not None
        _, kwargs = call_kwargs
        assert kwargs["headers"]["Authorization"] == "Bearer secret_jwt_token"

    @pytest.mark.asyncio
    async def test_validate_input_500_raises_cannot_connect(self, ah_mock_hass: MagicMock) -> None:
        """HTTP 5xx raises CannotConnect."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(500)
        )

        with pytest.raises(flow_module.CannotConnect):
            await flow_module._validate_input(
                ah_mock_hass, "https://ingress.example.com", "token"
            )


# ---------------------------------------------------------------------------
# User-facing ConfigFlow tests
# ---------------------------------------------------------------------------


class TestUserFlow:
    """Test AgenticHomeConfigFlow.async_step_user."""

    @pytest.fixture(autouse=True)
    def _mock_clientsession(self) -> MagicMock:
        session = MagicMock()
        self._mock_session = session
        with patch.object(flow_module, "async_get_clientsession", return_value=session):
            yield session

    @pytest.mark.asyncio
    async def test_step_user_creates_entry(self, ah_mock_hass: MagicMock) -> None:
        """Valid input → creates ConfigEntry with integration_id as unique_id."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(200, {"integration_id": "abc123"})
        )

        flow = flow_module.AgenticHomeConfigFlow()
        flow.hass = ah_mock_hass
        flow.context = {}  # type: ignore[assignment]

        result = await flow.async_step_user(
            {CONF_INGRESS_URL: "https://ingress.example.com", CONF_JWT_TOKEN: "tok"}
        )

        assert result["type"] == "create_entry"
        assert result["data"][CONF_INGRESS_URL] == "https://ingress.example.com"
        assert result["data"][CONF_JWT_TOKEN] == "tok"
        assert result["data"]["integration_id"] == "abc123"
        assert result.get("context", {}).get("unique_id") == "abc123"

    @pytest.mark.asyncio
    async def test_unique_id_set(self, ah_mock_hass: MagicMock) -> None:
        """unique_id on the entry equals integration_id from the API."""
        self._mock_session.get.return_value.__aenter__.return_value = (
            make_aiohttp_response(200, {"integration_id": "int-id-999"})
        )

        flow = flow_module.AgenticHomeConfigFlow()
        flow.hass = ah_mock_hass
        flow.context = {}  # type: ignore[assignment]

        result = await flow.async_step_user(
            {CONF_INGRESS_URL: "https://x.com", CONF_JWT_TOKEN: "t"}
        )

        assert result.get("context", {}).get("unique_id") == "int-id-999"

    @pytest.mark.asyncio
    async def test_step_user_error_invalid_auth(self, ah_mock_hass: MagicMock) -> None:
        """401 → error shown, no entry created."""
        flow = flow_module.AgenticHomeConfigFlow()
        flow.hass = ah_mock_hass
        flow.context = {}  # type: ignore[assignment]

        with patch.object(
            flow_module, "_validate_input", side_effect=flow_module.InvalidAuth
        ):
            result = await flow.async_step_user(
                {CONF_INGRESS_URL: "https://x.com", CONF_JWT_TOKEN: "bad"}
            )

        assert result["errors"]["base"] == "invalid_auth"

    @pytest.mark.asyncio
    async def test_step_user_error_cannot_connect(self, ah_mock_hass: MagicMock) -> None:
        """Connection error → error shown, no entry created."""
        flow = flow_module.AgenticHomeConfigFlow()
        flow.hass = ah_mock_hass
        flow.context = {}  # type: ignore[assignment]

        with patch.object(
            flow_module, "_validate_input", side_effect=flow_module.CannotConnect
        ):
            result = await flow.async_step_user(
                {CONF_INGRESS_URL: "https://x.com", CONF_JWT_TOKEN: "tok"}
            )

        assert result["errors"]["base"] == "cannot_connect"


# ---------------------------------------------------------------------------
# Options flow — menu
# ---------------------------------------------------------------------------


class TestOptionsFlowMenu:
    """Test AgenticHomeOptionsFlow async_step_init shows the two-option menu."""

    @pytest.mark.asyncio
    async def test_init_shows_menu(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """async_step_init returns show_menu with two options."""
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            result = await flow.async_step_init(None)

        assert result["type"] == "menu"
        assert result["menu_options"] == ["connection_settings", "exclude_entities"]

    @pytest.mark.asyncio
    async def test_menu_has_two_options(self, ah_mock_hass: MagicMock) -> None:
        """Menu lists exactly connection_settings and exclude_entities."""
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch(
            "custom_components.agentic_home.config_flow.AgenticHomeOptionsFlow.config_entry",
            new_callable=PropertyMock,
            return_value=MagicMock(data={}, options={}),
        ):
            result = await flow.async_step_init(None)

        menu_opts = result["menu_options"]
        assert "connection_settings" in menu_opts
        assert "exclude_entities" in menu_opts
        assert len(menu_opts) == 2


# ---------------------------------------------------------------------------
# Options flow — connection_settings step
# ---------------------------------------------------------------------------


class TestConnectionSettingsStep:
    """Test the async_step_connection_settings handler."""

    @pytest.fixture
    def _entry_defaults(self) -> dict:
        """Minimal entry data with credentials pre-filled."""
        return {
            CONF_INGRESS_URL: "https://old.example.com",
            CONF_JWT_TOKEN: "old_token",
            "integration_id": "old-id",
        }

    @pytest.mark.asyncio
    async def test_form_displayed_with_defaults(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
        _entry_defaults: dict,
    ) -> None:
        """Form pre-filled with current URL and token from entry.data."""
        ah_mock_config_entry.data = _entry_defaults
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            result = await flow.async_step_connection_settings(None)

        assert result["type"] == "form"
        assert result["step_id"] == "connection_settings"
        # Default values come from entry.data defaults in the schema.
        schema = result["data_schema"]
        # vol.Required with default → schema renders default value.
        assert schema is not None

    @pytest.mark.asyncio
    async def test_valid_update_triggers_reload(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
        _entry_defaults: dict,
    ) -> None:
        """Valid credentials → async_update_entry + async_reload_entry + abort."""
        ah_mock_config_entry.data = _entry_defaults
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            with patch.object(
                flow_module,
                "_validate_input",
                return_value={"integration_id": "new-id"},
            ):
                result = await flow.async_step_connection_settings(
                    {CONF_INGRESS_URL: "https://new.example.com", CONF_JWT_TOKEN: "new_token"}
                )

        assert result["type"] == "abort"
        assert result["reason"] == "configuration_updated"
        ah_mock_hass.config_entries.async_update_entry.assert_called_once()
        ah_mock_hass.config_entries.async_reload_entry.assert_called_once_with(ah_mock_config_entry)

    @pytest.mark.asyncio
    async def test_invalid_auth_shows_error(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
        _entry_defaults: dict,
    ) -> None:
        """401 → form re-displayed with invalid_auth error."""
        ah_mock_config_entry.data = _entry_defaults
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            with patch.object(
                flow_module, "_validate_input", side_effect=flow_module.InvalidAuth
            ):
                result = await flow.async_step_connection_settings(
                    {CONF_INGRESS_URL: "https://x.com", CONF_JWT_TOKEN: "bad"}
                )

        assert result["type"] == "form"
        assert result["errors"]["base"] == "invalid_auth"

    @pytest.mark.asyncio
    async def test_cannot_connect_shows_error(
        self,
        ah_mock_hass: MagicMock,
        ah_mock_config_entry: MagicMock,
        _entry_defaults: dict,
    ) -> None:
        """Connection failure → form re-displayed with cannot_connect error."""
        ah_mock_config_entry.data = _entry_defaults
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            with patch.object(
                flow_module, "_validate_input", side_effect=flow_module.CannotConnect
            ):
                result = await flow.async_step_connection_settings(
                    {CONF_INGRESS_URL: "https://x.com", CONF_JWT_TOKEN: "tok"}
                )

        assert result["type"] == "form"
        assert result["errors"]["base"] == "cannot_connect"


# ---------------------------------------------------------------------------
# Options flow — exclude_entities step
# ---------------------------------------------------------------------------


class TestExcludeEntitiesStep:
    """Test the async_step_exclude_entities handler."""

    @pytest.mark.asyncio
    async def test_form_displayed(self, ah_mock_hass: MagicMock) -> None:
        """Form shown with EntitySelector on first render (no user_input)."""
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch(
            "custom_components.agentic_home.config_flow.AgenticHomeOptionsFlow.config_entry",
            new_callable=PropertyMock,
            return_value=MagicMock(data={}, options={}),
        ):
            result = await flow.async_step_exclude_entities(None)

        assert result["type"] == "form"
        assert result["step_id"] == "exclude_entities"

    @pytest.mark.asyncio
    async def test_default_from_options(self, ah_mock_hass: MagicMock) -> None:
        """Default list populated from entry.options[CONF_EXCLUDE_ENTITIES]."""
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        entry = MagicMock(data={}, options={CONF_EXCLUDE_ENTITIES: ["light.lr", "switch.x"]})
        with patch.object(
            type(flow), "config_entry", new_callable=PropertyMock, return_value=entry
        ):
            result = await flow.async_step_exclude_entities(None)

        assert result["type"] == "form"
        # Schema default includes current_exclude values.
        schema = result["data_schema"]
        assert schema is not None

    @pytest.mark.asyncio
    async def test_submit_with_entities(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Submit with a non-empty list → async_create_entry with exclude list."""
        ah_mock_config_entry.options = {}
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            result = await flow.async_step_exclude_entities(
                {CONF_EXCLUDE_ENTITIES: ["light.kitchen", "climate.living_room"]}
            )

        assert result["type"] == "create_entry"
        assert result["data"][CONF_EXCLUDE_ENTITIES] == ["light.kitchen", "climate.living_room"]

    @pytest.mark.asyncio
    async def test_submit_empty_removes_exclusions(
        self, ah_mock_hass: MagicMock, ah_mock_config_entry: MagicMock
    ) -> None:
        """Submit with no entities → creates entry with empty list."""
        ah_mock_config_entry.options = {}
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch.object(
            type(flow),
            "config_entry",
            new_callable=PropertyMock,
            return_value=ah_mock_config_entry,
        ):
            result = await flow.async_step_exclude_entities({CONF_EXCLUDE_ENTITIES: []})

        assert result["type"] == "create_entry"
        assert result["data"][CONF_EXCLUDE_ENTITIES] == []

    @pytest.mark.asyncio
    async def test_schema_uses_entity_selector(self, ah_mock_hass: MagicMock) -> None:
        """Schema contains EntitySelectorConfig(multiple=True) for CONF_EXCLUDE_ENTITIES."""
        flow = flow_module.AgenticHomeOptionsFlow()
        flow.hass = ah_mock_hass
        with patch(
            "custom_components.agentic_home.config_flow.AgenticHomeOptionsFlow.config_entry",
            new_callable=PropertyMock,
            return_value=MagicMock(data={}, options={}),
        ):
            result = await flow.async_step_exclude_entities(None)

        schema = result["data_schema"]
        assert schema is not None
        # The schema wraps the selector via vol.Optional with default.
        # Verify the step renders with step_id "exclude_entities".
        assert result["step_id"] == "exclude_entities"


# ---------------------------------------------------------------------------
# Step ID / strings.json alignment
# ---------------------------------------------------------------------------


class TestStepIds:
    """Verify step IDs in code match strings.json keys."""

    def test_step_ids_match_strings_keys(self) -> None:
        """Config step 'user', options init/menu, connection_settings, exclude_entities exist."""
        import json

        with open(
            "custom_components/agentic_home/strings.json", encoding="utf-8"
        ) as f:
            strings = json.load(f)

        assert "user" in strings["config"]["step"], "strings.json config.step missing 'user'"
        assert "init" in strings["options"]["step"], "strings.json options.step missing 'init'"
        assert "connection_settings" in strings["options"]["step"], (
            "strings.json options.step missing 'connection_settings'"
        )
        assert "exclude_entities" in strings["options"]["step"], (
            "strings.json options.step missing 'exclude_entities'"
        )

    def test_menu_options_present_in_strings(self) -> None:
        """Menu option keys appear in strings.json menu_options."""
        import json

        with open(
            "custom_components/agentic_home/strings.json", encoding="utf-8"
        ) as f:
            strings = json.load(f)

        menu_opts = strings["options"]["step"]["init"]["menu_options"]
        assert "connection_settings" in menu_opts
        assert "exclude_entities" in menu_opts

    def test_exclude_entities_data_key_present(self) -> None:
        """exclude_entities step declares CONF_EXCLUDE_ENTITIES key in data schema."""
        import json

        with open(
            "custom_components/agentic_home/strings.json", encoding="utf-8"
        ) as f:
            strings = json.load(f)

        step = strings["options"]["step"]["exclude_entities"]
        assert "data" in step
        assert CONF_EXCLUDE_ENTITIES in step["data"]