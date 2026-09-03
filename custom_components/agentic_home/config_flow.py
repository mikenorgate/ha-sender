"""Config flow for Agentic Home."""

from __future__ import annotations

import aiohttp
import voluptuous as vol
from homeassistant import config_entries, exceptions
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import (
    CONF_EXCLUDE_ENTITIES,
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
    DOMAIN,
    HTTP_TIMEOUT_SECONDS,
    INGRESS_STATUS_PATH,
)

# ---------------------------------------------------------------------------
# Error classes — must match keys in strings.json "config/error" and
# "options/error".
# ---------------------------------------------------------------------------


class CannotConnect(exceptions.ConfigEntryNotReady):
    """Raised when the ingress endpoint cannot be reached."""


class InvalidAuth(exceptions.ConfigEntryAuthFailed):
    """Raised when the ingress endpoint rejects the JWT token."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _validate_input(
    hass: HomeAssistant, ingress_url: str, jwt_token: str
) -> dict[str, str]:
    """Call GET {ingress_url}/api/v1/ingress/status with Bearer token.

    Returns a dict with key "integration_id" on success.
    Raises CannotConnect on connection/timeout/non-2xx.
    Raises InvalidAuth on 401.
    """
    status_url = ingress_url.rstrip("/") + INGRESS_STATUS_PATH
    headers = {"Authorization": f"Bearer {jwt_token}"}
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)

    try:
        session = async_get_clientsession(hass)
        async with session.get(status_url, headers=headers, timeout=timeout) as resp:
            if resp.status == 401:
                raise InvalidAuth
            if resp.status >= 400:
                raise CannotConnect
            resp.raise_for_status()
            data = await resp.json()
            integration_id = data.get("integration_id")
            if not integration_id:
                raise CannotConnect
            return {"integration_id": integration_id}
    except aiohttp.ClientError as err:
        raise CannotConnect from err
    except TimeoutError as err:
        raise CannotConnect from err


# ---------------------------------------------------------------------------
# User-facing config flow
# ---------------------------------------------------------------------------


class AgenticHomeConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Agentic Home."""

    @staticmethod
    async def async_get_options_flow(entry: config_entries.ConfigEntry) -> OptionsFlow:
        """Return the options flow handler."""
        return AgenticHomeOptionsFlow()

    async def async_step_user(
        self, user_input: dict[str, str] | None = None
    ) -> FlowResult:
        """Invoke when the user starts the config flow."""
        errors: dict[str, str] = {}

        if user_input is not None:
            try:
                result = await _validate_input(
                    self.hass,
                    user_input[CONF_INGRESS_URL],
                    user_input[CONF_JWT_TOKEN],
                )
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                # unique_id pins this entry to a specific integration so HA
                # won't create a duplicate if the user re-runs the flow.
                self.context["unique_id"] = result["integration_id"]
                return self.async_create_entry(
                    title="Agentic Home",
                    data={
                        CONF_INGRESS_URL: user_input[CONF_INGRESS_URL],
                        CONF_JWT_TOKEN: user_input[CONF_JWT_TOKEN],
                        "integration_id": result["integration_id"],
                    },
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_INGRESS_URL): str,
                vol.Required(CONF_JWT_TOKEN): str,
            }),
            errors=errors,
            description_placeholders={},
        )


# ---------------------------------------------------------------------------
# Options flow (token rotation + entity exclusion)
# ---------------------------------------------------------------------------


class AgenticHomeOptionsFlow(config_entries.OptionsFlow):
    """Handle options for Agentic Home — menu-driven: connection_settings or exclude_entities."""

    async def async_step_init(
        self, user_input: dict[str, str] | None = None
    ) -> FlowResult:
        """Show the top-level options menu."""
        return self.async_show_menu(
            step_id="init",
            menu_options=["connection_settings", "exclude_entities"],
            description_placeholders={},
        )

    async def async_step_connection_settings(
        self, user_input: dict[str, str] | None = None
    ) -> FlowResult:
        """Handle connection settings update (URL + JWT token)."""
        errors: dict[str, str] = {}

        # Pull current data from the entry using .get() for safety.
        entry = self.config_entry
        current_url = entry.data.get(CONF_INGRESS_URL, "")
        current_token = entry.data.get(CONF_JWT_TOKEN, "")

        if user_input is not None:
            try:
                result = await _validate_input(
                    self.hass,
                    user_input[CONF_INGRESS_URL],
                    user_input[CONF_JWT_TOKEN],
                )
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            else:
                # Persist updated data and reload the entry so HA re-initialises.
                self.hass.config_entries.async_update_entry(
                    entry,
                    data={
                        CONF_INGRESS_URL: user_input[CONF_INGRESS_URL],
                        CONF_JWT_TOKEN: user_input[CONF_JWT_TOKEN],
                        "integration_id": result["integration_id"],
                    },
                )
                await self.hass.config_entries.async_reload_entry(entry)
                return self.async_abort(reason="configuration_updated")

        return self.async_show_form(
            step_id="connection_settings",
            data_schema=vol.Schema({
                vol.Required(CONF_INGRESS_URL, default=current_url): str,
                vol.Required(CONF_JWT_TOKEN, default=current_token): str,
            }),
            errors=errors,
            description_placeholders={},
        )

    async def async_step_exclude_entities(
        self, user_input: dict[str, str] | None = None
    ) -> FlowResult:
        """Handle entity exclusion configuration."""
        entry = self.config_entry
        current_exclude: list[str] = list(entry.options.get(CONF_EXCLUDE_ENTITIES, []))

        if user_input is not None:
            exclude_list: list[str] = list(user_input.get(CONF_EXCLUDE_ENTITIES, []))
            # async_create_entry with data={} merges data into entry.options.
            return self.async_create_entry(title="", data={CONF_EXCLUDE_ENTITIES: exclude_list})

        return self.async_show_form(
            step_id="exclude_entities",
            data_schema=vol.Schema({
                vol.Optional(CONF_EXCLUDE_ENTITIES, default=current_exclude): selector.EntitySelector(
                    selector.EntitySelectorConfig(multiple=True)
                ),
            }),
            description_placeholders={},
        )