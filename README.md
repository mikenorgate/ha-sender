# Agentic Home

Agentic Home is a Home Assistant custom integration that sends Home Assistant
events, registry snapshots, and action-catalog frames to an Agentic Home
Ingress endpoint.

## Testing-stage status

This public repository is a custom HACS repository for testing. It is not in
the HACS default catalogue and does not make a production support, security,
licensing, release, or availability commitment. The current local Tilt setup
uses HTTP; HTTPS and production deployment are outside this testing stage.

## Install in Home Assistant for testing

1. In HACS, open **Custom repositories**.
2. Add `https://github.com/mikenorgate/ha-sender` and select **Integration**.
3. Download **Agentic Home**, then restart Home Assistant.
4. Open **Settings → Devices & services → Add integration** and choose
   **Agentic Home**.
5. Enter the Agentic Home Ingress URL and its JWT token. For local Tilt testing,
   use the HTTP endpoint supplied by that environment; do not put real tokens
   or household data in issues, logs, commits, or screenshots.

The config flow validates the URL and token against
`/api/v1/ingress/status` before creating the entry. This repository has not
claimed a live-install result in this stage; real Home Assistant installation
and update evidence is recorded separately by the parent workflow.

## Update in Home Assistant for testing

When a reviewed child revision is available, HACS can update the custom
repository from its default branch (or from a published release if one is
created later). Review the revision, update from HACS, and restart Home
Assistant so the custom component is reloaded. Do not treat an update as a
production rollback or release process.

## Development

Run the tests from this repository root:

```bash
python -m pip install -e '.[test]'
python -m pytest -q custom_components/agentic_home/tests
```

The integration source remains under the Home Assistant-required
`custom_components/agentic_home/` directory. The parent platform consumes this
repository as the pinned `integrations/agentic_home` submodule.
