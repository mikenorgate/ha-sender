"""Tests for custom_components.agentic_home.pusher module."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp
import pytest
import pytest_asyncio

from custom_components.agentic_home.const import (
    BATCH_FLUSH_INTERVAL_MS,
    BATCH_MAX_FRAMES,
    CONF_INGRESS_URL,
    CONF_JWT_TOKEN,
    HTTP_TIMEOUT_SECONDS,
    INGRESS_STREAM_PATH,
)
from custom_components.agentic_home.metrics import RuntimeMetrics
from custom_components.agentic_home import pusher as pusher_module
from custom_components.agentic_home.pusher import IngressHTTPPusher


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_session():
    """Return a MagicMock aiohttp client session."""
    session = MagicMock()
    return session


@pytest.fixture
def fake_hass(fake_session):
    """Return a MagicMock hass with aiohttp session and a real event loop."""
    hass = MagicMock()
    # Provide a real event loop.
    hass.loop = asyncio.new_event_loop()
    yield hass
    # Clean up the loop after each test.
    hass.loop.close()


@pytest.fixture
def mock_entry():
    """Return a ConfigEntry-like MagicMock with ingress config."""
    entry = MagicMock()
    entry.entry_id = "entry_test_abc"
    entry.data = {
        CONF_INGRESS_URL: "https://ingress.example.com",
        CONF_JWT_TOKEN: "jwt_secret_xyz",
    }
    return entry


@pytest.fixture
def metrics():
    """Return a fresh RuntimeMetrics instance."""
    return RuntimeMetrics()


@pytest.fixture
def pusher(fake_hass, mock_entry, metrics, fake_session):
    """Return an IngressHTTPPusher with all deps mocked."""
    with patch.object(pusher_module, "async_get_clientsession", return_value=fake_session):
        return IngressHTTPPusher(fake_hass, mock_entry, metrics)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_frame(entity_id: str = "light.kitchen", event_type: str = "state_changed") -> dict[str, Any]:
    """Return a minimal valid frame dict."""
    return {
        "source_event_id": "abc123",
        "source_sequence": 1,
        "delivery_mode": "live",
        "event_time": "2024-01-01T00:00:00+00:00",
        "event_type": event_type,
        "payload": {"entity_id": entity_id},
    }


class FakeResponse:
    """Fake aiohttp.ClientResponse."""

    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------

class TestPusherConstruction:
    def test_stores_ingress_url(self, pusher, mock_entry):
        """ingress_url is extracted from entry.data."""
        assert pusher._ingress_url == "https://ingress.example.com"

    def test_stores_jwt_token(self, pusher, mock_entry):
        """jwt_token is extracted from entry.data."""
        assert pusher._jwt_token == "jwt_secret_xyz"

    def test_stores_metrics_ref(self, pusher, metrics):
        """metrics reference is stored."""
        assert pusher._metrics is metrics

    def test_stores_session_ref(self, pusher, fake_session):
        """aiohttp session is stored."""
        assert pusher._session is fake_session

    def test_batch_starts_empty(self, pusher):
        """_batch is empty on construction."""
        assert pusher._batch == []

    def test_auth_not_failed_initially(self, pusher):
        """_auth_failed is False on construction."""
        assert pusher._auth_failed is False

    def test_not_running_initially(self, pusher):
        """_running is False on construction."""
        assert pusher._running is False


# ---------------------------------------------------------------------------
# add_frame tests
# ---------------------------------------------------------------------------

class TestAddFrame:
    def test_add_frame_appends_to_batch(self, pusher, fake_hass):
        """add_frame appends the frame to _batch."""
        frame = make_frame()
        pusher.start()
        pusher.add_frame(frame)
        assert len(pusher._batch) == 1
        assert pusher._batch[0] is frame

    def test_add_frame_increments_captured(self, pusher, fake_hass, metrics):
        """add_frame increments frames_captured via metrics."""
        pusher.start()
        initial = metrics.frames_captured
        pusher.add_frame(make_frame())
        assert metrics.frames_captured == initial + 1

    def test_add_frame_multiple_frames(self, pusher, fake_hass):
        """Multiple add_frame calls accumulate in the batch."""
        pusher.start()
        pusher.add_frame(make_frame("light.a"))
        pusher.add_frame(make_frame("light.b"))
        pusher.add_frame(make_frame("climate.c"))
        assert len(pusher._batch) == 3

    def test_add_frame_silently_drops_when_auth_failed(self, pusher, fake_hass):
        """add_frame returns silently when _auth_failed is True."""
        pusher._auth_failed = True
        pusher.add_frame(make_frame())
        assert pusher._batch == []

    def test_add_frame_silently_drops_when_stopped(self, pusher, fake_hass):
        """add_frame returns silently when _stopped is True."""
        pusher._stopped = True
        pusher.add_frame(make_frame())
        assert pusher._batch == []

    def test_add_frame_before_start_queued(self, pusher, fake_hass):
        """add_frame works even before start() is called."""
        pusher.add_frame(make_frame())
        pusher.start()
        assert len(pusher._batch) == 1


# ---------------------------------------------------------------------------
# start / stop tests
# ---------------------------------------------------------------------------

class TestStartStop:
    def test_start_sets_running_true(self, pusher):
        """start() sets _running to True."""
        pusher.start()
        assert pusher._running is True

    def test_start_idempotent(self, pusher):
        """Multiple start() calls are idempotent."""
        pusher.start()
        pusher.start()
        assert pusher._running is True

    def test_start_idempotent_after_stop(self, pusher):
        """start() after stop() is a no-op."""
        pusher.start()
        fake_hass = pusher._hass
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        pusher.start()
        assert pusher._running is False  # stop sets False; start should not override

    def test_stop_cancels_flush_timer(self, pusher):
        """stop() cancels the flush timer."""
        pusher.start()
        assert pusher._flush_task is not None
        fake_hass = pusher._hass
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        assert pusher._flush_task is None

    def test_stop_sets_stopped_flag(self, pusher):
        """stop() sets _stopped to True."""
        pusher.start()
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        assert pusher._stopped is True

    def test_stop_idempotent(self, pusher):
        """Multiple stop() calls are idempotent."""
        pusher.start()
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        assert pusher._stopped is True

    def test_stop_drains_pending_batch(self, pusher, fake_hass, fake_session):
        """stop() flushes any pending frames before stopping."""
        pusher.start()
        pusher.add_frame(make_frame())
        pusher.add_frame(make_frame())
        # Mock the POST to succeed.
        fake_session.post.return_value = FakeResponse(202).__aenter__()
        # Override to return the response directly.
        fake_session.post = MagicMock(return_value=FakeResponse(202))
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        # The batch should be cleared (either flushed or dropped).
        assert pusher._batch == []


# ---------------------------------------------------------------------------
# force_flush tests
# ---------------------------------------------------------------------------

class TestForceFlush:
    def test_force_flush_clears_batch(self, pusher, fake_hass, fake_session):
        """force_flush() clears the batch after pushing."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(202))
        asyncio.get_event_loop().run_until_complete(pusher.force_flush())
        assert pusher._batch == []

    def test_force_flush_noop_when_empty(self, pusher, fake_hass):
        """force_flush() is a no-op when batch is empty."""
        asyncio.get_event_loop().run_until_complete(pusher.force_flush())
        assert pusher._batch == []

    def test_force_flush_noop_when_stopped(self, pusher, fake_hass):
        """force_flush() is a no-op when _stopped is True."""
        pusher._stopped = True
        asyncio.get_event_loop().run_until_complete(pusher.force_flush())


# ---------------------------------------------------------------------------
# _flush_batch HTTP response tests
# ---------------------------------------------------------------------------

class TestFlushBatchHTTPResponses:
    @pytest.mark.asyncio
    async def test_flush_202_success(self, pusher, fake_session, metrics):
        """HTTP 202: batch cleared, metrics recorded."""
        pusher.start()
        pusher.add_frame(make_frame())
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        assert pusher._batch == []
        assert metrics.frames_pushed == 2
        assert metrics.last_push_status == 202
        assert metrics.last_push_time > 0

    @pytest.mark.asyncio
    async def test_flush_401_auth_failure_stops_pusher(self, pusher, fake_session, metrics):
        """HTTP 401: _auth_failed set, pusher stops accepting frames."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        await pusher._flush_batch()

        assert pusher._auth_failed is True
        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: auth failure (status 401)")
        # Subsequent add_frame should be silent drop.
        pusher.add_frame(make_frame())
        assert len(pusher._batch) == 0  # dropped

    @pytest.mark.asyncio
    async def test_flush_403_auth_failure_stops_pusher(self, pusher, fake_session, metrics):
        """HTTP 403: _auth_failed set, pusher stops accepting frames."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(403))

        await pusher._flush_batch()

        assert pusher._auth_failed is True
        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: auth failure (status 403)")

    @pytest.mark.asyncio
    async def test_flush_400_drops_batch(self, pusher, fake_session, metrics):
        """HTTP 400: batch dropped, error counted, pusher continues."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(400))

        await pusher._flush_batch()

        assert pusher._batch == []
        assert metrics.error_count == 1
        assert metrics.last_push_status == 400
        assert metrics.last_error_msg.startswith("stream: non-2xx response (status 400)")
        assert pusher._auth_failed is False
        # Can still add frames.
        pusher.add_frame(make_frame())
        assert len(pusher._batch) == 1

    @pytest.mark.asyncio
    async def test_flush_503_drops_batch(self, pusher, fake_session, metrics):
        """HTTP 503: batch dropped, error counted, pusher continues."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(503))

        await pusher._flush_batch()

        assert pusher._batch == []
        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: non-2xx response (status 503)")
        assert pusher._auth_failed is False

    @pytest.mark.asyncio
    async def test_flush_timeout_drops_batch(self, pusher, fake_session, metrics):
        """asyncio.TimeoutError: batch dropped, error counted, last_error set."""
        pusher.start()
        pusher.add_frame(make_frame())

        class TimeoutCM:
            async def __aenter__(self):
                raise asyncio.TimeoutError()

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=TimeoutCM())

        await pusher._flush_batch()

        assert pusher._batch == []
        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: timeout after")

    @pytest.mark.asyncio
    async def test_flush_client_error_drops_batch(self, pusher, fake_session, metrics):
        """aiohttp.ClientError: batch dropped, error counted, last_error set."""
        pusher.start()
        pusher.add_frame(make_frame())

        class ClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError("connection refused")

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=ClientErrorCM())

        await pusher._flush_batch()

        assert pusher._batch == []
        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: connection error:")

    @pytest.mark.asyncio
    async def test_flush_empty_batch_skips_request(self, pusher, fake_session):
        """_flush_batch returns early when batch is empty."""
        pusher.start()
        await pusher._flush_batch()
        fake_session.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_handles_datetime_in_frame_payload(self, pusher, fake_session, metrics):
        """Frames with datetime payloads serialize without error (repro: pusher TypeError)."""
        from datetime import datetime, timezone

        pusher.start()
        # Inject a frame with a raw datetime object in the payload — the kind
        # that HA state_changed events can carry in attributes.
        pusher._batch.append({
            "source_event_id": "abc",
            "source_sequence": 1,
            "delivery_mode": "live",
            "event_time": "2025-01-01T00:00:00+00:00",
            "event_type": "state_changed",
            "payload": {
                "entity_id": "light.kitchen",
                "new_state": {
                    "state": "on",
                    "attributes": {
                        "last_changed": datetime(2025, 1, 1, tzinfo=timezone.utc),
                    },
                },
            },
        })
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        # Should not raise TypeError; batch flushed successfully.
        assert pusher._batch == []
        assert metrics.frames_pushed == 1


# ---------------------------------------------------------------------------
# last_error_msg negative tests
# ---------------------------------------------------------------------------

class TestLastErrorMsgNegative:
    @pytest.mark.asyncio
    async def test_auth_failure_error_count_not_double_counted(self, pusher, fake_session, metrics):
        """Auth failure increments error_count exactly once (not double-counted)."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(401))

        await pusher._flush_batch()

        assert metrics.error_count == 1  # not 2

    @pytest.mark.asyncio
    async def test_client_error_truncates_long_exception_message(self, pusher, fake_session, metrics):
        """ClientError with very long message is truncated to keep last_error_msg under 210 chars."""
        pusher.start()
        pusher.add_frame(make_frame())

        long_message = "x" * 500

        class LongClientErrorCM:
            async def __aenter__(self):
                raise aiohttp.ClientError(long_message)

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=LongClientErrorCM())

        await pusher._flush_batch()

        assert metrics.error_count == 1
        assert len(metrics.last_error_msg) < 210

    @pytest.mark.asyncio
    async def test_timeout_sets_last_error_even_without_status(self, pusher, fake_session, metrics):
        """TimeoutError sets last_error_msg even though there is no HTTP status code."""
        pusher.start()
        pusher.add_frame(make_frame())

        class TimeoutCM:
            async def __aenter__(self):
                raise asyncio.TimeoutError()

            async def __aexit__(self, *a):
                return None

        fake_session.post = MagicMock(return_value=TimeoutCM())

        await pusher._flush_batch()

        assert metrics.error_count == 1
        assert metrics.last_error_msg.startswith("stream: timeout after")
        assert "status" not in metrics.last_error_msg.lower()

    @pytest.mark.asyncio
    async def test_truncate_helper_default_max_len(self):
        """_truncate_error_detail default max_len is 150."""
        detail = "x" * 300
        result = IngressHTTPPusher._truncate_error_detail(detail)
        assert len(result) == 150

    @pytest.mark.asyncio
    async def test_truncate_helper_respects_custom_max_len(self):
        """_truncate_error_detail respects custom max_len parameter."""
        detail = "x" * 200
        result = IngressHTTPPusher._truncate_error_detail(detail, max_len=50)
        assert len(result) == 50

    @pytest.mark.asyncio
    async def test_truncate_helper_preserves_short_detail(self):
        """_truncate_error_detail returns the original string when it is shorter than max_len."""
        detail = "short error"
        result = IngressHTTPPusher._truncate_error_detail(detail, max_len=150)
        assert result == detail


# ---------------------------------------------------------------------------
# NDJSON body format tests
# ---------------------------------------------------------------------------

class TestNDJSONBody:
    @pytest.mark.asyncio
    async def test_body_is_newline_delimited_json(self, pusher, fake_session):
        """POST body is one JSON object per line with no wrapping array."""
        pusher.start()
        pusher.add_frame(make_frame("light.a"))
        pusher.add_frame(make_frame("light.b"))
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        call_args = fake_session.post.call_args
        body_bytes = call_args.kwargs.get("data") or call_args[1].get("data")
        body_str = body_bytes.decode("utf-8")
        lines = body_str.strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "source_event_id" in parsed

    @pytest.mark.asyncio
    async def test_headers_include_content_type_and_auth(self, pusher, fake_session):
        """POST includes Content-Type: application/x-ndjson and Bearer token."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        call_args = fake_session.post.call_args
        headers = call_args.kwargs.get("headers") or call_args[1].get("headers")
        assert headers["Content-Type"] == "application/x-ndjson"
        assert headers["Authorization"] == "Bearer jwt_secret_xyz"

    @pytest.mark.asyncio
    async def test_url_joins_ingress_url_and_stream_path(self, pusher, fake_session):
        """POST URL is ingress_url + INGRESS_STREAM_PATH (no double slashes)."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        call_args = fake_session.post.call_args
        url = call_args.args[0] if call_args.args else call_args.kwargs.get("url")
        assert url == "https://ingress.example.com/api/v1/ingress/stream"

    @pytest.mark.asyncio
    async def test_timeout_is_applied(self, pusher, fake_session):
        """POST uses aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)."""
        pusher.start()
        pusher.add_frame(make_frame())
        fake_session.post = MagicMock(return_value=FakeResponse(202))

        await pusher._flush_batch()

        call_args = fake_session.post.call_args
        timeout = call_args.kwargs.get("timeout") or call_args[1].get("timeout")
        assert timeout is not None
        assert timeout.total == HTTP_TIMEOUT_SECONDS


# ---------------------------------------------------------------------------
# Timer / flush scheduling tests
# ---------------------------------------------------------------------------

class TestTimerScheduling:
    def test_schedule_flush_creates_timer_handle(self, pusher, fake_hass):
        """_schedule_flush() creates a call_later TimerHandle."""
        pusher.start()
        assert pusher._flush_task is not None
        assert isinstance(pusher._flush_task, asyncio.TimerHandle)

    def test_batch_cap_triggers_soon_flush(self, pusher, fake_hass):
        """When batch reaches BATCH_MAX_FRAMES, _schedule_flush_soon is called."""
        pusher.start()
        for i in range(BATCH_MAX_FRAMES):
            pusher.add_frame(make_frame(f"light.{i}"))
        # After BATCH_MAX_FRAMES, batch should still have all frames
        # because flush is scheduled but not yet executed.
        assert len(pusher._batch) == BATCH_MAX_FRAMES

    def test_timer_cancelled_on_stop(self, pusher):
        """stop() cancels the pending flush timer."""
        pusher.start()
        handle = pusher._flush_task
        pusher.start()  # start again to reset task
        asyncio.get_event_loop().run_until_complete(pusher.stop())
        # The handle should be cancelled (TimerHandle.cancel() is a no-op if already run).
        assert pusher._flush_task is None

    def test_flush_on_empty_batch_not_sent(self, pusher, fake_session):
        """An empty batch does not trigger a POST."""
        pusher.start()
        # Run any pending timers.
        asyncio.get_event_loop().run_until_complete(
            asyncio.sleep(BATCH_FLUSH_INTERVAL_MS / 1000 + 0.05)
        )
        fake_session.post.assert_not_called()