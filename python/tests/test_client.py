"""Tests for SpanlyClient."""

import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spanly._client import SpanlyClient, MonitorOptions, _resolve_low_level_server, _get_server_info, _parse_region_from_api_key
from spanly._packet import McpServerInfo


class FakeServer:
    """A simple fake low-level Server for testing (avoids MagicMock auto-attribute issues)."""

    def __init__(self, name: str = "test-server", version: str = "1.0.0") -> None:
        self.name = name
        self.version = version
        self.run = self._default_run

    async def _default_run(self, *args: Any, **kwargs: Any) -> None:
        pass


# --- SpanlyClient init tests ---


def test_client_requires_api_key():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="API key is required"):
            SpanlyClient()


def test_client_uses_explicit_api_key():
    client = SpanlyClient(api_key="spanly_us_test123")
    assert client.api_key == "spanly_us_test123"


def test_client_uses_env_api_key():
    with patch.dict(os.environ, {"SPANLY_API_KEY": "spanly_us_from_env"}):
        client = SpanlyClient()
        assert client.api_key == "spanly_us_from_env"


def test_client_default_url_us():
    client = SpanlyClient(api_key="spanly_us_test")
    assert client.url == "https://ingest.us.spanly.com"


def test_client_default_url_eu():
    client = SpanlyClient(api_key="spanly_eu_test")
    assert client.url == "https://ingest.eu.spanly.com"


def test_client_custom_ingest_url():
    client = SpanlyClient(
        api_key="spanly_us_test",
        ingest_url=lambda region: f"http://localhost:3002/{region}",
    )
    assert client.url == "http://localhost:3002/us"


def test_client_invalid_api_key_region():
    with pytest.raises(ValueError, match="Invalid API key format"):
        SpanlyClient(api_key="spanly_xx_test")


def test_client_generates_uuid():
    client = SpanlyClient(api_key="spanly_us_test")
    assert len(client.client_id) == 36  # UUID format


def test_parse_region_from_api_key():
    assert _parse_region_from_api_key("spanly_us_abc") == "us"
    assert _parse_region_from_api_key("spanly_eu_abc") == "eu"
    with pytest.raises(ValueError):
        _parse_region_from_api_key("spanly_invalid")


# --- resolve / get_server_info tests ---


def test_resolve_low_level_server_from_mcp_server():
    low_level = FakeServer()

    class FakeHighLevel:
        _lowlevel_server = low_level

    assert _resolve_low_level_server(FakeHighLevel()) is low_level


def test_resolve_low_level_server_passthrough():
    server = FakeServer()
    assert _resolve_low_level_server(server) is server


def test_get_server_info():
    server = FakeServer(name="my-server", version="2.0.0")
    info = _get_server_info(server)
    assert info == McpServerInfo(name="my-server", version="2.0.0")


def test_get_server_info_no_name():
    server = MagicMock(spec=[])
    assert _get_server_info(server) is None


def test_get_server_info_no_version():
    server = FakeServer(name="my-server", version=None)  # type: ignore[arg-type]
    info = _get_server_info(server)
    assert info == McpServerInfo(name="my-server", version="unknown")


# --- monitor() tests ---


@pytest.mark.asyncio
async def test_monitor_patches_run():
    client = SpanlyClient(api_key="spanly_us_test")

    call_log: list[tuple[Any, ...]] = []

    async def original_run(*args: Any, **kwargs: Any) -> None:
        call_log.append(args)

    server = FakeServer()
    server.run = original_run

    client.monitor(server)

    # run should be replaced
    assert server.run is not original_run

    read_stream = MagicMock()
    write_stream = MagicMock()
    init_options = MagicMock()

    with patch("spanly._client.AsyncSender") as MockSender:
        mock_sender = MagicMock()
        mock_sender.close = AsyncMock()
        mock_sender.schedule = MagicMock()
        MockSender.return_value = mock_sender

        await server.run(read_stream, write_stream, init_options)

        # original_run should have been called with wrapped streams
        assert len(call_log) == 1
        args = call_log[0]
        assert args[0] is not read_stream  # wrapped read
        assert args[1] is not write_stream  # wrapped write
        assert args[2] is init_options
        # Sender should be closed
        mock_sender.close.assert_called_once()


@pytest.mark.asyncio
async def test_monitor_closes_sender_on_error():
    client = SpanlyClient(api_key="spanly_us_test")

    async def failing_run(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("server crashed")

    server = FakeServer()
    server.run = failing_run

    client.monitor(server)

    with patch("spanly._client.AsyncSender") as MockSender:
        mock_sender = MagicMock()
        mock_sender.close = AsyncMock()
        MockSender.return_value = mock_sender

        with pytest.raises(RuntimeError, match="server crashed"):
            await server.run(MagicMock(), MagicMock(), MagicMock())

        # Sender should still be closed even on error
        mock_sender.close.assert_called_once()
