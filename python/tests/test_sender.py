"""Tests for AsyncSender."""

import asyncio
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from spanly._client import MonitorOptions
from spanly._packet import SpanlyPacketContext
from spanly._sender import AsyncSender


@dataclass
class FakeSessionMessage:
    message: Any
    metadata: Any = None


@pytest.mark.asyncio
async def test_sender_drops_non_jsonrpc():
    sender = AsyncSender(url="http://localhost:3002", api_key="spanly_test")

    mock_message = MagicMock()
    mock_message.model_dump.return_value = {"not": "jsonrpc"}
    session_msg = FakeSessionMessage(message=mock_message)

    context = SpanlyPacketContext(
        spanly_client_id="c",
        spanly_monitor_id="m",
    )

    with patch.object(sender._http_client, "post", new_callable=AsyncMock) as mock_post:
        sender.schedule("from-client", context, session_msg, None)
        await asyncio.sleep(0.1)

        # Should not have sent anything
        mock_post.assert_not_called()

    await sender.close()


@pytest.mark.asyncio
async def test_sender_on_collect_can_drop_packet():
    sender = AsyncSender(url="http://localhost:3002", api_key="spanly_test")

    mock_message = MagicMock()
    mock_message.model_dump.return_value = {"jsonrpc": "2.0", "method": "ping", "id": 1}
    session_msg = FakeSessionMessage(message=mock_message)

    context = SpanlyPacketContext(spanly_client_id="c", spanly_monitor_id="m")
    options = MonitorOptions(on_collect=lambda d, c, p: None)  # Drop all packets

    with patch.object(sender._http_client, "post", new_callable=AsyncMock) as mock_post:
        sender.schedule("from-client", context, session_msg, options)
        await asyncio.sleep(0.1)
        mock_post.assert_not_called()

    await sender.close()


@pytest.mark.asyncio
async def test_sender_calls_on_error():
    sender = AsyncSender(url="http://localhost:3002", api_key="spanly_test")

    mock_message = MagicMock()
    mock_message.model_dump.return_value = {"jsonrpc": "2.0", "method": "ping", "id": 1}
    session_msg = FakeSessionMessage(message=mock_message)

    context = SpanlyPacketContext(spanly_client_id="c", spanly_monitor_id="m")
    errors: list[Exception] = []
    options = MonitorOptions(on_error=lambda e: errors.append(e))

    with patch.object(
        sender._http_client,
        "post",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("connection refused"),
    ):
        sender.schedule("from-client", context, session_msg, options)
        await asyncio.sleep(0.1)

        assert len(errors) == 1
        assert "connection refused" in str(errors[0])

    await sender.close()


@pytest.mark.asyncio
async def test_sender_calls_on_warning():
    sender = AsyncSender(url="http://localhost:3002", api_key="spanly_test")

    mock_message = MagicMock()
    mock_message.model_dump.return_value = {"jsonrpc": "2.0", "method": "ping", "id": 1}
    session_msg = FakeSessionMessage(message=mock_message)

    context = SpanlyPacketContext(spanly_client_id="c", spanly_monitor_id="m")
    warnings_received: list[Any] = []
    options = MonitorOptions(on_warning=lambda w: warnings_received.extend(w))

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "success": True,
        "warnings": [{"code": "SESSION_ID_HASHED", "message": "Session ID was hashed"}],
    }

    with patch.object(sender._http_client, "post", new_callable=AsyncMock, return_value=mock_response):
        sender.schedule("from-client", context, session_msg, options)
        await asyncio.sleep(0.1)

        assert len(warnings_received) == 1
        assert warnings_received[0]["code"] == "SESSION_ID_HASHED"

    await sender.close()


@pytest.mark.asyncio
async def test_sender_close_waits_for_pending_tasks():
    sender = AsyncSender(url="http://localhost:3002", api_key="spanly_test")

    mock_message = MagicMock()
    mock_message.model_dump.return_value = {"jsonrpc": "2.0", "method": "ping", "id": 1}
    session_msg = FakeSessionMessage(message=mock_message)

    context = SpanlyPacketContext(spanly_client_id="c", spanly_monitor_id="m")

    call_count = 0

    async def slow_post(*args: Any, **kwargs: Any) -> MagicMock:
        nonlocal call_count
        await asyncio.sleep(0.05)
        call_count += 1
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"success": True}
        return resp

    with patch.object(sender._http_client, "post", side_effect=slow_post):
        sender.schedule("from-client", context, session_msg, None)
        sender.schedule("to-client", context, session_msg, None)

        # close() should wait for both tasks
        await sender.close()
        assert call_count == 2
