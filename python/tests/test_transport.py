"""Tests for stream interceptors and helpers."""

import asyncio
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from spanly._packet import HttpTransportContext, StdioTransportContext
from spanly._transport import (
    InterceptedReceiveStream,
    InterceptedSendStream,
    extract_transport_context,
    session_message_to_dict,
)


# --- Fake stream helpers ---


class FakeReceiveStream:
    def __init__(self, items: list[Any]) -> None:
        self._items = list(items)
        self._index = 0

    async def receive(self) -> Any:
        if self._index >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._index]
        self._index += 1
        return item

    async def aclose(self) -> None:
        pass


class FakeSendStream:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    async def send(self, item: Any) -> None:
        self.sent.append(item)

    async def aclose(self) -> None:
        pass


# --- InterceptedReceiveStream / InterceptedSendStream tests ---
#
# Happy-path interception is exercised end-to-end by the Python E2E MCP server
# (apps/e2e-mcp-python + apps/e2e). Only callback-error invariants live here.


@pytest.mark.asyncio
async def test_receive_stream_callback_error_does_not_propagate():
    def failing_callback(msg: Any) -> None:
        raise RuntimeError("callback failed")

    inner = FakeReceiveStream(["msg1"])
    stream = InterceptedReceiveStream(inner, on_message=failing_callback)

    # Should still return the message despite callback failure
    result = await stream.receive()
    assert result == "msg1"


@pytest.mark.asyncio
async def test_send_stream_callback_error_does_not_block_send():
    def failing_callback(msg: Any) -> None:
        raise RuntimeError("callback failed")

    inner = FakeSendStream()
    stream = InterceptedSendStream(inner, on_message=failing_callback)

    await stream.send("msg1")
    assert inner.sent == ["msg1"]


# --- extract_transport_context tests ---


def test_extract_transport_context_no_metadata():
    @dataclass
    class FakeMessage:
        message: Any = None
        metadata: Any = None

    result = extract_transport_context(FakeMessage())
    assert isinstance(result, StdioTransportContext)


def test_extract_transport_context_with_starlette_request():
    @dataclass
    class FakeUrl:
        path: str = "/mcp"

    @dataclass
    class FakeRequest:
        method: str = "POST"
        url: FakeUrl = None  # type: ignore
        headers: dict[str, str] = None  # type: ignore

        def __post_init__(self) -> None:
            if self.url is None:
                self.url = FakeUrl()
            if self.headers is None:
                self.headers = {"content-type": "application/json"}

    @dataclass
    class FakeMetadata:
        request_context: Any = None

    @dataclass
    class FakeMessage:
        message: Any = None
        metadata: Any = None

    msg = FakeMessage(metadata=FakeMetadata(request_context=FakeRequest()))
    result = extract_transport_context(msg)

    assert isinstance(result, HttpTransportContext)
    assert result.http_method == "post"
    assert result.path == "/mcp"
    assert result.headers == {"content-type": "application/json"}


def _http_message(headers: dict[str, str]) -> Any:
    @dataclass
    class FakeUrl:
        path: str = "/mcp"

    @dataclass
    class FakeRequest:
        method: str = "POST"
        url: Any = None
        headers: Any = None

        def __post_init__(self) -> None:
            self.url = FakeUrl()

    @dataclass
    class FakeMetadata:
        request_context: Any = None

    @dataclass
    class FakeMessage:
        message: Any = None
        metadata: Any = None

    request = FakeRequest()
    request.headers = headers
    return FakeMessage(metadata=FakeMetadata(request_context=request))


def test_extract_transport_context_redacts_sensitive_headers():
    msg = _http_message(
        {
            "authorization": "Bearer super-secret",
            "cookie": "session=abc123",
            "set-cookie": "session=abc123",
            "proxy-authorization": "Basic dXNlcjpwYXNz",
            "x-api-key": "key-123",
            "content-type": "application/json",
            "mcp-session-id": "session-1",
        }
    )

    result = extract_transport_context(msg)

    assert isinstance(result, HttpTransportContext)
    assert result.headers == {
        "authorization": "[REDACTED]",
        "cookie": "[REDACTED]",
        "set-cookie": "[REDACTED]",
        "proxy-authorization": "[REDACTED]",
        "x-api-key": "[REDACTED]",
        "content-type": "application/json",
        "mcp-session-id": "session-1",
    }


def test_extract_transport_context_redacts_case_insensitively():
    msg = _http_message({"Authorization": "Bearer secret"})

    result = extract_transport_context(msg)

    assert isinstance(result, HttpTransportContext)
    assert result.headers == {"Authorization": "[REDACTED]"}


def test_extract_transport_context_redacts_extra_headers():
    msg = _http_message(
        {
            "x-custom-token": "secret-token",
            "accept": "application/json",
        }
    )

    result = extract_transport_context(msg, redact_headers=["X-Custom-Token"])

    assert isinstance(result, HttpTransportContext)
    assert result.headers == {
        "x-custom-token": "[REDACTED]",
        "accept": "application/json",
    }


# --- session_message_to_dict tests ---


def test_session_message_to_dict_with_model():
    mock_message = MagicMock()
    mock_message.model_dump.return_value = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": 1,
    }

    @dataclass
    class FakeSessionMessage:
        message: Any

    result = session_message_to_dict(FakeSessionMessage(message=mock_message))
    assert result == {"jsonrpc": "2.0", "method": "tools/list", "id": 1}
    mock_message.model_dump.assert_called_once_with(by_alias=True, exclude_unset=True)


def test_session_message_to_dict_failure_returns_none():
    @dataclass
    class FakeSessionMessage:
        message: Any = None

    result = session_message_to_dict(FakeSessionMessage(message=None))
    assert result is None
