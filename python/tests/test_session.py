"""Tests for synthetic session ID injection and termination capture."""

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from spanly._client import MonitorOptions, SpanlyClient
from spanly._packet import SESSION_TERMINATED_METHOD
from spanly._session import (
    SESSION_ID_HEADER,
    SYNTHETIC_SESSION_ID_PREFIX,
    SessionIdInjectorMiddleware,
    SessionTerminationMiddleware,
    _contains_initialize,
    patch_streamable_http_app,
)

INITIALIZE_BODY = json.dumps(
    {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
).encode()
TOOLS_CALL_BODY = json.dumps(
    {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}}
).encode()


def make_app(status: int = 200, response_headers: list | None = None):
    """Minimal ASGI app that reads the full body then responds."""

    async def app(scope: Any, receive: Any, send: Any) -> None:
        while True:
            message = await receive()
            if not message.get("more_body", False):
                break
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": response_headers or [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b"{}"})

    return app


async def run_middleware(
    app: Any,
    body: bytes,
    method: str = "POST",
    request_headers: list | None = None,
) -> list:
    middleware = SessionIdInjectorMiddleware(app)
    scope = {
        "type": "http",
        "method": method,
        "path": "/mcp",
        "headers": request_headers or [(b"content-type", b"application/json")],
    }
    incoming = [{"type": "http.request", "body": body, "more_body": False}]
    sent: list = []

    async def receive() -> Any:
        return incoming.pop(0)

    async def send(message: Any) -> None:
        sent.append(message)

    await middleware(scope, receive, send)
    return sent


def session_header(sent: list) -> bytes | None:
    start = next(m for m in sent if m["type"] == "http.response.start")
    for key, value in start["headers"]:
        if key.lower() == SESSION_ID_HEADER:
            return value
    return None


async def test_injects_synthetic_session_id_on_initialize():
    sent = await run_middleware(make_app(), INITIALIZE_BODY)
    value = session_header(sent)
    assert value is not None
    assert value.decode().startswith(SYNTHETIC_SESSION_ID_PREFIX)


async def test_does_not_inject_on_non_initialize():
    sent = await run_middleware(make_app(), TOOLS_CALL_BODY)
    assert session_header(sent) is None


async def test_does_not_inject_when_server_sets_session_id():
    app = make_app(
        response_headers=[
            (b"content-type", b"application/json"),
            (b"mcp-session-id", b"server-session"),
        ]
    )
    sent = await run_middleware(app, INITIALIZE_BODY)
    assert session_header(sent) == b"server-session"


async def test_does_not_inject_on_error_responses():
    sent = await run_middleware(make_app(status=400), INITIALIZE_BODY)
    assert session_header(sent) is None


async def test_skips_requests_that_already_carry_a_session_id():
    request_headers = [
        (b"content-type", b"application/json"),
        (b"mcp-session-id", b"existing"),
    ]
    sent = await run_middleware(
        make_app(), INITIALIZE_BODY, request_headers=request_headers
    )
    assert session_header(sent) is None


async def test_handles_chunked_request_bodies():
    middleware = SessionIdInjectorMiddleware(make_app())
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": [(b"content-type", b"application/json")],
    }
    half = len(INITIALIZE_BODY) // 2
    incoming = [
        {"type": "http.request", "body": INITIALIZE_BODY[:half], "more_body": True},
        {"type": "http.request", "body": INITIALIZE_BODY[half:], "more_body": False},
    ]
    sent: list = []

    async def receive() -> Any:
        return incoming.pop(0)

    async def send(message: Any) -> None:
        sent.append(message)

    await middleware(scope, receive, send)
    value = session_header(sent)
    assert value is not None
    assert value.decode().startswith(SYNTHETIC_SESSION_ID_PREFIX)


def test_contains_initialize():
    assert _contains_initialize(INITIALIZE_BODY)
    assert _contains_initialize(b'[{"method":"tools/call"},{"method":"initialize"}]')
    assert not _contains_initialize(TOOLS_CALL_BODY)
    assert not _contains_initialize(b"not json")
    assert not _contains_initialize(b"")


def test_patch_streamable_http_app_wraps_builder():
    class FakeFastMCP:
        def streamable_http_app(self):
            return make_app()

    server = FakeFastMCP()
    patch_streamable_http_app(server)
    app = server.streamable_http_app()
    assert isinstance(app, SessionIdInjectorMiddleware)

    # Re-patching is a no-op (no double wrapping).
    patch_streamable_http_app(server)
    assert getattr(server.streamable_http_app, "_spanly_session_id_patch", False)


def test_patch_is_noop_without_builder():
    class Bare:
        pass

    patch_streamable_http_app(Bare())


def _make_spanly_client() -> SpanlyClient:
    return SpanlyClient(api_key="spanly_us_test")


def test_patch_applies_both_middlewares_with_client():
    class FakeFastMCP:
        def streamable_http_app(self):
            return make_app()

    server = FakeFastMCP()
    patch_streamable_http_app(server, client=_make_spanly_client())
    app = server.streamable_http_app()
    assert isinstance(app, SessionIdInjectorMiddleware)
    assert isinstance(app.app, SessionTerminationMiddleware)


def test_patch_keeps_termination_capture_when_injection_disabled():
    class FakeFastMCP:
        def streamable_http_app(self):
            return make_app()

    server = FakeFastMCP()
    patch_streamable_http_app(
        server,
        client=_make_spanly_client(),
        options=MonitorOptions(inject_session_id=False),
    )
    app = server.streamable_http_app()
    assert isinstance(app, SessionTerminationMiddleware)


async def run_termination_middleware(
    status: int,
    request_headers: list | None = None,
    options: MonitorOptions | None = None,
) -> AsyncMock:
    """Run a DELETE through the middleware; returns the mocked httpx post."""
    middleware = SessionTerminationMiddleware(
        make_app(status=status), _make_spanly_client(), options
    )
    scope = {
        "type": "http",
        "method": "DELETE",
        "path": "/mcp",
        "headers": request_headers
        if request_headers is not None
        else [(b"mcp-session-id", b"session-1")],
        "client": ("127.0.0.1", 4242),
    }
    incoming = [{"type": "http.request", "body": b"", "more_body": False}]
    sent: list = []

    async def receive() -> Any:
        return incoming.pop(0)

    async def send(message: Any) -> None:
        sent.append(message)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True}

    with patch(
        "httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response
    ) as mock_post:
        await middleware(scope, receive, send)
        # The emit is fire-and-forget; let the pending task run to completion.
        await asyncio.sleep(0.1)

    return mock_post


async def test_records_termination_on_successful_delete():
    mock_post = await run_termination_middleware(status=200)

    mock_post.assert_called_once()
    packet = mock_post.call_args.kwargs["json"]
    assert packet["direction"] == "from-client"
    assert packet["mcpPacket"] == {
        "jsonrpc": "2.0",
        "method": SESSION_TERMINATED_METHOD,
        "params": {"sessionId": "session-1"},
    }
    assert packet["transportContext"]["httpMethod"] == "delete"
    assert packet["transportContext"]["headers"]["mcp-session-id"] == "session-1"
    assert packet["transportContext"]["remoteAddress"] == "127.0.0.1"


async def test_no_termination_when_delete_fails():
    mock_post = await run_termination_middleware(status=404)
    mock_post.assert_not_called()


async def test_no_termination_without_session_header():
    mock_post = await run_termination_middleware(
        status=200, request_headers=[(b"content-type", b"application/json")]
    )
    mock_post.assert_not_called()


async def test_termination_packet_passes_through_on_collect():
    seen: list[tuple[str, Any]] = []

    def on_collect(direction: str, context: Any, mcp_packet: dict) -> None:
        seen.append((direction, mcp_packet["method"]))
        return None

    mock_post = await run_termination_middleware(
        status=200, options=MonitorOptions(on_collect=on_collect)
    )

    assert seen == [("from-client", SESSION_TERMINATED_METHOD)]
    mock_post.assert_not_called()
