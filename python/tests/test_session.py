"""Tests for synthetic session ID injection."""

import json
from typing import Any

from spanly._session import (
    SESSION_ID_HEADER,
    SYNTHETIC_SESSION_ID_PREFIX,
    SessionIdInjectorMiddleware,
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
