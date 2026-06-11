"""Synthetic MCP session ID injection for sessionless Streamable HTTP servers."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

logger = logging.getLogger("spanly")

SESSION_ID_HEADER = b"mcp-session-id"
SYNTHETIC_SESSION_ID_PREFIX = "spanly-"

# Initialize requests are small JSON objects; anything past this bound is
# not buffered for inspection and is treated as non-initialize.
_MAX_INITIALIZE_BODY_BYTES = 64 * 1024


def new_synthetic_session_id() -> str:
    return f"{SYNTHETIC_SESSION_ID_PREFIX}{uuid.uuid4().hex}"


def _contains_initialize(body: bytes) -> bool:
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    items = parsed if isinstance(parsed, list) else [parsed]
    return any(
        isinstance(item, dict) and item.get("method") == "initialize" for item in items
    )


def _has_session_header(headers: Any) -> bool:
    return any(key.lower() == SESSION_ID_HEADER for key, _ in headers)


class SessionIdInjectorMiddleware:
    """ASGI middleware that assigns a synthetic ``Mcp-Session-Id``.

    When the wrapped MCP server runs sessionless (stateless Streamable
    HTTP), initialize responses carry no session ID and Spanly cannot
    group requests into sessions. This middleware watches POST requests
    that have no session header, and when the body is an MCP initialize
    request and the response is successful without a server-assigned
    session ID, appends a synthetic one. Compliant clients echo it on
    subsequent requests, which the stateless server ignores.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or _has_session_header(scope.get("headers", []))
        ):
            await self.app(scope, receive, send)
            return

        state = {"buffer": bytearray(), "overflow": False, "is_initialize": False}

        async def wrapped_receive() -> Any:
            message = await receive()
            if message["type"] == "http.request" and not state["overflow"]:
                buffer = state["buffer"]
                buffer.extend(message.get("body", b""))
                if len(buffer) > _MAX_INITIALIZE_BODY_BYTES:
                    state["overflow"] = True
                    buffer.clear()
                elif not message.get("more_body", False):
                    state["is_initialize"] = _contains_initialize(bytes(buffer))
                    buffer.clear()
            return message

        async def wrapped_send(message: Any) -> None:
            if (
                message["type"] == "http.response.start"
                and state["is_initialize"]
                and 200 <= message["status"] < 300
            ):
                headers = list(message.get("headers", []))
                if not _has_session_header(headers):
                    headers.append(
                        (SESSION_ID_HEADER, new_synthetic_session_id().encode("ascii"))
                    )
                    message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, wrapped_receive, wrapped_send)


def patch_streamable_http_app(server: Any) -> None:
    """Wrap a server's ``streamable_http_app`` builder with the middleware.

    Works for FastMCP (and any server exposing a ``streamable_http_app()``
    factory). Servers wired manually into Starlette/ASGI can apply
    :class:`SessionIdInjectorMiddleware` themselves. No-op when the
    attribute is missing or already patched.
    """
    builder = getattr(server, "streamable_http_app", None)
    if builder is None or not callable(builder):
        return
    if getattr(builder, "_spanly_session_id_patch", False):
        return

    def patched(*args: Any, **kwargs: Any) -> Any:
        app = builder(*args, **kwargs)
        return SessionIdInjectorMiddleware(app)

    patched._spanly_session_id_patch = True  # type: ignore[attr-defined]
    try:
        server.streamable_http_app = patched
    except AttributeError:
        logger.debug("Could not patch streamable_http_app", exc_info=True)
