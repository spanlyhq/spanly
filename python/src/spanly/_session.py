"""ASGI middlewares for Streamable HTTP concerns the message streams never
see: synthetic session ID injection and DELETE session termination capture."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

from spanly._packet import (
    SESSION_TERMINATED_METHOD,
    HttpTransportContext,
    SpanlyPacketContext,
)
from spanly._sender import AsyncSender
from spanly._transport import _redact_headers

if TYPE_CHECKING:
    from spanly._client import MonitorOptions, SpanlyClient

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


class SessionTerminationMiddleware:
    """ASGI middleware that records explicit session termination.

    MCP Streamable HTTP clients end a session by sending an HTTP DELETE
    with the ``Mcp-Session-Id`` header; no JSON-RPC message crosses the
    wire in either direction, so the stream interceptors never see it.
    This middleware watches DELETE requests that carry a session header
    and, once the server confirms with a 2xx response, emits a synthetic
    :data:`SESSION_TERMINATED_METHOD` packet.
    """

    def __init__(
        self,
        app: Any,
        client: SpanlyClient,
        options: MonitorOptions | None = None,
    ) -> None:
        self.app = app
        self._client = client
        self._options = options
        # Keep strong references so fire-and-forget emits aren't GC'd.
        self._tasks: set[asyncio.Task[None]] = set()

    def _emit(self, scope: Any, session_id: str) -> None:
        headers = {
            key.decode("latin-1"): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        options = self._options
        transport_context = HttpTransportContext(
            http_method="delete",
            path=scope.get("path", "/"),
            headers=_redact_headers(
                headers, options.redact_headers if options else None
            ),
        )
        client_addr = scope.get("client")
        if client_addr:
            transport_context.remote_address = client_addr[0]
            transport_context.remote_port = client_addr[1]

        context = SpanlyPacketContext(
            spanly_client_id=self._client.client_id,
            spanly_monitor_id=str(uuid.uuid4()),
        )
        mcp_packet: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": SESSION_TERMINATED_METHOD,
            "params": {"sessionId": session_id},
        }

        async def send_and_flush() -> None:
            sender = AsyncSender(url=self._client.url, api_key=self._client.api_key)
            try:
                sender.schedule_packet(
                    "from-client", context, mcp_packet, transport_context, options
                )
            finally:
                await sender.close()

        task = asyncio.create_task(send_and_flush())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("method") != "DELETE":
            await self.app(scope, receive, send)
            return

        session_id = ""
        for key, value in scope.get("headers", []):
            if key.lower() == SESSION_ID_HEADER:
                session_id = value.decode("latin-1")
                break
        if not session_id:
            await self.app(scope, receive, send)
            return

        async def wrapped_send(message: Any) -> None:
            if (
                message["type"] == "http.response.start"
                and 200 <= message["status"] < 300
            ):
                try:
                    self._emit(scope, session_id)
                except Exception:
                    logger.debug(
                        "Failed to emit session termination packet", exc_info=True
                    )
            await send(message)

        await self.app(scope, receive, wrapped_send)


def patch_streamable_http_app(
    server: Any,
    client: SpanlyClient | None = None,
    options: MonitorOptions | None = None,
) -> None:
    """Wrap a server's ``streamable_http_app`` builder with the middlewares.

    Works for FastMCP (and any server exposing a ``streamable_http_app()``
    factory). Servers wired manually into Starlette/ASGI can apply
    :class:`SessionIdInjectorMiddleware` and
    :class:`SessionTerminationMiddleware` themselves. No-op when the
    attribute is missing or already patched.

    Session ID injection is applied unless ``options.inject_session_id``
    is false; termination capture is applied whenever a ``client`` is
    given.
    """
    builder = getattr(server, "streamable_http_app", None)
    if builder is None or not callable(builder):
        return
    if getattr(builder, "_spanly_session_id_patch", False):
        return

    inject_session_id = options.inject_session_id if options else True

    def patched(*args: Any, **kwargs: Any) -> Any:
        app = builder(*args, **kwargs)
        if client is not None:
            app = SessionTerminationMiddleware(app, client, options)
        if inject_session_id:
            app = SessionIdInjectorMiddleware(app)
        return app

    patched._spanly_session_id_patch = True  # type: ignore[attr-defined]
    try:
        server.streamable_http_app = patched
    except AttributeError:
        logger.debug("Could not patch streamable_http_app", exc_info=True)
