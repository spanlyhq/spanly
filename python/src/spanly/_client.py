"""SpanlyClient — main entry point for monitoring MCP servers."""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import anyio

from spanly._packet import SpanlyPacketContext
from spanly._sender import AsyncSender
from spanly._session import patch_streamable_http_app
from spanly._transport import InterceptedReceiveStream, InterceptedSendStream

logger = logging.getLogger("spanly")

SpanlyRegion = Literal["us", "eu"]

DEFAULT_INGEST_URLS: dict[SpanlyRegion, str] = {
    "us": "https://ingest.us.spanly.com",
    "eu": "https://ingest.eu.spanly.com",
}


def _parse_region_from_api_key(api_key: str) -> SpanlyRegion:
    if api_key.startswith("spanly_us_"):
        return "us"
    if api_key.startswith("spanly_eu_"):
        return "eu"
    raise ValueError("Invalid API key format: must start with spanly_us_ or spanly_eu_")


@dataclass
class CollectWarning:
    code: str
    message: str


@dataclass
class MonitorOptions:
    on_error: Callable[[Exception], None] | None = None
    on_warning: Callable[[list[CollectWarning]], None] | None = None
    on_collect: Callable[[str, SpanlyPacketContext, dict[str, Any]], dict[str, Any] | None] | None = None
    # Additional header names to redact from captured transport context,
    # on top of DEFAULT_REDACTED_HEADERS. Case-insensitive.
    redact_headers: list[str] | None = None
    # When the server runs sessionless (stateless Streamable HTTP), set a
    # synthetic Mcp-Session-Id header on initialize responses so requests
    # from the same client are grouped into a session in Spanly.
    inject_session_id: bool = True


def _resolve_low_level_server(server: Any) -> Any:
    """Accept both high-level and low-level servers, return the low-level Server."""
    # MCPServer (upcoming mcp releases) stores the low-level Server in
    # _lowlevel_server; FastMCP (mcp 1.x) stores it in _mcp_server.
    if hasattr(server, "_lowlevel_server"):
        return server._lowlevel_server
    if hasattr(server, "_mcp_server"):
        return server._mcp_server
    return server


class SpanlyClient:
    """Client for monitoring MCP servers and sending telemetry to Spanly.

    Usage:
        client = SpanlyClient()
        client.monitor(server)
        server.run()  # Packets are automatically captured
    """

    def __init__(
        self,
        api_key: str | None = None,
        ingest_url: Callable[[SpanlyRegion], str] | None = None,
    ) -> None:
        self.client_id = str(uuid.uuid4())

        resolved_key = api_key or os.environ.get("SPANLY_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Spanly API key is required. Pass it as `api_key` or "
                "set the SPANLY_API_KEY environment variable."
            )
        self.api_key = resolved_key

        region = _parse_region_from_api_key(resolved_key)
        self.url = ingest_url(region) if ingest_url else DEFAULT_INGEST_URLS[region]

    def monitor(
        self,
        server: Any,
        options: MonitorOptions | None = None,
    ) -> None:
        """Instrument an MCP server to capture JSON-RPC packets.

        Works with both the high-level MCPServer and the low-level Server.
        Must be called before the server starts (before server.run()).
        """
        low_level_server = _resolve_low_level_server(server)
        original_run = low_level_server.run
        client = self
        opts = options

        if options is None or options.inject_session_id:
            patch_streamable_http_app(server)

        async def patched_run(
            read_stream: Any,
            write_stream: Any,
            initialization_options: Any,
            raise_exceptions: bool = False,
            stateless: bool = False,
        ) -> None:
            monitor_id = str(uuid.uuid4())
            context = SpanlyPacketContext(
                spanly_client_id=client.client_id,
                spanly_monitor_id=monitor_id,
            )

            sender = AsyncSender(
                url=client.url,
                api_key=client.api_key,
            )

            wrapped_read = InterceptedReceiveStream(
                read_stream,
                on_message=lambda msg: sender.schedule("from-client", context, msg, opts),
            )
            wrapped_write = InterceptedSendStream(
                write_stream,
                on_message=lambda msg: sender.schedule("to-client", context, msg, opts),
            )

            try:
                await original_run(
                    wrapped_read,
                    wrapped_write,
                    initialization_options,
                    raise_exceptions=raise_exceptions,
                    stateless=stateless,
                )
            finally:
                # Shield from cancellation so pending send tasks can complete.
                # In stateless mode, the MCP session manager cancels its task
                # group right after the request is handled; without shielding,
                # sender.close() would be interrupted and packets would be lost.
                with anyio.CancelScope(shield=True):
                    await sender.close()

        low_level_server.run = patched_run
