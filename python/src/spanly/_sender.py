"""Async fire-and-forget HTTP sender for telemetry packets."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

import httpx

from spanly._packet import SpanlyPacket, SpanlyPacketContext
from spanly._transport import extract_transport_context, session_message_to_dict

if TYPE_CHECKING:
    from spanly._client import MonitorOptions

logger = logging.getLogger("spanly")


class AsyncSender:
    """Sends packets to the Spanly ingest server without blocking MCP operations."""

    def __init__(self, url: str, api_key: str) -> None:
        self._url = url
        self._api_key = api_key
        self._http_client = httpx.AsyncClient(timeout=10.0)
        self._tasks: set[asyncio.Task[None]] = set()

    def schedule(
        self,
        direction: str,
        context: SpanlyPacketContext,
        session_message: Any,
        options: MonitorOptions | None,
    ) -> None:
        """Schedule a fire-and-forget send for a captured SessionMessage."""
        mcp_packet = session_message_to_dict(session_message)
        if mcp_packet is None:
            return

        if mcp_packet.get("jsonrpc") != "2.0":
            return

        if options and options.on_collect:
            result = options.on_collect(direction, context, mcp_packet)
            if result is None:
                return
            mcp_packet = result

        transport_context = extract_transport_context(session_message)

        packet = SpanlyPacket(
            timestamp=int(time.time() * 1000),
            direction=direction,  # type: ignore[arg-type]
            context=context,
            transport_context=transport_context,
            mcp_packet=mcp_packet,
        )

        task = asyncio.create_task(self._send(packet, options))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _send(
        self,
        packet: SpanlyPacket,
        options: MonitorOptions | None,
    ) -> None:
        try:
            response = await self._http_client.post(
                f"{self._url}/collect",
                json=packet.to_dict(),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._api_key}",
                },
            )

            if response.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"Ingest server returned {response.status_code}",
                    request=response.request,
                    response=response,
                )

            result = response.json()
            warnings = result.get("warnings")
            if warnings and options and options.on_warning:
                options.on_warning(warnings)

        except Exception as exc:
            if options and options.on_error:
                options.on_error(exc)
            else:
                logger.warning("Error sending packet to Spanly ingest server: %s", exc)

    async def close(self) -> None:
        """Wait for pending tasks and close the HTTP client."""
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._http_client.aclose()
