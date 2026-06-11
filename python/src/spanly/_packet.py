"""Packet dataclasses matching the Spanly ingest server's expected schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

# Synthetic method recorded when a client ends its session with an HTTP
# DELETE. Session termination is transport-level in MCP (no JSON-RPC
# message exists on the wire), so Spanly emits this packet itself; the
# ``spanly/`` prefix marks it as synthesized, like the ``spanly-`` session
# IDs.
SESSION_TERMINATED_METHOD = "spanly/session/terminated"


@dataclass
class SpanlyPacketContext:
    spanly_client_id: str
    spanly_monitor_id: str
    project_id: str | None = None
    environment_id: str | None = None
    organisation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "spanlyClientId": self.spanly_client_id,
            "spanlyMonitorId": self.spanly_monitor_id,
        }
        if self.project_id is not None:
            d["projectId"] = self.project_id
        if self.environment_id is not None:
            d["environmentId"] = self.environment_id
        if self.organisation_id is not None:
            d["organisationId"] = self.organisation_id
        return d


@dataclass
class StdioTransportContext:
    type: Literal["stdio"] = "stdio"

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type}


@dataclass
class HttpTransportContext:
    type: Literal["http"] = "http"
    http_method: str = "get"
    path: str = "/"
    headers: dict[str, str] = field(default_factory=dict)
    remote_address: str | None = None
    remote_port: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "type": self.type,
            "httpMethod": self.http_method,
            "path": self.path,
            "headers": self.headers,
        }
        if self.remote_address is not None:
            d["remoteAddress"] = self.remote_address
        if self.remote_port is not None:
            d["remotePort"] = self.remote_port
        return d


TransportContext = StdioTransportContext | HttpTransportContext


@dataclass
class SpanlyPacket:
    timestamp: int
    direction: Literal["from-client", "to-client"]
    context: SpanlyPacketContext
    transport_context: TransportContext
    mcp_packet: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "direction": self.direction,
            "context": self.context.to_dict(),
            "transportContext": self.transport_context.to_dict(),
            "mcpPacket": self.mcp_packet,
        }
