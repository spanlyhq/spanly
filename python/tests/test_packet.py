"""Tests for packet dataclasses and serialization."""

from spanly._packet import (
    HttpTransportContext,
    SpanlyPacket,
    SpanlyPacketContext,
    StdioTransportContext,
)


def test_spanly_packet_to_dict_stdio():
    packet = SpanlyPacket(
        timestamp=1700000000000,
        direction="from-client",
        context=SpanlyPacketContext(
            spanly_client_id="client-123",
            spanly_monitor_id="monitor-456",
        ),
        transport_context=StdioTransportContext(),
        mcp_packet={"jsonrpc": "2.0", "method": "tools/list", "id": 1},
    )

    d = packet.to_dict()

    assert d["timestamp"] == 1700000000000
    assert d["direction"] == "from-client"
    assert d["context"]["spanlyClientId"] == "client-123"
    assert d["context"]["spanlyMonitorId"] == "monitor-456"
    assert d["transportContext"] == {"type": "stdio"}
    assert d["mcpPacket"]["jsonrpc"] == "2.0"
    assert d["mcpPacket"]["method"] == "tools/list"


def test_spanly_packet_to_dict_http():
    packet = SpanlyPacket(
        timestamp=1700000000000,
        direction="to-client",
        context=SpanlyPacketContext(
            spanly_client_id="client-123",
            spanly_monitor_id="monitor-456",
        ),
        transport_context=HttpTransportContext(
            http_method="post",
            path="/mcp",
            headers={"content-type": "application/json"},
        ),
        mcp_packet={"jsonrpc": "2.0", "result": {}, "id": 1},
    )

    d = packet.to_dict()

    assert d["transportContext"]["type"] == "http"
    assert d["transportContext"]["httpMethod"] == "post"
    assert d["transportContext"]["path"] == "/mcp"
    assert d["transportContext"]["headers"] == {"content-type": "application/json"}


def test_context_optional_fields():
    ctx = SpanlyPacketContext(
        spanly_client_id="c",
        spanly_monitor_id="m",
        project_id="proj-1",
    )
    d = ctx.to_dict()
    assert d["projectId"] == "proj-1"
    assert "environmentId" not in d
    assert "organisationId" not in d
