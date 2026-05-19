"""Stream interceptors for capturing MCP JSON-RPC messages."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from spanly._packet import HttpTransportContext, StdioTransportContext, TransportContext

logger = logging.getLogger("spanly")

T = TypeVar("T")


class InterceptedReceiveStream(Generic[T]):
    """Wraps a receive stream to intercept incoming (from-client) messages."""

    def __init__(
        self,
        inner: Any,
        on_message: Callable[[Any], None],
    ) -> None:
        self._inner = inner
        self._on_message = on_message

    async def receive(self) -> T:
        item = await self._inner.receive()
        try:
            self._on_message(item)
        except Exception:
            logger.debug("Error in receive interceptor", exc_info=True)
        return item

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def __aenter__(self) -> InterceptedReceiveStream[T]:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    def __aiter__(self) -> InterceptedReceiveStream[T]:
        return self

    async def __anext__(self) -> T:
        try:
            return await self.receive()
        except Exception:
            raise StopAsyncIteration


class InterceptedSendStream(Generic[T]):
    """Wraps a send stream to intercept outgoing (to-client) messages."""

    def __init__(
        self,
        inner: Any,
        on_message: Callable[[Any], None],
    ) -> None:
        self._inner = inner
        self._on_message = on_message

    async def send(self, item: T) -> None:
        try:
            self._on_message(item)
        except Exception:
            logger.debug("Error in send interceptor", exc_info=True)
        await self._inner.send(item)

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def __aenter__(self) -> InterceptedSendStream[T]:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()


def extract_transport_context(session_message: Any) -> TransportContext:
    """Determine transport context from a SessionMessage's metadata."""
    metadata = getattr(session_message, "metadata", None)
    if metadata is None:
        return StdioTransportContext()

    request = getattr(metadata, "request_context", None)
    if request is None:
        return StdioTransportContext()

    # Check for Starlette Request (has .method and .url attributes)
    method_attr = getattr(request, "method", None)
    url_attr = getattr(request, "url", None)
    if method_attr is not None and url_attr is not None:
        try:
            method = str(method_attr).lower()
            path = str(getattr(url_attr, "path", "/"))
            headers: dict[str, str] = {}
            raw_headers = getattr(request, "headers", None)
            if raw_headers is not None:
                headers = {k: v for k, v in raw_headers.items()}
            return HttpTransportContext(
                http_method=method,
                path=path,
                headers=headers,
            )
        except Exception:
            logger.debug("Failed to extract HTTP transport context", exc_info=True)

    return StdioTransportContext()


def session_message_to_dict(session_message: Any) -> dict[str, Any] | None:
    """Convert a SessionMessage's JSONRPCMessage to a plain dict."""
    try:
        message = session_message.message
        return message.model_dump(by_alias=True, exclude_unset=True)
    except Exception:
        logger.debug("Failed to convert SessionMessage to dict", exc_info=True)
        return None
