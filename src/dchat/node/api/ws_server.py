"""FastAPI + uvicorn WebSocket endpoint, embedded in the node's own event loop.

uvicorn is started with ``Server.serve()`` inside the node's TaskGroup, and its signal-handler
installation is disabled so the node keeps one event loop and one shutdown path
(asyncio.run's Ctrl+C handling works the same on Windows, macOS and Linux).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .session import Disconnected, run_client_session

if TYPE_CHECKING:
    from ..runtime import ClusterNode


def create_app(node: ClusterNode) -> FastAPI:
    app = FastAPI(title=f"dchat {node.cfg.node_id}")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return node.status()

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket) -> None:
        await ws.accept()

        async def receive() -> str:
            try:
                return await ws.receive_text()
            except (WebSocketDisconnect, RuntimeError, KeyError) as exc:
                raise Disconnected from exc

        async def send(text: str) -> None:
            try:
                await ws.send_text(text)
            except (WebSocketDisconnect, RuntimeError) as exc:
                raise Disconnected from exc

        await run_client_session(node, receive, send)

    return app


class _QuietServer(uvicorn.Server):
    """uvicorn.Server that never touches process signal handlers (version-proof: we override
    both the old ``install_signal_handlers`` and the newer ``capture_signals`` hook)."""

    def install_signal_handlers(self) -> None:
        return None

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


async def serve_ws(node: ClusterNode) -> None:
    config = uvicorn.Config(
        create_app(node),
        host=node.cfg.host,
        port=node.cfg.ws_port,
        log_level="warning",
        lifespan="off",
    )
    server = _QuietServer(config)
    try:
        await server.serve()
    finally:
        server.should_exit = True
