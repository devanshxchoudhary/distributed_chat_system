"""Transport-agnostic client session: the WebSocket protocol without any web framework.

The FastAPI endpoint only adapts a WebSocket to the two callables below, which keeps this
logic fully testable. The client never takes part in election or ordering; it is only told
the leader and term the backend reports (WELCOME / NODE_STATUS).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dchat.protocol.client_messages import (
    ClientAck,
    ClientHello,
    ClientMessage,
    ErrorMessage,
    MessageDeliver,
    NodeStatus,
    Welcome,
    parse_client_message,
)

from ..raft.core import ChatRecord

if TYPE_CHECKING:
    from ..runtime import ClusterNode

STATUS_INTERVAL = 1.0


class Disconnected(Exception):
    """Raised by the receive/send callables when the client went away."""


async def run_client_session(
    node: ClusterNode,
    receive: Callable[[], Awaitable[str]],
    send: Callable[[str], Awaitable[None]],
    status_interval: float = STATUS_INTERVAL,
) -> None:
    send_lock = asyncio.Lock()  # one writer at a time on the socket

    async def emit(model: object) -> None:
        text = json.dumps(model.model_dump())  # type: ignore[attr-defined]
        async with send_lock:
            await send(text)

    try:
        try:
            first = parse_client_message(json.loads(await receive()))
        except (json.JSONDecodeError, ValidationError) as exc:
            await emit(ErrorMessage(code="bad_hello", detail=str(exc)[:200]))
            return
        if not isinstance(first, ClientHello):
            await emit(ErrorMessage(code="bad_hello", detail="first message must be CLIENT_HELLO"))
            return

        session = node.hub.register(first.user, first.last_seen_seq)
        st = node.status()
        await emit(
            Welcome(
                node_id=st["node_id"], role=st["role"], term=st["term"], leader_id=st["leader_id"]
            )
        )

        async def deliver(rec: ChatRecord) -> None:
            await emit(
                MessageDeliver(
                    seq=rec.seq,
                    index=rec.index,
                    msg_id=rec.msg_id,
                    user=rec.user,
                    text=rec.text,
                    ts=rec.ts,
                )
            )

        async def status_loop() -> None:
            while True:
                s = node.status()
                await emit(NodeStatus(**s))
                await asyncio.sleep(status_interval)

        async def receive_loop() -> None:
            while True:
                raw = await receive()
                try:
                    msg = parse_client_message(json.loads(raw))
                except (json.JSONDecodeError, ValidationError) as exc:
                    await emit(ErrorMessage(code="invalid_message", detail=str(exc)[:200]))
                    continue
                if isinstance(msg, ClientMessage):
                    status = await node.submit(msg.msg_id, first.user, msg.text)
                    await emit(ClientAck(msg_id=msg.msg_id, status=status))
                else:
                    await emit(
                        ErrorMessage(code="unexpected", detail="CLIENT_HELLO already received")
                    )

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(node.hub.pump(session, node.raft.messages_after, deliver))
                tg.create_task(status_loop())
                tg.create_task(receive_loop())
        finally:
            node.hub.unregister(session)
    except* Disconnected:
        pass
    except* ConnectionError:
        pass
