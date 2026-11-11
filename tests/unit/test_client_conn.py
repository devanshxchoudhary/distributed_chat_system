import asyncio
import contextlib
import json

from dchat.client.ws_client import ChatConnection, Connected, Disconnected
from dchat.protocol.client_messages import MessageDeliver


class FakeWS:
    def __init__(self, script: list[object]) -> None:
        self.sent: list[dict] = []  # type: ignore[type-arg]
        self.incoming: asyncio.Queue[object] = asyncio.Queue()
        for item in script:
            self.incoming.put_nowait(item)

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))

    async def recv(self) -> str:
        item = await self.incoming.get()
        if isinstance(item, Exception):
            raise item
        return json.dumps(item)


def deliver(seq: int, mid: str) -> dict:  # type: ignore[type-arg]
    return {
        "type": "MESSAGE_DELIVER",
        "seq": seq,
        "index": seq,
        "msg_id": mid,
        "user": "u",
        "text": "t",
        "ts": 1.0,
    }


async def test_reconnect_rotates_servers_resumes_from_last_seq_and_retries_pending() -> None:
    sockets = [
        None,  # attempt 1: server down
        FakeWS([deliver(1, "a"), deliver(2, "b"), ConnectionError("link dropped")]),
        FakeWS([deliver(2, "b"), deliver(3, "c")]),  # replay overlap with seq 2 must be ignored
    ]
    attempts: list[str] = []

    @contextlib.asynccontextmanager
    async def connector(url: str):  # type: ignore[no-untyped-def]
        attempts.append(url)
        ws = sockets[len(attempts) - 1]
        if ws is None:
            raise ConnectionRefusedError("down")
        yield ws
        # keep the "connection" open until the test cancels it
        await asyncio.sleep(3600)

    events: list[object] = []
    conn = ChatConnection(
        ["ws://n1/ws", "ws://n2/ws", "ws://n3/ws"],
        "alice",
        events.append,
        connector=connector,
        resend_interval=0.05,
        backoff_max=0.1,
    )
    task = asyncio.create_task(conn.run())
    for _ in range(200):
        if conn.last_seen_seq == 3:
            break
        await asyncio.sleep(0.02)
    pending_id = await conn.send_message("hello while connected")
    await asyncio.sleep(0.3)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert attempts[:3] == ["ws://n1/ws", "ws://n2/ws", "ws://n3/ws"]  # failover rotation
    assert conn.last_seen_seq == 3
    seqs = [e.seq for e in events if isinstance(e, MessageDeliver)]
    assert seqs == [1, 2, 3]  # no duplicates despite replay overlap
    assert sum(isinstance(e, Disconnected) for e in events) >= 2 and any(
        isinstance(e, Connected) for e in events
    )
    hello2 = sockets[2].sent[0]  # type: ignore[union-attr]
    assert hello2["type"] == "CLIENT_HELLO" and hello2["last_seen_seq"] == 2  # resumed after seq 2
    resent = [m for m in sockets[2].sent if m["type"] == "CLIENT_MESSAGE"]  # type: ignore[union-attr]
    assert resent and all(
        m["msg_id"] == pending_id for m in resent
    )  # same id re-sent (dedupe-safe)


async def test_pending_cleared_on_delivery_and_garbage_ignored() -> None:
    ws = FakeWS([])

    @contextlib.asynccontextmanager
    async def connector(url: str):  # type: ignore[no-untyped-def]
        yield ws
        await asyncio.sleep(3600)

    conn = ChatConnection(
        ["ws://n1/ws"], "bob", lambda e: None, connector=connector, resend_interval=10
    )
    task = asyncio.create_task(conn.run())
    await asyncio.sleep(0.1)
    mid = await conn.send_message("hi")
    assert mid in conn.pending
    ws.incoming.put_nowait(
        "not-json-object"
    )  # recv returns json.dumps(str) -> parsed as str -> ignored
    ws.incoming.put_nowait(deliver(1, mid))
    await asyncio.sleep(0.1)
    assert mid not in conn.pending and conn.last_seen_seq == 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
