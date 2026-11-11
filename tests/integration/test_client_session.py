"""Client protocol against a REAL 3-node cluster, using an in-memory socket instead of FastAPI."""

import asyncio
import json
import uuid

from dchat.node.api.session import Disconnected, run_client_session
from tests.support.cluster import RealCluster, wait_until


class FakeSocket:
    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.sent: list[dict] = []  # type: ignore[type-arg]

    async def receive(self) -> str:
        item = await self.inbox.get()
        if item is None:
            raise Disconnected
        return item

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))

    def push(self, obj: dict) -> None:  # type: ignore[type-arg]
        self.inbox.put_nowait(json.dumps(obj))

    def delivered(self) -> list[dict]:  # type: ignore[type-arg]
        return [m for m in self.sent if m["type"] == "MESSAGE_DELIVER"]


async def connect(node, user: str, last_seen: int = 0):  # type: ignore[no-untyped-def]
    sock = FakeSocket()
    task = asyncio.create_task(
        run_client_session(node, sock.receive, sock.send, status_interval=0.2)
    )
    sock.push({"type": "CLIENT_HELLO", "user": user, "last_seen_seq": last_seen})
    return sock, task


async def test_clients_on_different_nodes_see_same_order_and_reconnect_catches_up() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        leader = await c.wait_leader()
        a, b = [n for n in c.live()][:2]
        sa, ta = await connect(a, "alice")
        sb, tb = await connect(b, "bob")
        await wait_until(
            lambda: any(m["type"] == "WELCOME" for m in sa.sent + sb.sent), 5, "welcome"
        )
        ids = [str(uuid.uuid4()) for _ in range(6)]
        for i, mid in enumerate(ids):
            (sa if i % 2 == 0 else sb).push(
                {"type": "CLIENT_MESSAGE", "msg_id": mid, "text": f"m{i}"}
            )
            await asyncio.sleep(0.05)
        await wait_until(
            lambda: len(sa.delivered()) == 6 and len(sb.delivered()) == 6,
            15,
            "delivery to both clients",
        )
        order_a = [(m["seq"], m["msg_id"]) for m in sa.delivered()]
        order_b = [(m["seq"], m["msg_id"]) for m in sb.delivered()]
        assert order_a == order_b and [s for s, _ in order_a] == [1, 2, 3, 4, 5, 6]
        assert any(m["type"] == "CLIENT_ACK" for m in sa.sent)
        assert any(
            m["type"] == "NODE_STATUS" and m["leader_id"] == leader.raft.node_id for m in sa.sent
        )

        # alice drops, more messages arrive, she reconnects (to a different node) with last_seen_seq
        sa.inbox.put_nowait(None)
        await asyncio.gather(ta, return_exceptions=True)
        sb.push({"type": "CLIENT_MESSAGE", "msg_id": "while-away-1", "text": "x"})
        sb.push({"type": "CLIENT_MESSAGE", "msg_id": "while-away-2", "text": "y"})
        await wait_until(lambda: len(sb.delivered()) == 8, 10, "bob sees new messages")
        other = [n for n in c.live() if n is not a][-1]
        sa2, ta2 = await connect(other, "alice", last_seen=6)
        await wait_until(lambda: len(sa2.delivered()) == 2, 10, "alice catch-up")
        assert [m["msg_id"] for m in sa2.delivered()] == ["while-away-1", "while-away-2"]
        assert [m["seq"] for m in sa2.delivered()] == [7, 8]  # no duplicates, no gaps
        for s, t in ((sa2, ta2), (sb, tb)):
            s.inbox.put_nowait(None)
            await asyncio.gather(t, return_exceptions=True)
    finally:
        await c.close()


async def test_duplicate_client_retry_delivers_once_and_invalid_input_is_rejected() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        await c.wait_leader()
        node = c.live()[0]
        s, t = await connect(node, "carol")
        await wait_until(lambda: any(m["type"] == "WELCOME" for m in s.sent), 5, "welcome")
        for _ in range(3):  # client retry with the same msg_id
            s.push({"type": "CLIENT_MESSAGE", "msg_id": "same-id", "text": "hello"})
            await asyncio.sleep(0.1)
        s.push({"type": "CLIENT_MESSAGE", "msg_id": "x", "text": ""})  # empty text
        s.inbox.put_nowait("this is not json")
        s.push({"type": "NOPE"})
        await wait_until(
            lambda: sum(m["type"] == "ERROR" for m in s.sent) == 3, 5, "three validation errors"
        )
        await asyncio.sleep(1.0)
        assert [m["msg_id"] for m in s.delivered()] == ["same-id"]
        s.inbox.put_nowait(None)
        await asyncio.gather(t, return_exceptions=True)
    finally:
        await c.close()


async def test_bad_first_message_is_rejected_and_session_closes() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        await c.wait_leader()
        s = FakeSocket()
        task = asyncio.create_task(run_client_session(c.live()[0], s.receive, s.send))
        s.push({"type": "CLIENT_MESSAGE", "msg_id": "m", "text": "no hello first"})
        await asyncio.wait_for(task, 5)
        assert s.sent[0]["type"] == "ERROR" and s.sent[0]["code"] == "bad_hello"
        assert c.live()[0].hub.session_count == 0
    finally:
        await c.close()
