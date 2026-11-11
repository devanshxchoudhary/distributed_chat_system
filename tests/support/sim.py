"""Deterministic cluster simulator: ManualClock + in-memory network, no sockets, no sleeps."""

from __future__ import annotations

import random
from collections import deque

from dchat.node.clock import ManualClock
from dchat.node.raft.core import RaftNode, Role
from dchat.node.raft.storage import MemoryStorage
from dchat.protocol.messages import PeerMessageT, dump_peer_message, parse_peer_message


class SimTransport:
    def __init__(self, net: SimNet, owner: str) -> None:
        self.net, self.owner = net, owner
        self.addresses: dict[str, str] = {}

    def send(self, peer_id: str, message: PeerMessageT) -> None:
        self.net.enqueue(self.owner, peer_id, message)

    def set_peer_address(self, peer_id: str, addr: str) -> None:
        self.addresses[peer_id] = addr


class SimNet:
    def __init__(
        self,
        ids: list[str],
        seed: int = 1,
        drop_rate: float = 0.0,
        bootstrap: list[str] | None = None,
    ) -> None:
        self.clock = ManualClock()
        self.rng = random.Random(seed)
        self.drop_rate = drop_rate
        self.addr = {i: f"10.0.0.{n}:9000" for n, i in enumerate(ids, start=1)}
        self.bootstrap = {i: self.addr[i] for i in (bootstrap if bootstrap is not None else ids)}
        self.storages: dict[str, MemoryStorage] = {i: MemoryStorage() for i in ids}
        self.nodes: dict[str, RaftNode] = {}
        self.up: set[str] = set()
        self.blocked: set[tuple[str, str]] = set()
        self.queue: deque[tuple[str, str, dict]] = deque()  # type: ignore[type-arg]
        self.delivered_commits: dict[str, list[tuple[int, str]]] = {i: [] for i in ids}
        for i in ids:
            self._build(i)

    def _build(self, node_id: str) -> RaftNode:
        n = RaftNode(
            node_id=node_id,
            advertise_addr=self.addr[node_id],
            bootstrap_voters=self.bootstrap if node_id in self.bootstrap else {},
            storage=self.storages[node_id],
            transport=SimTransport(self, node_id),
            clock=self.clock,
            rng=random.Random(self.rng.random()),
            heartbeat_interval=0.5,
            election_timeout=(1.5, 3.0),
            on_commit=lambda rec, nid=node_id: self.delivered_commits[nid].append(
                (rec.seq, rec.msg_id)
            ),
        )
        self.nodes[node_id] = n
        return n

    async def start_all(self) -> None:
        for i in list(self.nodes):
            await self.start(i)

    async def start(self, node_id: str) -> None:
        self.delivered_commits[node_id] = []
        await self.nodes[node_id].start()
        self.up.add(node_id)

    def enqueue(self, src: str, dst: str, msg: PeerMessageT) -> None:
        if src not in self.up:
            return
        if self.drop_rate and self.rng.random() < self.drop_rate:
            return
        self.queue.append((src, dst, dump_peer_message(msg)))  # serialise: catches wire bugs

    def crash(self, node_id: str) -> None:
        """Simulate kill -9: lose all volatile state, keep only what hit 'disk'."""
        self.up.discard(node_id)
        self.queue = deque(m for m in self.queue if m[0] != node_id and m[1] != node_id)

    async def restart(self, node_id: str) -> RaftNode:
        self.crash(node_id)
        self._build(node_id)
        await self.start(node_id)
        return self.nodes[node_id]

    def partition(self, group_a: set[str], group_b: set[str]) -> None:
        for a in group_a:
            for b in group_b:
                self.blocked.update({(a, b), (b, a)})

    def heal(self) -> None:
        self.blocked.clear()

    async def _drain(self) -> None:
        for _ in range(20000):
            if not self.queue:
                return
            src, dst, raw = self.queue.popleft()
            if dst not in self.up or (src, dst) in self.blocked:
                continue
            await self.nodes[dst].on_message(parse_peer_message(raw))
        raise AssertionError("message storm: network never quiesced")

    async def run(self, seconds: float, step: float = 0.05) -> None:
        for _ in range(round(seconds / step)):
            self.clock.advance(step)
            for nid in sorted(self.up):
                await self.nodes[nid].tick()
            await self._drain()

    def leaders(self, among: set[str] | None = None) -> list[str]:
        ids = self.up if among is None else (self.up & among)
        return sorted(i for i in ids if self.nodes[i].role is Role.LEADER)

    def leader(self) -> RaftNode:
        ls = self.leaders()
        assert len(ls) == 1, f"expected exactly one leader, got {ls}"
        return self.nodes[ls[0]]

    async def wait_for_leader(self, limit: float = 30.0) -> RaftNode:
        waited = 0.0
        while waited < limit:
            await self.run(0.5)
            waited += 0.5
            ls = self.leaders()
            if len(ls) == 1:
                return self.nodes[ls[0]]
        raise AssertionError("no leader elected")

    def assert_logs_consistent(self) -> None:
        """Log matching + state machine safety across every node (up or crashed)."""
        logs = {i: self.storages[i].log for i in self.storages}
        for a in logs:
            for b in logs:
                for ea, eb in zip(logs[a], logs[b], strict=False):
                    if ea.term == eb.term and ea.index == eb.index:
                        assert ea == eb, f"log matching violated at {ea.index} between {a} and {b}"
        chats = {i: [(r.seq, r.msg_id) for r in self.nodes[i].chat] for i in self.up}
        for a in chats:
            for b in chats:
                n = min(len(chats[a]), len(chats[b]))
                assert chats[a][:n] == chats[b][:n], f"committed order diverged between {a} and {b}"
