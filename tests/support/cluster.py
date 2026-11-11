"""Run real ClusterNode instances (real TCP + SQLite) in one process for integration tests."""

from __future__ import annotations

import asyncio
import contextlib
import random
import socket
import tempfile
from collections.abc import Callable
from pathlib import Path

from dchat.node.config import NodeConfig
from dchat.node.raft.core import Role
from dchat.node.runtime import ClusterNode


def free_ports(n: int) -> list[int]:
    socks = [socket.socket() for _ in range(n)]
    try:
        for s in socks:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


async def wait_until(
    pred: Callable[[], bool], timeout: float = 20.0, what: str = "condition"
) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if pred():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


class RealCluster:
    def __init__(self, ids: list[str], bootstrap: list[str] | None = None) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.ids = ids
        self.bootstrap = ids if bootstrap is None else bootstrap
        self.ports = dict(zip(ids, free_ports(len(ids)), strict=True))
        self.nodes: dict[str, ClusterNode] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}

    def cfg(self, nid: str, seeds: tuple[str, ...] = ()) -> NodeConfig:
        return NodeConfig(
            node_id=nid,
            host="127.0.0.1",
            tcp_port=self.ports[nid],
            ws_port=0,
            advertise_addr=f"127.0.0.1:{self.ports[nid]}",
            db_path=str(Path(self.tmp.name) / f"{nid}.db"),
            heartbeat_ms=100,
            election_timeout_ms=(500, 1000),
            seeds=seeds,
            bootstrap_voters={i: f"127.0.0.1:{self.ports[i]}" for i in self.bootstrap},
        )

    async def start(self, nid: str, seeds: tuple[str, ...] = ()) -> ClusterNode:
        node = ClusterNode(
            self.cfg(nid, seeds), rng=random.Random(hash(nid) & 0xFFFF), enable_ws=False
        )
        self.nodes[nid] = node
        self.tasks[nid] = asyncio.create_task(node.run(), name=f"node-{nid}")
        await asyncio.wait_for(node.started.wait(), 10)
        return node

    async def start_all(self) -> None:
        for nid in self.bootstrap:
            await self.start(nid)

    async def stop(self, nid: str) -> None:
        task = self.tasks.pop(nid)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def close(self) -> None:
        for nid in list(self.tasks):
            await self.stop(nid)
        self.tmp.cleanup()

    def live(self) -> list[ClusterNode]:
        return [self.nodes[i] for i in self.tasks]

    def leaders(self) -> list[ClusterNode]:
        return [n for n in self.live() if n.raft.role is Role.LEADER]

    async def wait_leader(self, timeout: float = 20.0) -> ClusterNode:
        await wait_until(
            lambda: (
                len(self.leaders()) == 1
                and all(n.raft.leader_id == self.leaders()[0].raft.node_id for n in self.live())
            ),
            timeout,
            "stable leader",
        )
        return self.leaders()[0]

    async def send_until_committed(
        self, via: ClusterNode, msg_id: str, text: str, timeout: float = 15.0
    ) -> None:
        """Client behaviour: resend the same msg_id until everybody live has committed it."""

        async def committed() -> bool:
            return all(any(r.msg_id == msg_id for r in n.raft.chat) for n in self.live())

        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            with contextlib.suppress(Exception):
                await via.submit(msg_id, "tester", text)
            for _ in range(10):
                if await committed():
                    return
                await asyncio.sleep(0.05)
        raise AssertionError(f"{msg_id} never committed everywhere")
