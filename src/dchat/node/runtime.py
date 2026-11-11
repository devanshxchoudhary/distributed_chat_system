"""ClusterNode: wires storage, TCP transport, consensus core, discovery and the client API.

Every event that touches consensus state (peer messages, ticks, client submits) goes through
one queue and is handled strictly one at a time by ``_event_loop``. A handler exception is a
bug or a storage failure, so the node fails fast (crash-recovery is the designed answer).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from dchat.protocol.messages import ForwardRequest, ForwardResponse, JoinResponse, PeerMessageT

from .api.hub import ChatHub
from .clock import Clock, MonotonicClock
from .cluster.discovery import JoinDriver
from .config import NodeConfig
from .network.tcp import TcpTransport
from .raft.core import RaftNode, Role
from .raft.sqlite_storage import SqliteStorage
from .raft.storage import Storage

TICK_SECONDS = 0.05
log = logging.getLogger("dchat.node")


@dataclass
class _Job:
    fn: Callable[[], Awaitable[Any]]
    fut: asyncio.Future[Any] | None = None


class ClusterNode:
    def __init__(
        self,
        cfg: NodeConfig,
        *,
        storage: Storage | None = None,
        clock: Clock | None = None,
        rng: random.Random | None = None,
        enable_ws: bool = True,
    ) -> None:
        self.cfg = cfg
        self._storage = storage
        self.clock = clock or MonotonicClock()
        self.rng = rng or random.Random()
        self.enable_ws = enable_ws
        self.hub = ChatHub()
        self._inbox: asyncio.Queue[_Job] = asyncio.Queue(maxsize=10_000)
        self._stop = asyncio.Event()
        self._tick_pending = False
        self.started = asyncio.Event()
        self.raft: RaftNode
        self.transport: TcpTransport
        self._join: JoinDriver

    # ------------------------------------------------------------------ public API
    def request_stop(self) -> None:
        self._stop.set()

    async def submit(self, msg_id: str, user: str, text: str) -> str:
        """Accept a client message. Returns accepted | duplicate | forwarded | no_leader."""
        return cast(str, await self._call(lambda: self._submit(msg_id, user, text)))

    def status(self) -> dict[str, Any]:
        return self.raft.status()

    # ------------------------------------------------------------------ internals
    async def _call(self, fn: Callable[[], Awaitable[Any]]) -> Any:
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        await self._inbox.put(_Job(fn, fut))
        return await fut

    async def _submit(self, msg_id: str, user: str, text: str) -> str:
        raft = self.raft
        if raft.role is Role.LEADER:
            return (await raft.handle_submit(msg_id, user, text, time.time())).status
        if raft.leader_id is None:
            return "no_leader"
        self.transport.send(
            raft.leader_id,
            ForwardRequest(
                sender=raft.node_id,
                term=raft.term,
                msg_id=msg_id,
                user=user,
                text=text,
                ts=time.time(),
            ),
        )
        return "forwarded"

    async def _on_peer_message(self, msg: PeerMessageT) -> None:
        async def handle() -> None:
            if isinstance(msg, JoinResponse):
                self._join.on_join_response(msg)
            elif isinstance(msg, ForwardResponse):
                self.raft.logger.debug("forward result %s: %s", msg.msg_id, msg.status)
            else:
                await self.raft.on_message(msg)

        await self._inbox.put(_Job(handle))

    async def _event_loop(self) -> None:
        while True:
            job = await self._inbox.get()
            try:
                result = await job.fn()
            except asyncio.CancelledError:
                if job.fut is not None and not job.fut.done():
                    job.fut.cancel()
                raise
            except BaseException as exc:
                if job.fut is not None and not job.fut.done():
                    job.fut.set_exception(exc)
                log.exception(
                    "fatal error in event handler; node stops (restart recovers from disk)"
                )
                raise
            else:
                if job.fut is not None and not job.fut.done():
                    job.fut.set_result(result)

    async def _ticker(self) -> None:
        async def tick() -> None:
            try:
                await self.raft.tick()
            finally:
                self._tick_pending = False

        while True:
            await asyncio.sleep(TICK_SECONDS)
            if not self._tick_pending:
                self._tick_pending = True
                await self._inbox.put(_Job(tick))

    async def _run_ws(self) -> None:
        from .api.ws_server import serve_ws  # lazy: tests and --no-ws need no FastAPI

        await serve_ws(self)

    async def run(self) -> None:
        storage = self._storage or await SqliteStorage.open(self.cfg.db_path)
        lo, hi = self.cfg.election_timeout_ms
        self.transport = TcpTransport(
            node_id=self.cfg.node_id,
            advertise_addr=self.cfg.advertise_addr,
            listen_host=self.cfg.host,
            listen_port=self.cfg.tcp_port,
            on_message=self._on_peer_message,
        )
        self.raft = RaftNode(
            node_id=self.cfg.node_id,
            advertise_addr=self.cfg.advertise_addr,
            bootstrap_voters=self.cfg.bootstrap_voters,
            storage=storage,
            transport=self.transport,
            clock=self.clock,
            rng=self.rng,
            heartbeat_interval=self.cfg.heartbeat_ms / 1000,
            election_timeout=(lo / 1000, hi / 1000),
            on_commit=self.hub.notify,
        )
        self._join = JoinDriver(self.raft, self.transport, self.cfg.seeds)
        try:
            await self.raft.start()
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.transport.run(), name="tcp"),
                    tg.create_task(self._event_loop(), name="events"),
                    tg.create_task(self._ticker(), name="ticker"),
                    tg.create_task(self._join.run(), name="discovery"),
                ]
                if self.enable_ws:
                    tasks.append(tg.create_task(self._run_ws(), name="ws"))
                self.started.set()
                await self._stop.wait()
                for t in tasks:
                    t.cancel()
        finally:
            await storage.close()
            self.raft.logger.info("stopped")
