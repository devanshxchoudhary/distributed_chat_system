"""asyncio TCP transport between server nodes.

One outbound connection per peer (we dial, we send); responses come back on the peer's own
outbound connection to us. Every connection starts with PEER_HELLO carrying the sender's
advertised address, which is how nodes learn each other's addresses without broadcast.

Robustness: bounded per-peer queues (oldest dropped; Raft tolerates loss), reconnect with
exponential backoff + jitter, malformed frames skipped, oversize frames close the link.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from contextlib import suppress

from pydantic import ValidationError

from dchat.protocol.framing import (
    FrameError,
    MalformedFrame,
    OversizeFrame,
    encode_frame,
    read_frame,
)
from dchat.protocol.messages import (
    PeerHello,
    PeerMessageT,
    ProtocolVersionError,
    dump_peer_message,
    parse_peer_message,
)

from ..config import split_addr

OnMessage = Callable[[PeerMessageT], Awaitable[None]]
log = logging.getLogger("dchat.tcp")

SEND_QUEUE_MAX = 256
CONNECT_TIMEOUT = 3.0
BACKOFF_START, BACKOFF_MAX = 0.1, 2.0


class PeerLink:
    """Outbound half-duplex link to one peer, with reconnect/backoff."""

    def __init__(self, peer_id: str, addr: str, hello: PeerHello) -> None:
        self.peer_id = peer_id
        self.addr = addr
        self.hello = hello
        self.queue: asyncio.Queue[PeerMessageT] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        self._reconnect = False

    def set_addr(self, addr: str) -> None:
        if addr != self.addr:
            self.addr = addr
            self._reconnect = True

    def enqueue(self, msg: PeerMessageT) -> None:
        if self.queue.full():
            with suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()  # drop oldest, never block the consensus loop
        self.queue.put_nowait(msg)

    async def run(self) -> None:
        backoff = BACKOFF_START
        while True:
            host, port = split_addr(self.addr)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=CONNECT_TIMEOUT
                )
            except (OSError, TimeoutError):
                await asyncio.sleep(backoff * (0.5 + random.random()))
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            backoff = BACKOFF_START
            self._reconnect = False
            log.debug("connected to %s at %s", self.peer_id, self.addr)
            try:
                writer.write(encode_frame(dump_peer_message(self.hello)))
                await writer.drain()
                while not self._reconnect:
                    try:
                        msg = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                    except TimeoutError:
                        if reader.at_eof():
                            break  # peer closed while idle: reconnect
                        continue
                    writer.write(encode_frame(dump_peer_message(msg)))
                    await writer.drain()
            except (OSError, ConnectionError, FrameError):
                pass
            finally:
                writer.close()
                with suppress(Exception):
                    await writer.wait_closed()
            await asyncio.sleep(BACKOFF_START)


class TcpTransport:
    def __init__(
        self,
        *,
        node_id: str,
        advertise_addr: str,
        listen_host: str,
        listen_port: int,
        on_message: OnMessage,
    ) -> None:
        self.node_id = node_id
        self.advertise_addr = advertise_addr
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._on_message = on_message
        self._links: dict[str, PeerLink] = {}
        self._tg: asyncio.TaskGroup | None = None
        self._inbound: set[asyncio.StreamWriter] = set()
        self.ready = asyncio.Event()
        self._hello = PeerHello(sender=node_id, addr=advertise_addr)

    # -- Transport interface (called from the consensus loop; must not block) --------------
    def set_peer_address(self, peer_id: str, addr: str) -> None:
        if peer_id == self.node_id:
            return
        link = self._links.get(peer_id)
        if link is not None:
            link.set_addr(addr)
            return
        link = PeerLink(peer_id, addr, self._hello)
        self._links[peer_id] = link
        if self._tg is not None:
            self._tg.create_task(link.run(), name=f"link-{peer_id}")

    def send(self, peer_id: str, message: PeerMessageT) -> None:
        link = self._links.get(peer_id)
        if link is None:
            log.debug("no address for peer %s; dropping %s", peer_id, message.type)
            return
        link.enqueue(message)

    # -- lifecycle ---------------------------------------------------------------------------
    async def run(self) -> None:
        server = await asyncio.start_server(
            self._handle_inbound, self.listen_host, self.listen_port
        )
        try:
            async with asyncio.TaskGroup() as tg:
                self._tg = tg
                for link in self._links.values():
                    tg.create_task(link.run(), name=f"link-{link.peer_id}")
                self.ready.set()
                await asyncio.Event().wait()  # run until cancelled
        finally:
            self._tg = None
            server.close()
            for w in list(self._inbound):
                w.close()  # Python 3.12 wait_closed() would otherwise block on live peers
            with suppress(Exception):
                await asyncio.wait_for(server.wait_closed(), timeout=1.0)

    # -- inbound -----------------------------------------------------------------------------
    async def _handle_inbound(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._inbound.add(writer)
        peer_id = "?"
        try:
            try:
                first = parse_peer_message(await read_frame(reader))
            except (MalformedFrame, ValidationError, ProtocolVersionError) as exc:
                log.warning("bad handshake: %s", exc)
                return
            if not isinstance(first, PeerHello):
                log.warning("first frame was %s, expected PEER_HELLO", first.type)
                return
            peer_id = first.sender
            self.set_peer_address(peer_id, first.addr)
            while True:
                try:
                    msg = parse_peer_message(await read_frame(reader))
                except (MalformedFrame, ValidationError, ProtocolVersionError) as exc:
                    log.warning("dropping bad frame from %s: %s", peer_id, exc)
                    continue
                if msg.sender != peer_id:
                    log.warning("sender mismatch %s != %s; dropped", msg.sender, peer_id)
                    continue
                await self._on_message(msg)
        except (asyncio.IncompleteReadError, ConnectionError, OSError, OversizeFrame) as exc:
            log.debug("inbound link from %s closed: %r", peer_id, exc)
        finally:
            self._inbound.discard(writer)
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
