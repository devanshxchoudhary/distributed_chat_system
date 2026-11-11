"""Client connection manager (no UI): reconnect, server rotation, resume and retry.

* On every (re)connect it sends CLIENT_HELLO with ``last_seen_seq`` so the server replays
  exactly the committed messages we missed.
* Unconfirmed messages stay in ``pending`` and are re-sent with the SAME msg_id until a
  MESSAGE_DELIVER with that id arrives; the cluster deduplicates by msg_id.
* The client never elects or orders anything; it displays what the backend reports.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError

from dchat.protocol.client_messages import (
    ClientHello,
    ClientMessage,
    MessageDeliver,
    ServerToClientT,
    parse_server_message,
)


class Socket(Protocol):
    async def send(self, text: str) -> None: ...
    async def recv(self) -> str: ...


Connector = Callable[[str], AbstractAsyncContextManager[Socket]]


@dataclass(frozen=True)
class Connected:
    url: str


@dataclass(frozen=True)
class Disconnected:
    reason: str


Event = ServerToClientT | Connected | Disconnected


def default_connector(url: str) -> AbstractAsyncContextManager[Socket]:
    import websockets  # imported lazily so tests need no network library

    return websockets.connect(url)  # type: ignore[return-value]


class ChatConnection:
    def __init__(
        self,
        urls: list[str],
        user: str,
        on_event: Callable[[Event], None],
        *,
        connector: Connector = default_connector,
        resend_interval: float = 3.0,
        backoff_max: float = 5.0,
    ) -> None:
        if not urls:
            raise ValueError("at least one server url is required")
        self.urls, self.user, self.on_event = urls, user, on_event
        self._connector = connector
        self.resend_interval, self.backoff_max = resend_interval, backoff_max
        self.last_seen_seq = 0
        self.pending: dict[str, str] = {}
        self._ws: Socket | None = None
        self._next_url = 0

    async def send_message(self, text: str) -> str:
        msg_id = uuid.uuid4().hex
        self.pending[msg_id] = text
        await self._try_send(msg_id)
        return msg_id

    async def _try_send(self, msg_id: str) -> None:
        ws, text = self._ws, self.pending.get(msg_id)
        if ws is None or text is None:
            return
        try:
            await ws.send(json.dumps(ClientMessage(msg_id=msg_id, text=text).model_dump()))
        except Exception:  # connection died; run() will reconnect and resend
            pass

    async def _resend_loop(self) -> None:
        while True:
            await asyncio.sleep(self.resend_interval)
            for msg_id in list(self.pending):
                await self._try_send(msg_id)

    async def _recv_loop(self, ws: Socket) -> None:
        while True:
            raw = await ws.recv()
            try:
                msg = parse_server_message(json.loads(raw))
            except (json.JSONDecodeError, ValidationError):
                continue
            if isinstance(msg, MessageDeliver):
                self.pending.pop(msg.msg_id, None)
                if msg.seq <= self.last_seen_seq:
                    continue  # replay overlap: never show a message twice
                self.last_seen_seq = msg.seq
            self.on_event(msg)

    async def run(self) -> None:
        backoff = 0.3
        while True:
            url = self.urls[self._next_url % len(self.urls)]
            self._next_url += 1  # rotate: if this node is down, the next attempt tries another
            try:
                async with self._connector(url) as ws:
                    self._ws = ws
                    await ws.send(
                        json.dumps(
                            ClientHello(
                                user=self.user, last_seen_seq=self.last_seen_seq
                            ).model_dump()
                        )
                    )
                    self.on_event(Connected(url))
                    backoff = 0.3
                    for msg_id in list(self.pending):
                        await self._try_send(msg_id)
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._recv_loop(ws))
                        tg.create_task(self._resend_loop())
            except Exception as exc:
                self._ws = None
                reason = exc.exceptions[0] if isinstance(exc, ExceptionGroup) else exc
                self.on_event(Disconnected(f"{type(reason).__name__}: {reason}"[:120]))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.backoff_max)
            finally:
                self._ws = None
