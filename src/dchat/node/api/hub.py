"""Per-client delivery cursors over the committed chat list.

Ordered delivery by construction: every session reads ``chat[cursor:]`` of the node's
append-only committed list, so entry 105 can never overtake 104 and a reconnecting client
simply starts with ``cursor = last_seen_seq``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ..raft.core import ChatRecord


class Session:
    def __init__(self, user: str, cursor: int) -> None:
        self.user = user
        self.cursor = cursor
        self.event = asyncio.Event()
        self.event.set()  # triggers the initial catch-up pass


class ChatHub:
    def __init__(self) -> None:
        self._sessions: set[Session] = set()

    @property
    def session_count(self) -> int:
        return len(self._sessions)

    def register(self, user: str, last_seen_seq: int) -> Session:
        session = Session(user, max(0, last_seen_seq))
        self._sessions.add(session)
        return session

    def unregister(self, session: Session) -> None:
        self._sessions.discard(session)

    def notify(self, _record: ChatRecord | None = None) -> None:
        """Wake every session; called by the consensus core when a message commits."""
        for session in self._sessions:
            session.event.set()

    @staticmethod
    async def pump(
        session: Session,
        records_after: Callable[[int], list[ChatRecord]],
        send: Callable[[ChatRecord], Awaitable[None]],
    ) -> None:
        while True:
            await session.event.wait()
            session.event.clear()
            for record in records_after(session.cursor):
                await send(record)
                session.cursor = record.seq
