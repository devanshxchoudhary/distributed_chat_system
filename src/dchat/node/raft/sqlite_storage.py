"""SQLite persistence via aiosqlite: WAL journal, synchronous=FULL, one commit per call."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import aiosqlite

from dchat.protocol.messages import LogEntry

from .storage import PersistentState


class StorageCorrupt(RuntimeError):
    pass


class SqliteStorage:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    @classmethod
    async def open(cls, path: str) -> SqliteStorage:
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=FULL")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS log (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, data TEXT NOT NULL)"
        )
        await conn.commit()
        return cls(conn)

    async def _meta(self, key: str) -> str | None:
        cur = await self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = await cur.fetchone()
        await cur.close()
        return None if row is None else str(row[0])

    async def load(self) -> PersistentState:
        term = int(await self._meta("current_term") or 0)
        voted = await self._meta("voted_for") or None
        commit = int(await self._meta("commit_index") or 0)
        cur = await self._conn.execute("SELECT idx, data FROM log ORDER BY idx")
        rows = await cur.fetchall()
        await cur.close()
        entries: list[LogEntry] = []
        for pos, (idx, data) in enumerate(rows, start=1):
            if idx != pos:
                raise StorageCorrupt(f"log gap: expected index {pos}, found {idx}")
            entries.append(LogEntry.model_validate_json(data))
        return PersistentState(term, voted, entries, commit)

    async def save_term_vote(self, term: int, voted_for: str | None) -> None:
        try:
            await self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('current_term', ?)", (str(term),)
            )
            await self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('voted_for', ?)", (voted_for or "",)
            )
            await self._conn.commit()
        except BaseException:
            await self._conn.rollback()
            raise

    async def replace_suffix(self, from_index: int, entries: Sequence[LogEntry]) -> None:
        try:
            await self._conn.execute("DELETE FROM log WHERE idx >= ?", (from_index,))
            await self._conn.executemany(
                "INSERT INTO log(idx, term, data) VALUES (?, ?, ?)",
                [(e.index, e.term, e.model_dump_json()) for e in entries],
            )
            await self._conn.commit()
        except BaseException:
            await self._conn.rollback()
            raise

    async def save_commit_index(self, commit_index: int) -> None:
        await self._conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('commit_index', ?)",
            (str(commit_index),),
        )
        await self._conn.commit()

    async def close(self) -> None:
        await self._conn.close()
