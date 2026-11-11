"""In-memory replicated log with dedupe table and config-entry index.

The whole log is also persisted in SQLite; this structure is rebuilt from it on startup,
which is how the msg_id dedupe table survives a crash.
"""

from __future__ import annotations

from collections.abc import Iterable

from dchat.protocol.messages import LogEntry


class RaftLog:
    def __init__(self, entries: Iterable[LogEntry] = ()) -> None:
        self._entries: list[LogEntry] = []
        self._by_msg_id: dict[str, int] = {}
        self._config_indexes: list[int] = []
        for entry in entries:
            self.append(entry)

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    @property
    def last_config_index(self) -> int:
        return self._config_indexes[-1] if self._config_indexes else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if not 1 <= index <= len(self._entries):
            raise IndexError(f"log index {index} out of range 1..{len(self._entries)}")
        return self._entries[index - 1].term

    def get(self, index: int) -> LogEntry:
        if not 1 <= index <= len(self._entries):
            raise IndexError(f"log index {index} out of range 1..{len(self._entries)}")
        return self._entries[index - 1]

    def slice(self, start: int, limit: int) -> list[LogEntry]:
        start = max(1, start)
        return self._entries[start - 1 : start - 1 + limit]

    def append(self, entry: LogEntry) -> None:
        if entry.index != len(self._entries) + 1:
            raise ValueError(
                f"non-contiguous append: got {entry.index}, expected {len(self._entries) + 1}"
            )
        self._entries.append(entry)
        if entry.kind == "chat" and entry.msg_id:
            self._by_msg_id.setdefault(entry.msg_id, entry.index)
        elif entry.kind == "config":
            self._config_indexes.append(entry.index)

    def truncate_from(self, index: int) -> None:
        """Delete entry ``index`` and everything after it."""
        if index < 1:
            raise ValueError("cannot truncate from index < 1")
        for entry in self._entries[index - 1 :]:
            if entry.kind == "chat" and self._by_msg_id.get(entry.msg_id) == entry.index:
                del self._by_msg_id[entry.msg_id]
        del self._entries[index - 1 :]
        while self._config_indexes and self._config_indexes[-1] >= index:
            self._config_indexes.pop()

    def index_of_msg(self, msg_id: str) -> int | None:
        return self._by_msg_id.get(msg_id)

    def latest_config_entry(self) -> LogEntry | None:
        return self.get(self._config_indexes[-1]) if self._config_indexes else None

    def first_index_of_term(self, term: int, upto: int) -> int:
        """First index of the contiguous run of ``term`` that ends at ``upto``."""
        i = upto
        while i > 1 and self.term_at(i - 1) == term:
            i -= 1
        return i
