"""Durable-state interface used by the consensus core, plus an in-memory implementation.

Durability contract (enforced by the core's call order):
* save_term_vote() completes before a vote is granted or a candidacy is announced.
* replace_suffix() completes before APPEND_ACK is sent / before the leader counts the entry.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from dchat.protocol.messages import LogEntry


@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)
    commit_index: int = 0


class Storage(Protocol):
    async def load(self) -> PersistentState: ...
    async def save_term_vote(self, term: int, voted_for: str | None) -> None: ...
    async def replace_suffix(self, from_index: int, entries: Sequence[LogEntry]) -> None: ...
    async def save_commit_index(self, commit_index: int) -> None: ...
    async def close(self) -> None: ...


class MemoryStorage:
    """Stands in for the disk in tests. Re-using one instance across a 'restart' keeps state."""

    def __init__(self) -> None:
        self.term = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []
        self.commit_index = 0
        self.fsyncs = 0

    async def load(self) -> PersistentState:
        return PersistentState(self.term, self.voted_for, list(self.log), self.commit_index)

    async def save_term_vote(self, term: int, voted_for: str | None) -> None:
        self.term, self.voted_for = term, voted_for
        self.fsyncs += 1

    async def replace_suffix(self, from_index: int, entries: Sequence[LogEntry]) -> None:
        del self.log[from_index - 1 :]
        self.log.extend(entries)
        self.fsyncs += 1

    async def save_commit_index(self, commit_index: int) -> None:
        self.commit_index = max(self.commit_index, commit_index)

    async def close(self) -> None:
        return None
