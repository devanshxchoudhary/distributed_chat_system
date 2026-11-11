"""Raft-style consensus core (hand written; no library).

Concurrency contract: every public coroutine must be awaited one at a time. ClusterNode
funnels all events (messages, ticks, client submits) through a single queue, and tests call
the methods sequentially. That makes locks unnecessary. The only awaits inside a handler are
the durable writes that must precede a reply.

Safety rules implemented here (see docs/protocol.md):
* term and vote are persisted before a vote is granted / a candidacy is announced
* log entries are persisted before APPEND_ACK (and before the leader counts itself)
* a leader commits only entries of its own term by counting replicas
* votes are granted only to candidates whose log is at least as up to date
* any message with a higher term demotes the receiver to follower
* membership changes are single-server, learner first, one at a time
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from dchat.protocol.messages import (
    AppendAck,
    AppendEntries,
    ForwardRequest,
    ForwardResponse,
    JoinRequest,
    JoinResponse,
    LogEntry,
    PeerMessageT,
    VoteRequest,
    VoteResponse,
)

from ..clock import Clock
from ..network.transport import Transport
from ..observability.logger import StateLogger
from .log import RaftLog
from .storage import Storage


class Role(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class ClusterConfig:
    voters: dict[str, str]
    learners: dict[str, str]

    def is_voter(self, node_id: str) -> bool:
        return node_id in self.voters

    def is_member(self, node_id: str) -> bool:
        return node_id in self.voters or node_id in self.learners

    def members(self) -> dict[str, str]:
        return {**self.voters, **self.learners}

    @property
    def quorum(self) -> int:
        return len(self.voters) // 2 + 1


@dataclass(frozen=True)
class ChatRecord:
    """A committed chat message. ``seq`` is gap-free and identical on every node."""

    seq: int
    index: int
    msg_id: str
    user: str
    text: str
    ts: float


@dataclass(frozen=True)
class SubmitResult:
    status: str  # accepted | duplicate | not_leader
    index: int = 0
    leader_id: str | None = None


class RaftNode:
    def __init__(
        self,
        *,
        node_id: str,
        advertise_addr: str,
        bootstrap_voters: dict[str, str],
        storage: Storage,
        transport: Transport,
        clock: Clock,
        rng: random.Random,
        heartbeat_interval: float = 0.5,
        election_timeout: tuple[float, float] = (1.5, 3.0),
        max_batch: int = 64,
        on_commit: Callable[[ChatRecord], None] | None = None,
        logger: logging.LoggerAdapter[Any] | None = None,
    ) -> None:
        self.node_id = node_id
        self.addr = advertise_addr
        self.storage = storage
        self.transport = transport
        self.clock = clock
        self.rng = rng
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout = election_timeout
        self.max_batch = max_batch
        self.on_commit = on_commit
        self.logger: logging.LoggerAdapter[Any] = logger or StateLogger(
            node_id, lambda: (self.role.value, self.term)
        )

        # persistent
        self.term = 0
        self.voted_for: str | None = None
        self.rlog = RaftLog()
        # volatile
        self.commit_index = 0
        self.last_applied = 0
        self.role = Role.FOLLOWER
        self.leader_id: str | None = None
        self.chat: list[ChatRecord] = []

        self._base_config = ClusterConfig(dict(bootstrap_voters), {})
        self.config = self._base_config
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._votes: set[str] = set()
        self._election_deadline = 0.0
        self._next_heartbeat = 0.0
        self._term_start_index = 0  # index of this leader's no-op

    # ------------------------------------------------------------------ properties
    @property
    def is_voter(self) -> bool:
        return self.config.is_voter(self.node_id)

    @property
    def is_member(self) -> bool:
        return self.config.is_member(self.node_id)

    def status(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_index": self.rlog.last_index,
            "voters": sorted(self.config.voters),
            "learners": sorted(self.config.learners),
        }

    def messages_after(self, seq: int) -> list[ChatRecord]:
        """Committed chat records with sequence number > ``seq`` (per-client cursor reads)."""
        return self.chat[max(0, seq) :]

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        """Recover persisted state; rebuild dedupe table, config and applied chat history."""
        state = await self.storage.load()
        self.term = state.current_term
        self.voted_for = state.voted_for
        self.rlog = RaftLog(state.log)
        self.commit_index = min(state.commit_index, self.rlog.last_index)
        self.last_applied = 0
        self.chat = []
        self._refresh_config()
        self._apply_committed()
        self._reset_election_timer()
        self.logger.info(
            "started: last_index=%d commit_index=%d voters=%s learners=%s",
            self.rlog.last_index,
            self.commit_index,
            sorted(self.config.voters),
            sorted(self.config.learners),
        )

    # ------------------------------------------------------------------ timers
    def _reset_election_timer(self) -> None:
        lo, hi = self.election_timeout
        self._election_deadline = self.clock.now() + self.rng.uniform(lo, hi)

    async def tick(self) -> None:
        now = self.clock.now()
        if self.role is Role.LEADER:
            if now >= self._next_heartbeat:
                self._broadcast_append()
                await self._maybe_promote_learner()
        elif self.is_voter and now >= self._election_deadline:
            await self._start_election()

    # ------------------------------------------------------------------ term handling
    async def _become_follower(self, term: int) -> None:
        term_changed = term > self.term
        if term_changed:
            self.term = term
            self.voted_for = None
        if self.role is not Role.FOLLOWER:
            self.logger.info("stepping down to follower (saw term %d)", term)
        self.role = Role.FOLLOWER
        if term_changed:
            self.leader_id = None
            await self.storage.save_term_vote(self.term, self.voted_for)
        self._reset_election_timer()

    # ------------------------------------------------------------------ election
    async def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._votes = {self.node_id}
        self._reset_election_timer()
        await self.storage.save_term_vote(self.term, self.voted_for)  # durable before asking
        self.logger.info("election timeout: starting election")
        if await self._maybe_win():
            return
        for peer in self.config.voters:
            if peer != self.node_id:
                self.transport.send(
                    peer,
                    VoteRequest(
                        sender=self.node_id,
                        term=self.term,
                        last_log_index=self.rlog.last_index,
                        last_log_term=self.rlog.last_term,
                    ),
                )

    async def _maybe_win(self) -> bool:
        votes = self._votes & set(self.config.voters)
        if self.role is Role.CANDIDATE and len(votes) >= self.config.quorum:
            await self._become_leader()
            return True
        return False

    async def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.node_id
        self.next_index = {}
        self.match_index = {}
        self._refresh_config()  # seeds next/match index for every peer
        self.logger.info("won election with votes %s", sorted(self._votes))
        noop = await self._append_local("noop")
        self._term_start_index = noop.index
        await self._advance_commit()  # single-voter clusters commit immediately
        self._broadcast_append()

    async def _on_vote_request(self, m: VoteRequest) -> None:
        if m.term > self.term:
            await self._become_follower(m.term)
        up_to_date = (m.last_log_term, m.last_log_index) >= (
            self.rlog.last_term,
            self.rlog.last_index,
        )
        granted = (
            m.term == self.term
            and self.is_voter
            and self.voted_for in (None, m.sender)
            and up_to_date
        )
        if granted:
            self.voted_for = m.sender
            await self.storage.save_term_vote(self.term, self.voted_for)  # durable before reply
            self._reset_election_timer()
            self.logger.info("granted vote to %s", m.sender)
        self.transport.send(
            m.sender, VoteResponse(sender=self.node_id, term=self.term, granted=granted)
        )

    async def _on_vote_response(self, m: VoteResponse) -> None:
        if m.term > self.term:
            await self._become_follower(m.term)
            return
        if self.role is Role.CANDIDATE and m.term == self.term and m.granted:
            self._votes.add(m.sender)
            await self._maybe_win()

    # ------------------------------------------------------------------ replication (leader)
    def _peers(self) -> list[str]:
        return [p for p in self.config.members() if p != self.node_id]

    def _send_append(self, peer: str) -> None:
        next_idx = max(1, self.next_index.get(peer, self.rlog.last_index + 1))
        prev = next_idx - 1
        self.transport.send(
            peer,
            AppendEntries(
                sender=self.node_id,
                term=self.term,
                prev_log_index=prev,
                prev_log_term=self.rlog.term_at(prev),
                entries=self.rlog.slice(next_idx, self.max_batch),
                leader_commit=self.commit_index,
            ),
        )

    def _broadcast_append(self) -> None:
        self._next_heartbeat = self.clock.now() + self.heartbeat_interval
        for peer in self._peers():
            self._send_append(peer)

    async def _append_local(
        self,
        kind: Literal["noop", "chat", "config"],
        *,
        msg_id: str = "",
        user: str = "",
        text: str = "",
        ts: float = 0.0,
        voters: dict[str, str] | None = None,
        learners: dict[str, str] | None = None,
    ) -> LogEntry:
        entry = LogEntry(
            index=self.rlog.last_index + 1,
            term=self.term,
            kind=kind,
            msg_id=msg_id,
            user=user,
            text=text,
            ts=ts,
            voters=voters or {},
            learners=learners or {},
        )
        await self.storage.replace_suffix(entry.index, [entry])  # durable before counting
        self.rlog.append(entry)
        if kind == "config":
            self._refresh_config()
        return entry

    async def handle_submit(self, msg_id: str, user: str, text: str, ts: float) -> SubmitResult:
        """Client submission. Only the leader appends; duplicates never create a second entry."""
        if self.role is not Role.LEADER:
            return SubmitResult("not_leader", leader_id=self.leader_id)
        existing = self.rlog.index_of_msg(msg_id)
        if existing is not None:
            return SubmitResult("duplicate", index=existing, leader_id=self.node_id)
        entry = await self._append_local("chat", msg_id=msg_id, user=user, text=text, ts=ts)
        await self._advance_commit()
        self._broadcast_append()
        return SubmitResult("accepted", index=entry.index, leader_id=self.node_id)

    async def _advance_commit(self) -> None:
        if self.role is not Role.LEADER:
            return
        voters = self.config.voters
        new_commit = self.commit_index
        for n in range(self.rlog.last_index, self.commit_index, -1):
            if self.rlog.term_at(n) != self.term:
                break  # only current-term entries are committed by counting
            replicas = sum(
                1 for v in voters if v == self.node_id or self.match_index.get(v, 0) >= n
            )
            if replicas >= self.config.quorum:
                new_commit = n
                break
        if new_commit > self.commit_index:
            self.commit_index = new_commit
            self._apply_committed()
            await self.storage.save_commit_index(self.commit_index)
            self._broadcast_append()  # tell followers promptly

    async def _on_append_ack(self, m: AppendAck) -> None:
        if m.term > self.term:
            await self._become_follower(m.term)
            return
        if self.role is not Role.LEADER or m.term < self.term:
            return
        peer = m.sender
        if peer not in self.next_index:
            return
        if m.success:
            self.match_index[peer] = max(self.match_index.get(peer, 0), m.match_index)
            self.next_index[peer] = max(self.next_index[peer], self.match_index[peer] + 1)
            await self._advance_commit()
            if self.role is Role.LEADER and self.next_index[peer] <= self.rlog.last_index:
                self._send_append(peer)
        else:
            self.next_index[peer] = max(1, min(m.conflict_index, self.next_index[peer] - 1))
            self._send_append(peer)

    # ------------------------------------------------------------------ replication (follower)
    async def _on_append_entries(self, m: AppendEntries) -> None:
        def reply(success: bool, match: int = 0, conflict: int = 0) -> None:
            self.transport.send(
                m.sender,
                AppendAck(
                    sender=self.node_id,
                    term=self.term,
                    success=success,
                    match_index=match,
                    conflict_index=conflict,
                ),
            )

        if m.term < self.term:
            reply(False)
            return
        if m.term > self.term or self.role is not Role.FOLLOWER:
            await self._become_follower(m.term)
        self.leader_id = m.sender
        self._reset_election_timer()

        for offset, entry in enumerate(m.entries):
            if entry.index != m.prev_log_index + 1 + offset:
                self.logger.warning("malformed APPEND_ENTRIES from %s: non-contiguous", m.sender)
                reply(False, conflict=self.rlog.last_index + 1)
                return
        if m.prev_log_index > self.rlog.last_index:
            reply(False, conflict=self.rlog.last_index + 1)
            return
        if m.prev_log_index > 0 and self.rlog.term_at(m.prev_log_index) != m.prev_log_term:
            bad_term = self.rlog.term_at(m.prev_log_index)
            reply(False, conflict=self.rlog.first_index_of_term(bad_term, m.prev_log_index))
            return

        truncate_at: int | None = None
        to_append: list[LogEntry] = []
        for entry in m.entries:
            if truncate_at is None and entry.index <= self.rlog.last_index:
                if self.rlog.term_at(entry.index) == entry.term:
                    continue  # already have it
                truncate_at = entry.index
            to_append.append(entry)

        if truncate_at is not None and truncate_at <= self.commit_index:
            raise RuntimeError(
                f"safety violation: leader asks to overwrite committed index {truncate_at}"
            )
        if to_append or truncate_at is not None:
            from_index = truncate_at if truncate_at is not None else to_append[0].index
            await self.storage.replace_suffix(from_index, to_append)  # durable before ACK
            if truncate_at is not None:
                self.rlog.truncate_from(truncate_at)
            for entry in to_append:
                self.rlog.append(entry)
            self._refresh_config()

        last_new = m.prev_log_index + len(m.entries)
        new_commit = min(m.leader_commit, last_new)
        if new_commit > self.commit_index:
            self.commit_index = new_commit
            self._apply_committed()
            await self.storage.save_commit_index(self.commit_index)
        reply(True, match=last_new)

    # ------------------------------------------------------------------ apply / config
    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.rlog.get(self.last_applied)
            if entry.kind != "chat":
                continue
            record = ChatRecord(
                seq=len(self.chat) + 1,
                index=entry.index,
                msg_id=entry.msg_id,
                user=entry.user,
                text=entry.text,
                ts=entry.ts,
            )
            self.chat.append(record)
            if self.on_commit is not None:
                self.on_commit(record)

    def _refresh_config(self) -> None:
        """Membership is whatever the latest config entry in the log says (Raft single-server)."""
        entry = self.rlog.latest_config_entry()
        self.config = (
            ClusterConfig(dict(entry.voters), dict(entry.learners)) if entry else self._base_config
        )
        for peer, addr in self.config.members().items():
            if peer != self.node_id:
                self.transport.set_peer_address(peer, addr)
        if self.role is Role.LEADER:
            for peer in self._peers():
                self.next_index.setdefault(peer, self.rlog.last_index + 1)
                self.match_index.setdefault(peer, 0)

    def _config_change_pending(self) -> bool:
        return self.rlog.last_config_index > self.commit_index

    # ------------------------------------------------------------------ membership
    async def handle_join(self, m: JoinRequest) -> JoinResponse:
        """Leader adds the newcomer as a non-voting learner; promotion happens in tick()."""
        self.transport.set_peer_address(m.sender, m.addr)
        if self.role is not Role.LEADER:
            leader_addr = self.config.members().get(self.leader_id or "")
            if self.leader_id == self.node_id:
                leader_addr = self.addr
            return JoinResponse(
                sender=self.node_id,
                term=self.term,
                accepted=False,
                status="not_leader",
                leader_id=self.leader_id,
                leader_addr=leader_addr,
            )
        if self.config.is_member(m.sender):
            return JoinResponse(
                sender=self.node_id,
                term=self.term,
                accepted=True,
                status="member",
                leader_id=self.node_id,
                leader_addr=self.addr,
            )
        if self._config_change_pending() or self.commit_index < self._term_start_index:
            return JoinResponse(
                sender=self.node_id,
                term=self.term,
                accepted=False,
                status="busy",
                leader_id=self.node_id,
                leader_addr=self.addr,
            )
        learners = {**self.config.learners, m.sender: m.addr}
        await self._append_local("config", voters=dict(self.config.voters), learners=learners)
        self.logger.info("added learner %s at %s", m.sender, m.addr)
        self._broadcast_append()
        return JoinResponse(
            sender=self.node_id,
            term=self.term,
            accepted=True,
            status="learner_added",
            leader_id=self.node_id,
            leader_addr=self.addr,
        )

    async def _maybe_promote_learner(self) -> None:
        """Promote one caught-up learner via a membership-change log entry."""
        if self._config_change_pending() or self.commit_index < self._term_start_index:
            return
        for learner, addr in sorted(self.config.learners.items()):
            if self.match_index.get(learner, 0) >= self.commit_index:
                voters = {**self.config.voters, learner: addr}
                learners = {k: v for k, v in self.config.learners.items() if k != learner}
                await self._append_local("config", voters=voters, learners=learners)
                self.logger.info("promoting learner %s to voter", learner)
                await self._advance_commit()
                self._broadcast_append()
                return

    # ------------------------------------------------------------------ dispatch
    async def on_message(self, msg: PeerMessageT) -> None:
        if isinstance(msg, VoteRequest):
            await self._on_vote_request(msg)
        elif isinstance(msg, VoteResponse):
            await self._on_vote_response(msg)
        elif isinstance(msg, AppendEntries):
            await self._on_append_entries(msg)
        elif isinstance(msg, AppendAck):
            await self._on_append_ack(msg)
        elif isinstance(msg, JoinRequest):
            self.transport.send(msg.sender, await self.handle_join(msg))
        elif isinstance(msg, ForwardRequest):
            result = await self.handle_submit(msg.msg_id, msg.user, msg.text, msg.ts)
            self.transport.send(
                msg.sender,
                ForwardResponse(
                    sender=self.node_id,
                    term=self.term,
                    msg_id=msg.msg_id,
                    status=result.status,
                    index=result.index,
                    leader_id=result.leader_id,
                ),
            )
        # PEER_HELLO / JOIN_RESPONSE / FORWARD_RESPONSE are handled by the node runtime.
