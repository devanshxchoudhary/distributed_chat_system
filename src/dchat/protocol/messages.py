"""Server-to-server wire messages and replicated log entries (Pydantic v2).

Every peer message carries ``version``, ``sender`` and ``term``. Ordering never comes
from timestamps; it comes from (term, index) in the replicated log.
"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from .constants import MAX_CHAT_TEXT, PROTOCOL_VERSION


class LogEntry(BaseModel):
    """One replicated log slot. ``index`` starts at 1 and is assigned by the leader.

    kind="chat"   -> a user message (msg_id is the dedupe key)
    kind="noop"   -> appended by a new leader so older entries can commit
    kind="config" -> full membership snapshot; takes effect when appended
    """

    index: int
    term: int
    kind: Literal["chat", "noop", "config"]
    msg_id: str = ""
    user: str = ""
    text: str = ""
    ts: float = 0.0
    voters: dict[str, str] = Field(default_factory=dict)
    learners: dict[str, str] = Field(default_factory=dict)


class PeerMessage(BaseModel):
    version: int = PROTOCOL_VERSION
    sender: str
    term: int = 0


class PeerHello(PeerMessage):
    type: Literal["PEER_HELLO"] = "PEER_HELLO"
    addr: str


class JoinRequest(PeerMessage):
    type: Literal["JOIN_REQUEST"] = "JOIN_REQUEST"
    addr: str


class JoinResponse(PeerMessage):
    """Membership answer. ``status`` is one of: learner_added, member, busy, not_leader."""

    type: Literal["JOIN_RESPONSE"] = "JOIN_RESPONSE"
    accepted: bool
    status: str
    leader_id: str | None = None
    leader_addr: str | None = None
    voters: dict[str, str] = Field(default_factory=dict)
    learners: dict[str, str] = Field(default_factory=dict)


class VoteRequest(PeerMessage):
    type: Literal["VOTE_REQUEST"] = "VOTE_REQUEST"
    last_log_index: int
    last_log_term: int


class VoteResponse(PeerMessage):
    type: Literal["VOTE_RESPONSE"] = "VOTE_RESPONSE"
    granted: bool


class AppendEntries(PeerMessage):
    """Replication and heartbeat (empty ``entries``). ``leader_commit`` is COMMIT_INDEX."""

    type: Literal["APPEND_ENTRIES"] = "APPEND_ENTRIES"
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry] = Field(default_factory=list)
    leader_commit: int = 0


class AppendAck(PeerMessage):
    """Sent only after the entries are durable in SQLite."""

    type: Literal["APPEND_ACK"] = "APPEND_ACK"
    success: bool
    match_index: int = 0
    conflict_index: int = 0


class ForwardRequest(PeerMessage):
    """A follower hands a client message to the leader."""

    type: Literal["CLIENT_FORWARD"] = "CLIENT_FORWARD"
    msg_id: str
    user: str
    text: str = Field(min_length=1, max_length=MAX_CHAT_TEXT)
    ts: float = 0.0


class ForwardResponse(PeerMessage):
    type: Literal["FORWARD_RESPONSE"] = "FORWARD_RESPONSE"
    msg_id: str
    status: str
    index: int = 0
    leader_id: str | None = None


PeerMessageT = (
    PeerHello
    | JoinRequest
    | JoinResponse
    | VoteRequest
    | VoteResponse
    | AppendEntries
    | AppendAck
    | ForwardRequest
    | ForwardResponse
)

_adapter: TypeAdapter[Any] = TypeAdapter(Annotated[PeerMessageT, Field(discriminator="type")])


class ProtocolVersionError(ValueError):
    """Peer speaks a different protocol version."""


def parse_peer_message(obj: dict[str, Any]) -> PeerMessageT:
    """Validate a decoded frame. Raises pydantic.ValidationError or ProtocolVersionError."""
    msg: PeerMessageT = _adapter.validate_python(obj)
    if msg.version != PROTOCOL_VERSION:
        raise ProtocolVersionError(f"unsupported protocol version {msg.version}")
    return msg


def dump_peer_message(msg: PeerMessageT) -> dict[str, Any]:
    return msg.model_dump()
