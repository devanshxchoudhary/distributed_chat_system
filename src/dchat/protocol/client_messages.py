"""Client <-> server WebSocket messages (JSON text frames, discriminated on ``type``)."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter

from .constants import MAX_CHAT_TEXT


class ClientHello(BaseModel):
    """First message on a connection. ``last_seen_seq`` drives catch-up after reconnect."""

    type: Literal["CLIENT_HELLO"] = "CLIENT_HELLO"
    user: str = Field(min_length=1, max_length=32)
    last_seen_seq: int = 0


class ClientMessage(BaseModel):
    type: Literal["CLIENT_MESSAGE"] = "CLIENT_MESSAGE"
    msg_id: str = Field(min_length=1, max_length=64)
    text: str = Field(min_length=1, max_length=MAX_CHAT_TEXT)


class Welcome(BaseModel):
    type: Literal["WELCOME"] = "WELCOME"
    node_id: str
    role: str
    term: int
    leader_id: str | None = None


class ClientAck(BaseModel):
    """Submission status. Final confirmation is the MESSAGE_DELIVER carrying the same msg_id."""

    type: Literal["CLIENT_ACK"] = "CLIENT_ACK"
    msg_id: str
    status: str  # accepted | duplicate | forwarded | no_leader


class MessageDeliver(BaseModel):
    """A committed message. ``seq`` is the gap-free chat sequence used for ordering."""

    type: Literal["MESSAGE_DELIVER"] = "MESSAGE_DELIVER"
    seq: int
    index: int
    msg_id: str
    user: str
    text: str
    ts: float


class NodeStatus(BaseModel):
    type: Literal["NODE_STATUS"] = "NODE_STATUS"
    node_id: str
    role: str
    term: int
    leader_id: str | None = None
    commit_index: int = 0
    last_index: int = 0
    voters: list[str] = Field(default_factory=list)
    learners: list[str] = Field(default_factory=list)


class ErrorMessage(BaseModel):
    type: Literal["ERROR"] = "ERROR"
    code: str
    detail: str = ""


ClientToServerT = ClientHello | ClientMessage
ServerToClientT = Welcome | ClientAck | MessageDeliver | NodeStatus | ErrorMessage

_c2s: TypeAdapter[Any] = TypeAdapter(Annotated[ClientToServerT, Field(discriminator="type")])
_s2c: TypeAdapter[Any] = TypeAdapter(Annotated[ServerToClientT, Field(discriminator="type")])


def parse_client_message(obj: dict[str, Any]) -> ClientToServerT:
    msg: ClientToServerT = _c2s.validate_python(obj)
    return msg


def parse_server_message(obj: dict[str, Any]) -> ServerToClientT:
    msg: ServerToClientT = _s2c.validate_python(obj)
    return msg
