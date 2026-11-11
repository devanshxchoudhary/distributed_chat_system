from pydantic import ValidationError
from pytest import raises

from dchat.protocol.client_messages import parse_client_message, parse_server_message
from dchat.protocol.messages import (
    AppendEntries,
    LogEntry,
    ProtocolVersionError,
    VoteRequest,
    dump_peer_message,
    parse_peer_message,
)


def test_peer_roundtrip_with_nested_entries() -> None:
    msg = AppendEntries(
        sender="a",
        term=3,
        prev_log_index=1,
        prev_log_term=2,
        entries=[LogEntry(index=2, term=3, kind="chat", msg_id="m", user="u", text="hi", ts=1.5)],
        leader_commit=1,
    )
    back = parse_peer_message(dump_peer_message(msg))
    assert isinstance(back, AppendEntries) and back.entries[0].text == "hi"


def test_unknown_type_and_missing_fields_rejected() -> None:
    with raises(ValidationError):
        parse_peer_message({"type": "NOPE", "sender": "a"})
    with raises(ValidationError):
        parse_peer_message({"type": "VOTE_REQUEST", "sender": "a", "term": 1})


def test_wrong_protocol_version_rejected() -> None:
    raw = dump_peer_message(VoteRequest(sender="a", term=1, last_log_index=0, last_log_term=0))
    raw["version"] = 99
    with raises(ProtocolVersionError):
        parse_peer_message(raw)


def test_bad_log_entry_kind_rejected() -> None:
    with raises(ValidationError):
        LogEntry(index=1, term=1, kind="evil")  # type: ignore[arg-type]


def test_client_messages_validation() -> None:
    assert (
        parse_client_message({"type": "CLIENT_HELLO", "user": "bob", "last_seen_seq": 4}).user
        == "bob"
    )  # type: ignore[union-attr]
    with raises(ValidationError):
        parse_client_message({"type": "CLIENT_MESSAGE", "msg_id": "x", "text": ""})
    with raises(ValidationError):
        parse_client_message({"type": "CLIENT_MESSAGE", "msg_id": "x", "text": "y" * 5000})
    with raises(ValidationError):
        parse_client_message({"type": "WELCOME"})
    assert (
        parse_server_message({"type": "CLIENT_ACK", "msg_id": "x", "status": "accepted"}).status
        == "accepted"
    )  # type: ignore[union-attr]
