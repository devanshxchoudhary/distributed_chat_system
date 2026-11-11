from pytest import raises

from dchat.node.raft.log import RaftLog
from dchat.protocol.messages import LogEntry


def e(i: int, t: int, kind: str = "chat", mid: str = "") -> LogEntry:
    return LogEntry(index=i, term=t, kind=kind, msg_id=mid or f"m{i}", text="x")  # type: ignore[arg-type]


def test_append_contiguity_and_terms() -> None:
    log = RaftLog()
    log.append(e(1, 1))
    with raises(ValueError):
        log.append(e(3, 1))
    log.append(e(2, 2))
    assert (log.last_index, log.last_term, log.term_at(0), log.term_at(1)) == (2, 2, 0, 1)
    with raises(IndexError):
        log.term_at(3)


def test_dedupe_and_truncate() -> None:
    log = RaftLog([e(1, 1), e(2, 1), e(3, 2)])
    assert log.index_of_msg("m2") == 2
    log.truncate_from(2)
    assert log.last_index == 1 and log.index_of_msg("m2") is None and log.index_of_msg("m1") == 1


def test_config_index_tracking_and_first_index_of_term() -> None:
    log = RaftLog([e(1, 1), e(2, 1, "config"), e(3, 2), e(4, 2, "config")])
    assert log.last_config_index == 4 and log.latest_config_entry().index == 4  # type: ignore[union-attr]
    log.truncate_from(4)
    assert log.last_config_index == 2
    assert RaftLog([e(1, 1), e(2, 2), e(3, 2), e(4, 2)]).first_index_of_term(2, 4) == 2


def test_slice_limits() -> None:
    log = RaftLog([e(i, 1) for i in range(1, 11)])
    assert [x.index for x in log.slice(4, 3)] == [4, 5, 6]
    assert log.slice(11, 5) == []
