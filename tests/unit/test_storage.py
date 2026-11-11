import tempfile
from pathlib import Path

from dchat.node.raft.sqlite_storage import SqliteStorage
from dchat.protocol.messages import LogEntry


def mk(i: int, t: int = 1) -> LogEntry:
    return LogEntry(index=i, term=t, kind="chat", msg_id=f"m{i}", user="u", text=f"t{i}", ts=1.0)


async def test_persist_and_reload_after_close() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "n.db")
        s = await SqliteStorage.open(path)
        await s.save_term_vote(5, "node-b")
        await s.replace_suffix(1, [mk(1), mk(2), mk(3)])
        await s.save_commit_index(2)
        await s.close()
        s2 = await SqliteStorage.open(path)
        st = await s2.load()
        assert (st.current_term, st.voted_for, st.commit_index) == (5, "node-b", 2)
        assert [e.index for e in st.log] == [1, 2, 3] and st.log[2].text == "t3"
        await s2.close()


async def test_replace_suffix_truncates_conflicts() -> None:
    s = await SqliteStorage.open(":memory:")
    await s.replace_suffix(1, [mk(1), mk(2), mk(3)])
    await s.replace_suffix(2, [mk(2, t=2)])
    st = await s.load()
    assert [(e.index, e.term) for e in st.log] == [(1, 1), (2, 2)]
    await s.close()


async def test_vote_cleared_and_defaults() -> None:
    s = await SqliteStorage.open(":memory:")
    assert (await s.load()).voted_for is None
    await s.save_term_vote(2, "x")
    await s.save_term_vote(3, None)
    st = await s.load()
    assert (st.current_term, st.voted_for) == (3, None)
    await s.close()


async def test_wal_and_synchronous_full_on_disk() -> None:
    import sqlite3

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "n.db")
        s = await SqliteStorage.open(path)
        await s.save_term_vote(1, None)
        mode = sqlite3.connect(path).execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        await s.close()
