import random

from dchat.node.raft.core import Role
from dchat.protocol.messages import AppendEntries, LogEntry
from tests.support.sim import SimNet


async def test_message_commits_on_all_nodes_in_same_order() -> None:
    net = SimNet(["a", "b", "c"], seed=1)
    await net.start_all()
    leader = await net.wait_for_leader()
    for i in range(10):
        await leader.handle_submit(f"m{i}", "u", f"t{i}", float(i))
    await net.run(3)
    for n in net.nodes.values():
        assert [r.msg_id for r in n.chat] == [f"m{i}" for i in range(10)]
        assert [r.seq for r in n.chat] == list(range(1, 11))
    net.assert_logs_consistent()


async def test_duplicate_submission_creates_one_entry() -> None:
    net = SimNet(["a", "b", "c"], seed=2)
    await net.start_all()
    leader = await net.wait_for_leader()
    r1 = await leader.handle_submit("same", "u", "hello", 0.0)
    r2 = await leader.handle_submit("same", "u", "hello", 0.0)
    await net.run(2)
    assert (r1.status, r2.status, r1.index) == ("accepted", "duplicate", r2.index)
    assert [r.msg_id for r in leader.chat] == ["same"]


async def test_dedupe_survives_leader_failover_and_restart() -> None:
    net = SimNet(["a", "b", "c"], seed=12)
    await net.start_all()
    old = await net.wait_for_leader()
    await old.handle_submit("dup", "u", "once", 0.0)
    await net.run(2)
    net.crash(old.node_id)
    await net.run(6)
    new = net.leader()
    assert (await new.handle_submit("dup", "u", "once", 0.0)).status == "duplicate"
    restarted = await net.restart(old.node_id)
    assert restarted.rlog.index_of_msg("dup") is not None  # dedupe table rebuilt from the log


async def test_entry_persisted_before_ack_and_before_leader_counts_it() -> None:
    net = SimNet(["a", "b", "c"], seed=3)
    await net.start_all()
    leader = await net.wait_for_leader()
    follower = net.nodes[[i for i in net.nodes if i != leader.node_id][0]]
    events: list[str] = []
    orig = follower.storage.replace_suffix

    async def spy(i, entries):  # type: ignore[no-untyped-def]
        await orig(i, entries)
        events.append("persisted")

    follower.storage.replace_suffix = spy  # type: ignore[method-assign]
    real_send = follower.transport.send
    follower.transport.send = lambda p, m: (events.append(m.type), real_send(p, m))  # type: ignore[method-assign]
    await leader.handle_submit("m", "u", "x", 0.0)
    await net.run(1)
    assert events.index("persisted") < events.index("APPEND_ACK")


async def test_leader_does_not_commit_old_term_entry_by_counting() -> None:
    """Figure-8 style: an old-term entry on a majority must not be committed directly."""
    net = SimNet(["a", "b", "c"], seed=5)
    await net.start_all()
    leader = await net.wait_for_leader()
    # Hand-build: leader's log has an entry from an older term replicated on a majority.
    old_entry = LogEntry(
        index=leader.rlog.last_index + 1,
        term=leader.term - 1 if leader.term > 1 else 0,
        kind="chat",
        msg_id="old",
        text="o",
    )
    await net.nodes[leader.node_id].storage.replace_suffix(old_entry.index, [old_entry])
    leader.rlog.append(old_entry)
    for p in leader._peers():
        leader.match_index[p] = old_entry.index
    before = leader.commit_index
    await leader._advance_commit()
    assert leader.commit_index == before  # old-term entry alone never advances commit


async def test_follower_repairs_divergent_log() -> None:
    """A follower holding uncommitted junk from an older term must be repaired by the leader."""
    net = SimNet(["a", "b", "c"], seed=14)
    await net.start_all()
    first = await net.wait_for_leader()
    net.crash(first.node_id)
    await net.run(6)
    leader = net.leader()
    await net.restart(first.node_id)
    await net.run(4)
    assert leader.term >= 2
    victim = net.nodes[first.node_id]
    stale_term = leader.term - 1
    junk = [
        LogEntry(
            index=victim.rlog.last_index + k,
            term=stale_term,
            kind="chat",
            msg_id=f"junk{k}",
            text="j",
        )
        for k in (1, 2)
    ]
    await victim.storage.replace_suffix(junk[0].index, junk)
    for j in junk:
        victim.rlog.append(j)
    await leader.handle_submit("real", "u", "x", 0.0)
    await net.run(4)
    assert [r.msg_id for r in victim.chat] == [r.msg_id for r in leader.chat]
    assert victim.rlog.index_of_msg("junk1") is None and victim.rlog.index_of_msg("junk2") is None
    assert victim.rlog.last_index == leader.rlog.last_index
    net.assert_logs_consistent()


async def test_follower_refuses_to_overwrite_committed_entries() -> None:
    net = SimNet(["a", "b", "c"], seed=15)
    await net.start_all()
    leader = await net.wait_for_leader()
    await leader.handle_submit("m", "u", "x", 0.0)
    await net.run(2)
    f = net.nodes[[i for i in net.nodes if i != leader.node_id][0]]
    evil = LogEntry(index=f.commit_index, term=f.term + 10, kind="chat", msg_id="evil", text="e")
    try:
        await f.on_message(
            AppendEntries(
                sender="x",
                term=f.term + 10,
                prev_log_index=f.commit_index - 1,
                prev_log_term=f.rlog.term_at(f.commit_index - 1),
                entries=[evil],
            )
        )
    except RuntimeError as exc:
        assert "safety violation" in str(exc)
    else:
        raise AssertionError("committed entry was overwritten")


async def test_replication_converges_under_message_loss() -> None:
    net = SimNet(["a", "b", "c"], seed=21, drop_rate=0.25)
    await net.start_all()
    await net.wait_for_leader(limit=60)
    sent = 0
    for _ in range(30):
        await net.run(0.7)
        ls = net.leaders()
        if len(ls) == 1:
            await net.nodes[ls[0]].handle_submit(f"m{sent}", "u", "x", 0.0)
            sent += 1
    net.drop_rate = 0.0
    await net.run(10)
    lengths = {len(n.chat) for n in net.nodes.values()}
    assert len(lengths) == 1 and lengths.pop() > 0
    net.assert_logs_consistent()


async def test_single_node_cluster_commits_alone() -> None:
    net = SimNet(["solo"], seed=1)
    await net.start_all()
    leader = await net.wait_for_leader()
    await leader.handle_submit("m", "u", "x", 0.0)
    assert leader.role is Role.LEADER and [r.msg_id for r in leader.chat] == ["m"]


async def test_chaos_random_crashes_partitions_preserve_safety() -> None:
    rng = random.Random(1234)
    ids = ["a", "b", "c", "d", "e"]
    net = SimNet(ids, seed=77)
    await net.start_all()
    acked: list[str] = []
    counter = 0
    for round_no in range(120):
        action = rng.random()
        if action < 0.12 and len(net.up) > 3:
            net.crash(rng.choice(sorted(net.up)))
        elif action < 0.30 and len(net.up) < len(ids):
            await net.restart(rng.choice(sorted(set(ids) - net.up)))
        elif action < 0.38:
            net.heal()
            cut = set(rng.sample(ids, 2))
            net.partition(cut, set(ids) - cut)
        elif action < 0.46:
            net.heal()
        await net.run(rng.choice([0.3, 1.0, 2.5]))
        for lid in net.leaders():
            counter += 1
            res = await net.nodes[lid].handle_submit(f"m{counter}", "u", "x", 0.0)
            if res.status == "accepted":
                acked.append(f"m{counter}")
        net.assert_logs_consistent()
        terms_seen: dict[int, set[str]] = {}
        for nid in net.up:
            n = net.nodes[nid]
            if n.role is Role.LEADER:
                terms_seen.setdefault(n.term, set()).add(nid)
        assert all(len(v) == 1 for v in terms_seen.values()), (
            f"two leaders in one term: {terms_seen}"
        )
    net.heal()
    for i in set(ids) - net.up:
        await net.restart(i)
    await net.run(30)
    net.assert_logs_consistent()
    final = [r.msg_id for r in net.leader().chat]
    assert len(final) == len(set(final)), "duplicate delivery"
    assert len(final) > 0
