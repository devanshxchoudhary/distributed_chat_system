from dchat.node.raft.core import Role
from dchat.protocol.messages import AppendEntries, VoteRequest
from tests.support.sim import SimNet


async def test_leader_elected_on_startup_and_unique_per_term() -> None:
    net = SimNet(["a", "b", "c"], seed=3)
    await net.start_all()
    leader = await net.wait_for_leader()
    await net.run(10)
    assert net.leaders() == [leader.node_id]
    terms = {n.term for n in net.nodes.values()}
    assert len(terms) == 1
    assert all(n.leader_id == leader.node_id for n in net.nodes.values())


async def test_same_seed_same_outcome_deterministic() -> None:
    async def run() -> tuple[str, int]:
        net = SimNet(["a", "b", "c"], seed=11)
        await net.start_all()
        leader = await net.wait_for_leader()
        return leader.node_id, leader.term

    assert await run() == await run()


async def test_leader_crash_triggers_new_election_within_bound() -> None:
    net = SimNet(["a", "b", "c"], seed=5)
    await net.start_all()
    old = await net.wait_for_leader()
    net.crash(old.node_id)
    await net.run(6)  # max election timeout 3s + a round
    new = net.leader()
    assert new.node_id != old.node_id and new.term > old.term


async def test_restarted_old_leader_rejoins_as_follower_without_overwriting() -> None:
    net = SimNet(["a", "b", "c"], seed=8)
    await net.start_all()
    old = await net.wait_for_leader()
    await old.handle_submit("m1", "u", "before", 0.0)
    await net.run(2)
    net.crash(old.node_id)
    await net.run(6)
    new = net.leader()
    await new.handle_submit("m2", "u", "during", 0.0)
    await net.run(2)
    restarted = await net.restart(old.node_id)
    await net.run(6)
    assert restarted.role is Role.FOLLOWER and net.leaders() == [new.node_id]
    assert [r.msg_id for r in restarted.chat] == ["m1", "m2"]
    net.assert_logs_consistent()


async def test_no_leader_without_majority() -> None:
    net = SimNet(["a", "b", "c"], seed=2)
    await net.start_all()
    leader = await net.wait_for_leader()
    others = [i for i in net.nodes if i != leader.node_id]
    for o in others:
        net.crash(o)
    await net.run(15)
    r = await leader.handle_submit("m", "u", "x", 0.0)
    await net.run(5)
    assert leader.commit_index < leader.rlog.last_index  # cannot commit without a majority
    assert r.status == "accepted" or r.status == "not_leader"


async def test_vote_granted_only_to_up_to_date_candidate() -> None:
    net = SimNet(["a", "b", "c"], seed=4)
    await net.start_all()
    leader = await net.wait_for_leader()
    await leader.handle_submit("m1", "u", "x", 0.0)
    await net.run(2)
    voter = net.nodes[[i for i in net.nodes if i != leader.node_id][0]]
    sent: list = []
    voter.transport.send = lambda peer, msg: sent.append(msg)  # type: ignore[method-assign]
    stale = VoteRequest(sender="c", term=voter.term + 1, last_log_index=0, last_log_term=0)
    await voter.on_message(stale)
    assert sent[-1].granted is False
    assert voter.term == stale.term  # still adopted the higher term
    fresh = VoteRequest(
        sender="c",
        term=voter.term + 1,
        last_log_index=voter.rlog.last_index,
        last_log_term=voter.rlog.last_term,
    )
    await voter.on_message(fresh)
    assert sent[-1].granted is True


async def test_one_vote_per_term_and_persisted_before_reply() -> None:
    net = SimNet(["a", "b", "c"], seed=6)
    await net.start_all()
    node = net.nodes["a"]
    sent: list = []
    order: list[str] = []
    orig = node.storage.save_term_vote

    async def spy(term: int, voted: str | None) -> None:
        order.append("persist")
        await orig(term, voted)

    node.storage.save_term_vote = spy  # type: ignore[method-assign]
    node.transport.send = lambda peer, msg: (order.append("send"), sent.append(msg))  # type: ignore[method-assign]
    t = node.term + 5
    await node.on_message(VoteRequest(sender="b", term=t, last_log_index=0, last_log_term=0))
    await node.on_message(VoteRequest(sender="c", term=t, last_log_index=0, last_log_term=0))
    assert [m.granted for m in sent] == [True, False]
    assert order.index("persist") < order.index("send")
    assert net.storages["a"].voted_for == "b" and net.storages["a"].term == t


async def test_higher_term_message_demotes_leader() -> None:
    net = SimNet(["a", "b", "c"], seed=9)
    await net.start_all()
    leader = await net.wait_for_leader()
    await leader.on_message(
        AppendEntries(sender="x", term=leader.term + 3, prev_log_index=0, prev_log_term=0)
    )
    assert leader.role is Role.FOLLOWER and leader.term > 0 and leader.leader_id == "x"


async def test_vote_state_survives_restart() -> None:
    net = SimNet(["a", "b", "c"], seed=10)
    await net.start_all()
    node = net.nodes["a"]
    t = node.term + 4
    await node.on_message(VoteRequest(sender="b", term=t, last_log_index=0, last_log_term=0))
    node2 = await net.restart("a")
    assert (node2.term, node2.voted_for) == (t, "b")
