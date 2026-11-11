from dchat.node.raft.core import Role
from dchat.protocol.messages import JoinRequest
from tests.support.sim import SimNet


async def _cluster_with_spare() -> tuple[SimNet, str]:
    net = SimNet(["a", "b", "c", "d"], seed=31, bootstrap=["a", "b", "c"])
    for nid in ("a", "b", "c"):
        await net.start(nid)
    await net.wait_for_leader()
    return net, "d"


async def test_new_node_joins_as_learner_catches_up_then_is_promoted() -> None:
    net, d = await _cluster_with_spare()
    leader = net.leader()
    for i in range(5):
        await leader.handle_submit(f"m{i}", "u", f"t{i}", 0.0)
    await net.run(2)
    await net.start(d)
    assert not net.nodes[d].is_member and net.nodes[d].role is Role.FOLLOWER
    net.enqueue(d, leader.node_id, JoinRequest(sender=d, addr=net.addr[d]))
    await net.run(0.2)
    assert d in leader.config.learners and d not in leader.config.voters  # learner first
    await net.run(5)
    assert d in leader.config.voters and not leader.config.learners  # promoted via log entry
    node_d = net.nodes[d]
    assert node_d.config.is_voter(d)
    assert [r.msg_id for r in node_d.chat] == [f"m{i}" for i in range(5)]
    assert sorted(leader.config.voters) == ["a", "b", "c", "d"]
    net.assert_logs_consistent()


async def test_learner_never_campaigns_or_votes() -> None:
    net, d = await _cluster_with_spare()
    await net.start(d)  # has empty config: not a voter
    term_before = net.leader().term
    await net.run(20)
    assert net.nodes[d].term == 0 and net.nodes[d].role is Role.FOLLOWER  # never campaigned
    assert net.leader().term == term_before  # and never disturbed the cluster
    assert d not in net.leader().config.voters


async def test_join_via_follower_redirects_to_leader() -> None:
    net, d = await _cluster_with_spare()
    leader = net.leader()
    follower = net.nodes[[i for i in ("a", "b", "c") if i != leader.node_id][0]]
    resp = await follower.handle_join(JoinRequest(sender=d, addr=net.addr[d]))
    assert (resp.accepted, resp.status, resp.leader_id, resp.leader_addr) == (
        False,
        "not_leader",
        leader.node_id,
        net.addr[leader.node_id],
    )


async def test_join_is_idempotent_and_cluster_keeps_working_with_four_voters() -> None:
    net, d = await _cluster_with_spare()
    await net.start(d)
    for _ in range(3):
        net.enqueue(d, net.leader().node_id, JoinRequest(sender=d, addr=net.addr[d]))
        await net.run(1)
    await net.run(5)
    leader = net.leader()
    assert sorted(leader.config.voters) == ["a", "b", "c", "d"]
    configs = [e for e in net.storages[leader.node_id].log if e.kind == "config"]
    assert len(configs) == 2  # add_learner + promote, never duplicated
    await leader.handle_submit("after", "u", "x", 0.0)
    await net.run(2)
    assert all(n.chat[-1].msg_id == "after" for n in net.nodes.values())


async def test_new_voter_survives_restart_and_leader_failover_with_four_nodes() -> None:
    net, d = await _cluster_with_spare()
    await net.start(d)
    net.enqueue(d, net.leader().node_id, JoinRequest(sender=d, addr=net.addr[d]))
    await net.run(6)
    restarted = await net.restart(d)
    assert restarted.config.is_voter(d)  # membership rebuilt from the persisted log
    old = net.leader()
    net.crash(old.node_id)
    await net.run(8)
    new = net.leader()
    await new.handle_submit("post-failover", "u", "x", 0.0)
    await net.run(3)
    assert new.node_id != old.node_id and new.chat[-1].msg_id == "post-failover"
    net.assert_logs_consistent()
