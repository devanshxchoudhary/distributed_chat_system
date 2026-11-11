import asyncio

from dchat.protocol.framing import encode_frame
from tests.support.cluster import RealCluster, wait_until


def chat_ids(node) -> list[str]:  # type: ignore[no-untyped-def]
    return [r.msg_id for r in node.raft.chat]


async def test_three_nodes_elect_and_order_messages_over_tcp() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        leader = await c.wait_leader()
        follower = next(n for n in c.live() if n is not leader)
        for i in range(8):  # alternate entry points: leader and a follower (forwarded)
            await c.send_until_committed(leader if i % 2 == 0 else follower, f"m{i}", f"text {i}")
        orders = {tuple(chat_ids(n)) for n in c.live()}
        assert len(orders) == 1 and len(orders.pop()) == 8
        assert [r.seq for r in leader.raft.chat] == list(range(1, 9))
    finally:
        await c.close()


async def test_leader_crash_failover_and_restart_catch_up() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        old = await c.wait_leader()
        await c.send_until_committed(old, "before", "x")
        old_id = old.raft.node_id
        await c.stop(old_id)  # leader dies
        new = await c.wait_leader()
        assert new.raft.node_id != old_id and new.raft.term > old.raft.term
        await c.send_until_committed(new, "during", "x")
        restarted = await c.start(old_id)  # same SQLite file
        await wait_until(
            lambda: chat_ids(restarted) == ["before", "during"], 15, "restarted node catch-up"
        )
        await c.send_until_committed(new, "after", "x")
        assert {tuple(chat_ids(n)) for n in c.live()} == {("before", "during", "after")}
        assert len(c.leaders()) == 1
    finally:
        await c.close()


async def test_full_cluster_restart_recovers_history_from_disk() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        leader = await c.wait_leader()
        for i in range(5):
            await c.send_until_committed(leader, f"p{i}", "persist me")
        for nid in list(c.tasks):
            await c.stop(nid)
        await c.start_all()
        leader = await c.wait_leader()
        await c.send_until_committed(leader, "new", "post-restart")
        want = [f"p{i}" for i in range(5)] + ["new"]
        assert {tuple(chat_ids(n)) for n in c.live()} == {tuple(want)}
    finally:
        await c.close()


async def test_fourth_node_joins_running_cluster_via_seed() -> None:
    c = RealCluster(
        ["node-a", "node-b", "node-c", "node-d"], bootstrap=["node-a", "node-b", "node-c"]
    )
    try:
        await c.start_all()
        leader = await c.wait_leader()
        for i in range(3):
            await c.send_until_committed(leader, f"h{i}", "history")
        seed = f"127.0.0.1:{c.ports['node-a']}"
        d = await c.start("node-d", seeds=(seed,))
        await wait_until(lambda: d.raft.is_voter, 20, "node-d promoted to voter")
        await wait_until(lambda: chat_ids(d) == ["h0", "h1", "h2"], 10, "node-d log sync")
        leader = await c.wait_leader()
        assert sorted(leader.raft.config.voters) == ["node-a", "node-b", "node-c", "node-d"]
        await c.send_until_committed(leader, "h3", "after join")
        assert chat_ids(d)[-1] == "h3"
    finally:
        await c.close()


async def test_node_survives_garbage_oversize_and_truncated_frames() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        leader = await c.wait_leader()
        port = c.ports["node-a"]
        attacks = [
            b"\x00\x00\x00\x05hello",  # not JSON, bad handshake
            (2 * 1024 * 1024).to_bytes(4, "big") + b"x",  # oversize announcement
            encode_frame({"type": "PEER_HELLO", "sender": "evil", "addr": "127.0.0.1:1"})[
                :-3
            ],  # truncated
            encode_frame({"type": "PEER_HELLO", "sender": "evil", "addr": "127.0.0.1:1"})
            + encode_frame(
                {
                    "type": "VOTE_REQUEST",
                    "sender": "someone-else",
                    "term": 99,
                    "last_log_index": 0,
                    "last_log_term": 0,
                }
            ),  # sender spoofing
            b"\xff" * 4096,
        ]
        for blob in attacks:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(blob)
            await w.drain()
            await asyncio.sleep(0.05)
            w.close()
        await c.send_until_committed(leader, "still-works", "x")
        assert len(c.leaders()) == 1
    finally:
        await c.close()


async def test_peer_link_reconnects_after_peer_restart() -> None:
    c = RealCluster(["node-a", "node-b", "node-c"])
    try:
        await c.start_all()
        leader = await c.wait_leader()
        victim = next(n for n in c.live() if n is not leader).raft.node_id
        for round_no in range(3):  # repeated disconnect/reconnect
            await c.stop(victim)
            await c.send_until_committed(leader, f"r{round_no}-down", "x")
            await c.start(victim)
            await wait_until(
                lambda r=round_no: f"r{r}-down" in chat_ids(c.nodes[victim]),
                15,
                "reconnect+catch-up",
            )
        assert len(c.leaders()) == 1
    finally:
        await c.close()
