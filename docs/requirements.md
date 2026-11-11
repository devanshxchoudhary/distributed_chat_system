# Requirements -> feature -> test

| Requirement | Feature | Evidence |
|---|---|---|
| Multiple independent server processes | `python -m dchat.node` per node, own port + SQLite file | `tests/failure/test_process_kill.py` (3 OS processes) |
| Dynamic discovery / membership | seed join, learner -> promotion via log entry | `tests/unit/test_raft_membership.py`, `tests/integration/test_tcp_cluster.py::test_fourth_node_joins...` |
| Failure detection + leader election | heartbeats, randomized timeouts, persisted votes | `tests/unit/test_raft_election.py`, kill -9 test |
| Reliable ordered delivery | Raft-style log, commit rule, `seq` cursors | `tests/unit/test_raft_replication.py`, `tests/integration/test_client_session.py` |
| Persistence + crash recovery | SQLite WAL/FULL; state rebuilt on start | `tests/unit/test_storage.py`, full-restart + kill -9 tests |
| Client reconnect + catch-up | `last_seen_seq` resume, pending resend | `test_client_session.py`, `tests/unit/test_client_conn.py` |
| >= 2 physical machines, TCP between servers | LAN addresses from TOML, no broadcast | **manual**: `docs/demo-script.md` |
| Report + 15-minute demo | docs + demo script | **manual** |
