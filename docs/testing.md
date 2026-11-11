# Testing

```
uv run pytest -q          # everything (about 1 minute)
uv run pytest tests/unit  # pure logic, fake clock, no sockets
uv run ruff check . && uv run mypy src
```

| Layer | What it proves |
|---|---|
| `tests/unit/test_framing.py` | round trip, byte-by-byte partial reads, several frames per chunk, oversize, malformed (stream stays in sync), truncated, 300-case fuzz |
| `tests/unit/test_messages.py` | discriminated unions, bad types, version mismatch, client input validation |
| `tests/unit/test_log.py`, `test_storage.py` | contiguity, truncation, dedupe, config index; SQLite round trip, WAL, reload after close |
| `tests/unit/test_raft_election.py` | startup election, determinism (same seed => same result), leader crash re-election, old leader rejoins as follower, one vote per term, vote persisted before reply, up-to-date rule, higher term demotes, no leader without majority |
| `tests/unit/test_raft_replication.py` | same order everywhere, dedupe (also across failover/restart), persist-before-ACK, no commit of old-term entries by counting, divergent-log repair, refusal to overwrite committed entries, 25% message loss, single node, **randomized chaos** (crashes, restarts, partitions; checks log matching, one leader per term, identical committed order, no duplicates) |
| `tests/unit/test_raft_membership.py` | learner first, catch-up, promotion by log entry, redirect via follower, idempotent join, restart + failover with 4 voters |
| `tests/integration/test_tcp_cluster.py` | real sockets + SQLite: election, forwarding, failover, restart catch-up, full cluster restart, node join, garbage/oversize/truncated/spoofed frames, repeated peer disconnect |
| `tests/integration/test_client_session.py` | client protocol on a real cluster: same order on different nodes, reconnect catch-up without gaps/duplicates, duplicate retry, validation errors |
| `tests/failure/test_process_kill.py` | separate OS processes, `kill -9` on the leader, re-election, replication, restart from SQLite |
| `tests/unit/test_client_conn.py` | server rotation, resume from `last_seen_seq`, pending resend with same id, replay overlap ignored |

Not automated (do these on the two laptops and keep the logs as report evidence): cross-machine
connectivity, firewall behaviour, Textual UI, the FastAPI/uvicorn endpoint, loop-lag measurement
under load.
