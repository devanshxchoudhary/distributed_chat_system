# Architecture

```mermaid
flowchart TB
    C1[Textual client] -- WebSocket --> WA
    C2[Textual client] -- WebSocket --> WC
    subgraph Laptop 1
      subgraph node-a process
        WA[FastAPI /ws] --> SA[ClusterNode event queue] --> RA[RaftNode] --> DA[(node-a.db)]
      end
      subgraph node-b process
        WB[FastAPI /ws] --> SB[ClusterNode event queue] --> RB[RaftNode] --> DB[(node-b.db)]
      end
    end
    subgraph Laptop 2
      subgraph node-c process
        WC[FastAPI /ws] --> SC[ClusterNode event queue] --> RC[RaftNode] --> DC[(node-c.db)]
      end
    end
    RA <-- "TCP: votes, heartbeats, replication" --> RB
    RB <--> RC
    RA <--> RC
```

## Modules (one responsibility each)

| Path | Responsibility |
|---|---|
| `protocol/framing.py` | length-prefixed JSON, partial reads, oversize/malformed handling |
| `protocol/messages.py`, `client_messages.py` | Pydantic wire models, discriminated unions |
| `node/raft/core.py` | election, replication, commit rule, membership (pure logic, injected Clock/Transport/Storage) |
| `node/raft/log.py` | in-memory log, dedupe table, config index |
| `node/raft/storage.py`, `sqlite_storage.py` | Storage interface; in-memory and aiosqlite (WAL, synchronous=FULL) |
| `node/network/tcp.py` | asyncio TCP server + per-peer links with reconnect/backoff, bounded queues |
| `node/cluster/discovery.py` | seed-based join loop |
| `node/runtime.py` | wiring, single event queue, TaskGroup, shutdown |
| `node/api/session.py` | WebSocket client protocol (framework-free) |
| `node/api/ws_server.py` | FastAPI adapter + embedded uvicorn |
| `node/api/hub.py` | per-client delivery cursors (ordered delivery buffer) |
| `client/ws_client.py`, `app.py` | reconnecting client logic; Textual UI |

## Concurrency model

One event loop per node. Every state-touching event (peer message, 50 ms tick, client submit) is
put on one queue and handled strictly one at a time, so consensus state needs no locks and a
handler's `await` on SQLite cannot interleave with another handler. Outbound sends are
non-blocking (bounded per-peer queues, oldest dropped). A handler exception is fatal by design: the
node stops and recovers from disk on restart. Timeouts use `time.monotonic()`; election timeouts are
randomized with a seedable `random.Random`.

## Ordered delivery

Committed chat entries are applied to an append-only list. Each connected client has a cursor into
that list and is pushed `list[cursor:]` in order, so entry 105 can never overtake 104 and a client
that reconnects (to any node) with `last_seen_seq` receives exactly what it missed.

## Election sequence

```mermaid
sequenceDiagram
    participant A as node-a (leader, dies)
    participant B as node-b
    participant C as node-c
    A-)B: APPEND_ENTRIES (heartbeat)
    A-)C: APPEND_ENTRIES (heartbeat)
    Note over A: kill -9
    Note over B: election timeout (random 1.5-3 s)
    B->>B: term+1, vote self, persist
    B-)C: VOTE_REQUEST(term, last_log)
    C->>C: persist vote, then reply
    C-)B: VOTE_RESPONSE(granted)
    Note over B: majority -> leader, append noop
    B-)C: APPEND_ENTRIES (noop)
    C-)B: APPEND_ACK (after SQLite commit)
```

## Replication sequence

```mermaid
sequenceDiagram
    participant U as Client
    participant F as Follower
    participant L as Leader
    participant G as Other follower
    U->>F: CLIENT_MESSAGE(msg_id)
    F-)L: CLIENT_FORWARD
    L->>L: dedupe check, append, persist
    L-)F: APPEND_ENTRIES
    L-)G: APPEND_ENTRIES
    F->>F: persist
    F-)L: APPEND_ACK
    Note over L: majority -> commit_index advances (own-term entry)
    L-)F: APPEND_ENTRIES (leader_commit)
    F-->>U: MESSAGE_DELIVER(seq)
```
