# 15-minute demo script

Layout: **Laptop 1** runs node-a and node-b (two terminals). **Laptop 2** runs node-c, and later
node-d. Client terminals on either laptop. LAPTOP1 = `192.168.1.20`, LAPTOP2 = `192.168.1.21`
(edit `config/*.toml` to your real IPs first). Keep every node terminal visible: each log line
shows `[node role term]`, which is your evidence.

Before the demo: both laptops on the same LAN/hotspot, `scripts/firewall-windows.ps1` run once as
admin, `data/` folders deleted for a clean start, a screen recording as backup.

| Min | Step | Commands (Windows PowerShell; macOS/Linux use `./scripts/*.sh`) | Show |
|---|---|---|---|
| 2 | 1. Start cluster | L1: `.\scripts\start-node.ps1 a` and `... b`; L2: `.\scripts\start-node.ps1 c` | one node logs `won election`, others `granted vote` |
| 3 | 2. Messaging | `uv run python -m dchat.client --user alice --server ws://192.168.1.20:8001/ws --server ws://192.168.1.20:8002/ws --server ws://192.168.1.21:8003/ws` (L1) and the same with `--user bob --server ws://192.168.1.21:8003/ws ...` (L2). Send from both at once | same `[seq]` order on both screens; status bar shows leader + term |
| 2 | 3. Join a node | L2: `.\scripts\start-node.ps1 d` | log: join response, `added learner`, `promoting learner`, then node-d's chat history appears |
| 3 | 4. Crash the leader | on the leader's laptop: `.\scripts\kill-node.ps1 <leader letter>` | heartbeat silence, election timeout, `won election` on another node, higher term in status bar |
| 2 | 5. Recovery | keep sending; kill the client's node (it fails over by itself); restart the dead node with `start-node` | `last_seq` continues, no gaps/duplicates; restarted node logs `started: last_index=...` then catches up |
| 3 | 6. Q&A | | architecture, ordering, commit rule, limitations (`docs/protocol.md`) |

Safe talking points: Raft-style leader + majority replication; entries are fsynced before ACK;
only current-term entries are committed by counting; a 3-voter cluster survives 1 crash; with the
4th voter it still needs 3 of 4 (so also survives only 1). Limitations: no snapshots, no
pre-vote, no auth/TLS.
