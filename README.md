# Distributed Chat (Python 3.12, asyncio)

Three independent server nodes (own process, port, SQLite file) that discover each other, elect a
leader and deliver chat messages in one agreed order using a **hand-written Raft-style replicated
log**. Clients connect over WebSocket (Textual terminal UI) and can reconnect to any node and catch up.

Docs: [protocol](docs/protocol.md) | [architecture](docs/architecture.md) | [testing](docs/testing.md) |
[demo script](docs/demo-script.md) | [requirements map](docs/requirements.md)

## 1. Install (every machine)

Python 3.12 and [uv](https://docs.astral.sh/uv/) are required.

```powershell
# Windows
winget install astral-sh.uv           # or: powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
uv sync                               # creates .venv and uv.lock  -> COMMIT uv.lock, use the same on both laptops
```
```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

## 2. Run the tests first

```
uv run pytest -q
uv run ruff check --fix . && uv run ruff format .    # first run: auto-fixes import ordering/style
uv run mypy src
```

## 3. Two physical machines

1. Put both laptops on the same network (a phone hotspot or a router is safest; university Wi-Fi
   often isolates clients). Note each laptop's IPv4 address (`ipconfig` / `ifconfig` / `ip addr`).
2. Edit `config/node-a.toml`, `node-b.toml`, `node-c.toml`, `node-d.toml`: replace `192.168.1.20`
   (Laptop 1) and `192.168.1.21` (Laptop 2) in `advertise_addr`, `seeds` and `[bootstrap_voters]`.
   The `bootstrap_voters` table must be identical in a, b, c. `advertise_addr` must be the address the
   OTHER machine can reach, never `0.0.0.0`. Nodes bind `0.0.0.0`.
3. Open the firewall once per laptop.
   - Windows (Administrator PowerShell): `.\scripts\firewall-windows.ps1`
   - Linux: `sudo ufw allow 9001:9004/tcp && sudo ufw allow 8001:8004/tcp`
   - macOS: allow incoming connections for Python when the prompt appears.
4. Start nodes (each in its own terminal):

   | Windows | macOS / Linux | Machine |
   |---|---|---|
   | `.\scripts\start-node.ps1 a` | `./scripts/start-node.sh a` | Laptop 1 |
   | `.\scripts\start-node.ps1 b` | `./scripts/start-node.sh b` | Laptop 1 |
   | `.\scripts\start-node.ps1 c` | `./scripts/start-node.sh c` | Laptop 2 |

   Equivalent direct command: `uv run python -m dchat.node --config config/node-a.toml`
   (`--no-ws` runs headless, `--log-level DEBUG` for more detail).
5. Check connectivity before blaming the code: from Laptop 1 run
   `Test-NetConnection 192.168.1.21 -Port 9003` (Windows) or `nc -vz 192.168.1.21 9003` (macOS/Linux).
6. Start clients: `uv run python -m dchat.client --user alice --server ws://192.168.1.20:8001/ws --server ws://192.168.1.21:8003/ws`
   (repeat `--server`; the client fails over between them). Type and press Enter; Ctrl+C quits.
7. Kill / restart a node (the demo's "kill -9"): `.\scripts\kill-node.ps1 a` /
   `./scripts/kill-node.sh a`, then start it again; it recovers from `data/node-a.db`.
8. Join a 4th node into the running cluster: `.\scripts\start-node.ps1 d` (config has no voters, only seeds).

## Troubleshooting

* Nodes never elect a leader: peers cannot reach each other. Re-check IPs in the TOML files, the firewall and `Test-NetConnection`.
* "startup failed: ... address already in use": another process owns the port; change `tcp_port`/`ws_port`.
* Laptop IP changed (DHCP): edit the TOML files again, restart nodes.
* Timer note (Windows): do not set election timeouts below ~150 ms (the config loader rejects it).
* If uvicorn complains about signal handling after an upgrade: `src/dchat/node/api/ws_server.py` (`_QuietServer`) overrides the two hooks uvicorn has used; check `uv run python -c "import uvicorn; print(uvicorn.__version__)"`.

## Scope decisions

No snapshots or log compaction (catch-up replays the log), no node removal, no pre-vote, one global
room, no auth/TLS. See `docs/protocol.md` for the exact guarantees and limits.
