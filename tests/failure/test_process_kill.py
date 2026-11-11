"""Real OS processes (python -m dchat.node --no-ws), real kill -9, evidence from logs + SQLite."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

from dchat.protocol.framing import encode_frame
from dchat.protocol.messages import ForwardRequest, PeerHello, dump_peer_message
from tests.support.cluster import free_ports, wait_until

IDS = ["node-a", "node-b", "node-c"]


def toml_for(nid: str, ports: dict[str, int], db: Path) -> str:
    voters = "\n".join(f'{i} = "127.0.0.1:{ports[i]}"' for i in IDS)
    return (
        f'node_id = "{nid}"\nhost = "127.0.0.1"\ntcp_port = {ports[nid]}\nws_port = 0\n'
        f'advertise_addr = "127.0.0.1:{ports[nid]}"\ndb_path = "{db.as_posix()}"\n'
        f"heartbeat_ms = 100\nelection_timeout_ms = [500, 1000]\nseeds = []\n\n[bootstrap_voters]\n{voters}\n"
    )


class Proc:
    def __init__(self, nid: str, proc: asyncio.subprocess.Process) -> None:
        self.nid, self.proc, self.lines = nid, proc, []
        self.reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        assert self.proc.stdout is not None
        while line := await self.proc.stdout.readline():
            self.lines.append((time.monotonic(), line.decode(errors="replace").rstrip()))

    def won_after(self, t: float) -> bool:
        return any(ts > t and "won election" in ln for ts, ln in self.lines)


async def spawn(nid: str, cfg: Path) -> Proc:
    env = dict(
        os.environ, PYTHONPATH=os.pathsep.join(p for p in sys.path if p), PYTHONUNBUFFERED="1"
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "dchat.node",
        "--config",
        str(cfg),
        "--no-ws",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    return Proc(nid, proc)


async def inject(port: int, msg_id: str) -> None:
    """Act as a tiny peer: hello + CLIENT_FORWARD straight to the leader's TCP port."""
    _, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(encode_frame(dump_peer_message(PeerHello(sender="tester", addr="127.0.0.1:9"))))
    w.write(
        encode_frame(
            dump_peer_message(
                ForwardRequest(
                    sender="tester", msg_id=msg_id, user="t", text=f"text {msg_id}", ts=1.0
                )
            )
        )
    )
    await w.drain()
    await asyncio.sleep(0.2)
    w.close()


def chat_ids_on_disk(db: Path) -> list[str]:
    con = sqlite3.connect(db)
    try:
        import json

        rows = con.execute("SELECT data FROM log ORDER BY idx").fetchall()
        return [json.loads(r[0])["msg_id"] for r in rows if json.loads(r[0])["kind"] == "chat"]
    finally:
        con.close()


async def test_kill9_leader_reelects_and_committed_messages_survive() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        t = Path(tmp)
        ports = dict(zip(IDS, free_ports(3), strict=True))
        cfgs = {}
        for nid in IDS:
            cfgs[nid] = t / f"{nid}.toml"
            cfgs[nid].write_text(toml_for(nid, ports, t / f"{nid}.db"))
        procs = {nid: await spawn(nid, cfgs[nid]) for nid in IDS}
        try:
            await wait_until(
                lambda: sum(p.won_after(0) for p in procs.values()) == 1, 20, "first leader"
            )
            leader = next(n for n, p in procs.items() if p.won_after(0))
            # every state-bearing log line must carry node id, role and term
            tagged = [ln for p in procs.values() for _, ln in p.lines if "role=" in ln]
            assert tagged and all(
                re.search(r"\[node-[abc] role=(follower|candidate|leader) term=\d+\]", ln)
                for ln in tagged
            )
            await inject(ports[leader], "k1")
            await wait_until(
                lambda: all("k1" in chat_ids_on_disk(t / f"{n}.db") for n in IDS),
                10,
                "k1 replicated on disk",
            )

            kill_time = time.monotonic()
            procs[leader].proc.kill()  # SIGKILL / TerminateProcess: no cleanup runs
            await procs[leader].proc.wait()
            survivors = [n for n in IDS if n != leader]
            await wait_until(
                lambda: any(procs[n].won_after(kill_time) for n in survivors),
                15,
                "new leader after kill -9",
            )
            new_leader = next(n for n in survivors if procs[n].won_after(kill_time))
            await inject(ports[new_leader], "k2")
            await wait_until(
                lambda: all("k2" in chat_ids_on_disk(t / f"{n}.db") for n in survivors),
                10,
                "k2 replicated",
            )

            # restart the killed node from its SQLite file; it must catch up
            procs[leader] = await spawn(leader, cfgs[leader])
            await wait_until(
                lambda: "k2" in chat_ids_on_disk(t / f"{leader}.db"), 15, "killed node catches up"
            )
            orders = {tuple(chat_ids_on_disk(t / f"{n}.db")) for n in IDS}
            assert orders == {("k1", "k2")}, orders
        finally:
            for p in procs.values():
                if p.proc.returncode is None:
                    p.proc.kill()
                    await p.proc.wait()
                p.reader.cancel()
