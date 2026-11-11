"""Per-node TOML configuration with optional DCHAT_<KEY> environment overrides."""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,40}$")


class ConfigError(ValueError):
    pass


def split_addr(addr: str) -> tuple[str, int]:
    host, sep, port = addr.rpartition(":")
    if not sep or not host or not port.isdigit() or not (0 < int(port) < 65536):
        raise ConfigError(f"bad address {addr!r}, expected host:port")
    return host, int(port)


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    host: str
    tcp_port: int
    ws_port: int
    advertise_addr: str
    db_path: str
    heartbeat_ms: int
    election_timeout_ms: tuple[int, int]
    seeds: tuple[str, ...]
    bootstrap_voters: dict[str, str]


def _apply_env(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    for key, value in data.items():
        env = os.environ.get(f"DCHAT_{key.upper()}")
        if env is None or isinstance(value, (dict, list)):
            continue
        out[key] = int(env) if isinstance(value, int) else env
    return out


def parse_config(raw: dict[str, Any]) -> NodeConfig:
    data = _apply_env(raw)
    try:
        node_id = str(data["node_id"])
        tcp_port = int(data["tcp_port"])
        advertise = str(data["advertise_addr"])
        db_path = str(data["db_path"])
    except KeyError as exc:
        raise ConfigError(f"missing required key {exc}") from exc
    if not _ID_RE.match(node_id):
        raise ConfigError(f"node_id {node_id!r} must match {_ID_RE.pattern}")
    split_addr(advertise)
    lo, hi = (int(x) for x in data.get("election_timeout_ms", [1500, 3000]))
    hb = int(data.get("heartbeat_ms", 500))
    if lo < 150 or hi <= lo:
        raise ConfigError("election_timeout_ms must be [lo, hi] with lo >= 150 and hi > lo")
    if hb * 2 > lo:
        raise ConfigError("heartbeat_ms must be at most half of the minimum election timeout")
    voters = {str(k): str(v) for k, v in dict(data.get("bootstrap_voters", {})).items()}
    for nid, addr in voters.items():
        if not _ID_RE.match(nid):
            raise ConfigError(f"bad voter id {nid!r}")
        split_addr(addr)
    seeds = tuple(str(s) for s in data.get("seeds", []))
    for s in seeds:
        split_addr(s)
    return NodeConfig(
        node_id=node_id,
        host=str(data.get("host", "0.0.0.0")),  # noqa: S104 - binding all interfaces is intended
        tcp_port=tcp_port,
        ws_port=int(data.get("ws_port", 0)),
        advertise_addr=advertise,
        db_path=db_path,
        heartbeat_ms=hb,
        election_timeout_ms=(lo, hi),
        seeds=seeds,
        bootstrap_voters=voters,
    )


def load_config(path: str | Path) -> NodeConfig:
    with open(path, "rb") as fh:
        return parse_config(tomllib.load(fh))
