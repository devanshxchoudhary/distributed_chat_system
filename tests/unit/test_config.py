from pathlib import Path

from pytest import raises

from dchat.node.config import ConfigError, load_config, parse_config, split_addr

ROOT = Path(__file__).resolve().parents[2]


def base() -> dict:  # type: ignore[type-arg]
    return {
        "node_id": "node-x",
        "tcp_port": 9001,
        "advertise_addr": "10.0.0.5:9001",
        "db_path": "d.db",
    }


def test_shipped_example_configs_load_and_are_consistent() -> None:
    cfgs = {n: load_config(ROOT / "config" / f"node-{n}.toml") for n in "abcd"}
    assert {c.node_id for c in cfgs.values()} == {"node-a", "node-b", "node-c", "node-d"}
    assert cfgs["a"].bootstrap_voters == cfgs["b"].bootstrap_voters == cfgs["c"].bootstrap_voters
    assert (
        len({c.tcp_port for c in cfgs.values()}) == 4
        and len({c.ws_port for c in cfgs.values()}) == 4
    )
    assert cfgs["d"].bootstrap_voters == {} and cfgs["d"].seeds  # joiner: no voters, has seeds
    for n in "abc":
        assert cfgs[n].advertise_addr in cfgs[n].bootstrap_voters.values()


def test_defaults_and_validation() -> None:
    cfg = parse_config(base())
    assert (
        cfg.host == "0.0.0.0"
        and cfg.election_timeout_ms == (1500, 3000)
        and cfg.heartbeat_ms == 500
    )
    for bad in (
        {**base(), "node_id": "has space"},
        {**base(), "advertise_addr": "nohostport"},
        {**base(), "election_timeout_ms": [100, 200]},
        {**base(), "election_timeout_ms": [1500, 1500]},
        {**base(), "heartbeat_ms": 900, "election_timeout_ms": [1500, 3000]},
        {**base(), "seeds": ["x:notaport"]},
        {k: v for k, v in base().items() if k != "db_path"},
    ):
        with raises(ConfigError):
            parse_config(bad)


def test_env_override_and_split_addr() -> None:
    import os

    os.environ["DCHAT_DB_PATH"] = "/tmp/override.db"
    os.environ["DCHAT_HEARTBEAT_MS"] = "250"
    try:
        cfg = parse_config({**base(), "heartbeat_ms": 500})
        assert cfg.db_path == "/tmp/override.db" and cfg.heartbeat_ms == 250
    finally:
        del os.environ["DCHAT_DB_PATH"], os.environ["DCHAT_HEARTBEAT_MS"]
    assert split_addr("192.168.1.2:99") == ("192.168.1.2", 99)
    with raises(ConfigError):
        split_addr("host:99999")
