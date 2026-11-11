"""Entry point: ``python -m dchat.node --config config/node-a.toml``."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .config import ConfigError, load_config
from .observability.logger import configure_logging
from .runtime import ClusterNode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dchat.node", description="Run one chat server node")
    parser.add_argument("--config", required=True, help="path to the node's TOML config")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--no-ws", action="store_true", help="disable the WebSocket API (headless)")
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    try:
        cfg = load_config(args.config)
    except (ConfigError, OSError) as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    node = ClusterNode(cfg, enable_ws=not args.no_ws)
    try:
        # asyncio.run cancels the main task on Ctrl+C (portable, incl. Windows), which runs
        # the TaskGroup / storage cleanup in ClusterNode.run's finally blocks.
        asyncio.run(_guarded(node))
    except KeyboardInterrupt:
        return 0
    except OSError as exc:
        print(f"startup failed: {exc}", file=sys.stderr)
        return 1
    return 0


async def _guarded(node: ClusterNode) -> None:
    """Unwrap TaskGroup ExceptionGroups so a bind failure surfaces as a plain OSError."""
    try:
        await node.run()
    except ExceptionGroup as group:
        first = group.exceptions[0]
        if isinstance(first, OSError):
            raise first from None
        raise


if __name__ == "__main__":
    raise SystemExit(main())
