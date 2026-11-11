"""``python -m dchat.client --user alice --server ws://HOST:8001/ws [--server ...]``"""

from __future__ import annotations

import argparse

from .app import ChatApp


def main() -> None:
    parser = argparse.ArgumentParser(prog="dchat.client")
    parser.add_argument("--user", required=True)
    parser.add_argument(
        "--server",
        action="append",
        required=True,
        help="ws://host:port/ws ; repeat for several nodes (the client fails over between them)",
    )
    args = parser.parse_args()
    ChatApp(args.server, args.user).run()


if __name__ == "__main__":
    main()
