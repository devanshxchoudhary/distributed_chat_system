#!/usr/bin/env bash
# macOS / Linux: ./scripts/start-node.sh a      (a | b | c | d)
set -euo pipefail
exec uv run python -m dchat.node --config "config/node-${1:?usage: start-node.sh a|b|c|d}.toml"
