#!/usr/bin/env bash
# macOS / Linux: hard-kill a node (SIGKILL, no cleanup): ./scripts/kill-node.sh a
set -euo pipefail
pkill -9 -f "node-${1:?usage: kill-node.sh a|b|c|d}.toml" && echo "killed node-$1" || echo "node-$1 not running"
