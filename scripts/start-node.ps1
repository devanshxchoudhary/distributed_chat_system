# Windows: .\scripts\start-node.ps1 a      (a | b | c | d)
param([Parameter(Mandatory=$true)][string]$Node)
uv run python -m dchat.node --config "config\node-$Node.toml"
