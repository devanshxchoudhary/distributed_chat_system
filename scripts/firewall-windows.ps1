# Windows, run once per laptop in an ADMINISTRATOR PowerShell: opens the node + WebSocket ports.
New-NetFirewallRule -DisplayName "dchat" -Direction Inbound -Protocol TCP `
  -LocalPort 9001-9004,8001-8004 -Action Allow -Profile Any
