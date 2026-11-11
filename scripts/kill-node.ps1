# Windows: hard-kill a node (the demo's "kill -9"): .\scripts\kill-node.ps1 a
param([Parameter(Mandatory=$true)][string]$Node)
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -like "*node-$Node.toml*" } |
  ForEach-Object { Write-Host "killing PID $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
