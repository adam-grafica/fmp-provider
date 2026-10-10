# FMP Windows local install (loopback-only). No admin required.
# Dry-run: $env:DRY_RUN=1; .\install.ps1
# Usage:   .\install.ps1 [-Smoke] [-Port 2089]
param([switch]$Smoke = $true, [int]$Port = 2089)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Dry = $env:DRY_RUN -eq "1"
function Run($cmd) { if ($Dry) { Write-Host "dry-run: $cmd" } else { Invoke-Expression $cmd } }

Write-Host "== FMP install (Windows, localhost-only) =="
try { $nv = node --version } catch { Write-Error "node 18+ required (https://nodejs.org)"; exit 1 }
Write-Host "node $nv"

if (-not (Test-Path "$Root\.env")) {
  Run "Copy-Item '$Root\.env.example' '$Root\.env'"
  Write-Host "created .env from .env.example — edit FMP_API_KEYS before shared use"
} else { Write-Host ".env exists, leaving untouched" }

Run "node --check '$Root\server.js'"
Run "node '$Root\bin\fmp.js' doctor"

if ($Smoke) {
  Write-Host "smoke: starting temp instance on 127.0.0.1:23117"
  if ($Dry) { Write-Host "dry-run: FMP_PORT=23117 node server.js & verify & stop" }
  else {
    $env:FMP_PORT = "23117"
    $p = Start-Process node -ArgumentList "$Root\server.js" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 2
    try {
      $env:FMP_BASE = "http://127.0.0.1:23117"
      node "$Root\bin\fmp.js" verify
    } finally { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
  }
}

Write-Host ""
Write-Host "OK. Start:  cd $Root; node server.js"
Write-Host "Uninstall: Remove-Item -Recurse '$Root'  (no services, no registry keys)"
Write-Host "Rollback: git checkout main -- .  (or restore from backup)"
