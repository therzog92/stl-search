#Requires -Version 5.1
<#
.SYNOPSIS
  Copy local STL Search secrets + Telegram session + channel data to the Synology NAS,
  then pull the latest Docker image and restart the container.

.EXAMPLE
  .\sync-to-synology.ps1
  .\sync-to-synology.ps1 -SshHost synology-nas
#>
param(
  [string]$SshHost = $(if ($env:STL_NAS_SSH) { $env:STL_NAS_SSH } else { "synology-nas" }),
  [string]$RemoteDir = $(if ($env:STL_INSTALL_DIR) { $env:STL_INSTALL_DIR } else { "/volume1/docker/stl-search" }),
  [switch]$SkipRestart
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Require-File([string]$Path, [string]$Hint) {
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Missing $Path — $Hint"
  }
}

Require-File (Join-Path $Root ".env") "create from .env.example and set TELEGRAM_API_ID / HASH"
Require-File (Join-Path $Root "data\stl_search.session") "log into STL Search on this PC first"

$ssh = Get-Command ssh -ErrorAction SilentlyContinue
$scp = Get-Command scp -ErrorAction SilentlyContinue
if (-not $ssh -or -not $scp) {
  throw "OpenSSH client required (ssh/scp). Install 'OpenSSH Client' optional feature on Windows."
}

Write-Host ""
Write-Host "========================================"
Write-Host "  STL Search → Synology sync"
Write-Host "========================================"
Write-Host "SSH host:    $SshHost"
Write-Host "Remote dir:  $RemoteDir"
Write-Host ""

Write-Host "→ Ensuring remote folders..."
& ssh $SshHost "mkdir -p '$RemoteDir/data' '$RemoteDir'"

Write-Host "→ Stopping container (if running) so the session file is free..."
& ssh $SshHost "cd '$RemoteDir' && (docker compose stop stl-search 2>/dev/null || docker-compose stop stl-search 2>/dev/null || docker stop stl-search 2>/dev/null || true)"

Write-Host "→ Uploading .env (API keys)..."
& scp (Join-Path $Root ".env") "${SshHost}:${RemoteDir}/.env"

if (Test-Path (Join-Path $Root "channels.txt")) {
  Write-Host "→ Uploading channels.txt (seeds)..."
  & scp (Join-Path $Root "channels.txt") "${SshHost}:${RemoteDir}/channels.txt"
}

$dataFiles = @(
  "stl_search.session",
  "discovered_channels.json",
  "channel_cache.json",
  "joined_channels.json",
  "blacklist.json",
  "join_web_progress.json",
  "download_history.json",
  "download_index.json"
)

Write-Host "→ Uploading data/ (session + channel lists + join log)..."
foreach ($name in $dataFiles) {
  $local = Join-Path $Root "data\$name"
  if (Test-Path -LiteralPath $local) {
    & scp $local "${SshHost}:${RemoteDir}/data/$name"
    Write-Host "   · $name"
  }
}

# Journal sidecar if Telethon created one
Get-ChildItem (Join-Path $Root "data") -Filter "stl_search.session*" -File -ErrorAction SilentlyContinue |
  ForEach-Object {
    & scp $_.FullName "${SshHost}:${RemoteDir}/data/$($_.Name)"
  }

if (-not $SkipRestart) {
  Write-Host "→ Refreshing compose file + pulling latest image..."
  & ssh $SshHost @"
set -e
cd '$RemoteDir'
curl -fsSL https://raw.githubusercontent.com/therzog92/stl-search/main/docker-compose.synology.yml -o docker-compose.yml
docker pull ghcr.io/therzog92/stl-search:latest
if docker compose version >/dev/null 2>&1; then
  docker compose up -d
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d
else
  echo 'ERROR: docker compose not available' >&2
  exit 1
fi
docker ps --filter name=stl-search --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
"@
}

Write-Host ""
Write-Host "Done. Open http://$SshHost:8787 (or your NAS IP) — you should already be logged in."
Write-Host "Note: don't run the Windows app and the NAS container with the same session at the same time."
Write-Host ""
