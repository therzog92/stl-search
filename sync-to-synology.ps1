#Requires -Version 5.1
param(
  [string]$SshHost = $(if ($env:STL_NAS_SSH) { $env:STL_NAS_SSH } else { "synology-nas" }),
  [string]$RemoteDir = $(if ($env:STL_INSTALL_DIR) { $env:STL_INSTALL_DIR } else { "/volume1/docker/stl-search" }),
  [switch]$BuildOnNas
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Require-File([string]$Path, [string]$Hint) {
  if (-not (Test-Path -LiteralPath $Path)) {
    throw "Missing $Path - $Hint"
  }
}

Require-File (Join-Path $Root ".env") "set TELEGRAM_API_ID / HASH first"
Require-File (Join-Path $Root "data\stl_search.session") "log into STL Search on this PC first"

if (-not (Get-Command ssh -ErrorAction SilentlyContinue) -or -not (Get-Command scp -ErrorAction SilentlyContinue)) {
  throw "OpenSSH client required (ssh/scp)."
}

Write-Host ""
Write-Host "========================================"
Write-Host "  STL Search -> Synology deploy"
Write-Host "========================================"
Write-Host "SSH host:    $SshHost"
Write-Host "Remote dir:  $RemoteDir"
Write-Host ""

Write-Host "-> Stopping local STL Search on port 8787 (if any)..."
try {
  Get-NetTCPConnection -LocalPort 8787 -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -gt 0 } |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
} catch {}
Start-Sleep -Seconds 1

Write-Host "-> Ensuring remote folders..."
& ssh $SshHost "mkdir -p '$RemoteDir/data'"

Write-Host "-> Stopping NAS container..."
& ssh $SshHost "cd '$RemoteDir' && (docker compose stop 2>/dev/null || docker-compose stop 2>/dev/null || docker stop stl-search 2>/dev/null || true)"

Write-Host "-> Uploading .env..."
& scp (Join-Path $Root ".env") "${SshHost}:${RemoteDir}/.env"

$channels = Join-Path $Root "channels.txt"
if (-not (Test-Path $channels)) { $channels = Join-Path $Root "channels.example.txt" }
Write-Host "-> Uploading channels.txt..."
& scp $channels "${SshHost}:${RemoteDir}/channels.txt"

Write-Host "-> Uploading data/ (session + lists + join log)..."
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
foreach ($name in $dataFiles) {
  $local = Join-Path $Root "data\$name"
  if (Test-Path -LiteralPath $local) {
    & scp $local "${SshHost}:${RemoteDir}/data/$name"
    Write-Host "   $name"
  }
}
Get-ChildItem (Join-Path $Root "data") -Filter "stl_search.session*" -File -ErrorAction SilentlyContinue |
  ForEach-Object { & scp $_.FullName "${SshHost}:${RemoteDir}/data/$($_.Name)" }

if ($BuildOnNas) {
  Write-Host "-> Uploading app source for on-NAS build..."
  foreach ($f in @("Dockerfile", "requirements.txt", "run.py", "channels.example.txt", ".dockerignore")) {
    & scp (Join-Path $Root $f) "${SshHost}:${RemoteDir}/$f"
  }
  & scp -r (Join-Path $Root "app") "${SshHost}:${RemoteDir}/"
  $compose = @"
services:
  stl-search:
    build: .
    image: stl-search:local
    container_name: stl-search
    restart: unless-stopped
    ports:
      - "8787:8787"
    env_file:
      - .env
    environment:
      STL_HOST: "0.0.0.0"
      STL_PORT: "8787"
      STL_DOWNLOAD_DIR: "/downloads"
    volumes:
      - /volume1/docker/stl-search/data:/app/data
      - "/volume1/NAS_Shared/Telegram STLs:/downloads"
      - /volume1/docker/stl-search/channels.txt:/app/channels.txt:ro
"@
  $tmp = Join-Path $env:TEMP "stl-search-compose.yml"
  [System.IO.File]::WriteAllText($tmp, ($compose -replace "`r`n", "`n"))
  & scp $tmp "${SshHost}:${RemoteDir}/docker-compose.yml"
  Remove-Item $tmp -Force -ErrorAction SilentlyContinue

  Write-Host "-> Building on NAS and starting..."
  & ssh $SshHost "cd '$RemoteDir' && (docker compose build --pull && docker compose up -d --force-recreate) || (docker-compose build --pull && docker-compose up -d --force-recreate)"
} else {
  Write-Host "-> Refreshing compose + pulling ghcr.io/therzog92/stl-search:latest..."
  $remoteCmd = @"
set -e
cd '$RemoteDir'
curl -fsSL https://raw.githubusercontent.com/therzog92/stl-search/main/docker-compose.synology.yml -o docker-compose.yml
docker pull ghcr.io/therzog92/stl-search:latest
if docker compose version >/dev/null 2>&1; then
  docker compose up -d --force-recreate
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d --force-recreate
else
  echo 'ERROR: docker compose not available' >&2
  exit 1
fi
docker ps --filter name=stl-search --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -fsS -o /dev/null -w 'health:%{http_code}\n' http://127.0.0.1:8787/health || true
"@
  & ssh $SshHost ($remoteCmd -replace "`r`n", "`n")
}

Write-Host ""
Write-Host "Done. Open http://192.168.0.165:8787"
Write-Host "Do not run the Windows app with the same session while the NAS is using it."
Write-Host ""
