#Requires -Version 5.1
# Deploy keys/session + pull latest image on Synology.
# Uses scp -O (Synology SFTP is flaky). Docker needs sudo (password prompt).
param(
  [string]$SshHost = $(if ($env:STL_NAS_SSH) { $env:STL_NAS_SSH } else { "synology-nas" }),
  [string]$RemoteDir = "/volume1/docker/stl-search"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

if (-not (Test-Path .env)) { throw "Missing .env" }
if (-not (Test-Path "data\stl_search.session")) { throw "Missing data\stl_search.session" }

Write-Host "Stopping local app on :8787 (if any)..."
try {
  Get-NetTCPConnection -LocalPort 8787 -ErrorAction SilentlyContinue |
    Where-Object { $_.OwningProcess -gt 0 } |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
} catch {}
Start-Sleep -Seconds 1

Write-Host "Uploading .env, channels, data..."
ssh $SshHost "mkdir -p '$RemoteDir/data/incoming'"
scp -O .\.env "${SshHost}:${RemoteDir}/.env"

$channels = if (Test-Path .\channels.txt) { ".\channels.txt" } else { ".\channels.example.txt" }
scp -O $channels "${SshHost}:${RemoteDir}/channels.txt"

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
    scp -O $local "${SshHost}:${RemoteDir}/data/incoming/$name"
    Write-Host "  uploaded $name"
  }
}

$remoteSh = @"
set -e
export PATH=/usr/local/bin:/usr/bin:/bin:/sbin:/usr/sbin:`$PATH
DIR='$RemoteDir'
cd "`$DIR"

echo '==> Moving uploaded data into place (sudo)...'
sudo mkdir -p "`$DIR/data"
for f in stl_search.session discovered_channels.json channel_cache.json joined_channels.json blacklist.json join_web_progress.json download_history.json download_index.json; do
  if [ -f "`$DIR/data/incoming/`$f" ]; then
    sudo mv -f "`$DIR/data/incoming/`$f" "`$DIR/data/`$f"
    sudo chown Tommy:users "`$DIR/data/`$f" 2>/dev/null || true
    echo "  installed `$f"
  fi
done
sudo chmod 600 "`$DIR/data/stl_search.session" 2>/dev/null || true

echo '==> Refreshing compose file...'
curl -fsSL https://raw.githubusercontent.com/therzog92/stl-search/main/docker-compose.synology.yml -o "`$DIR/docker-compose.yml"

echo '==> Pulling ghcr.io/therzog92/stl-search:latest ...'
sudo docker pull ghcr.io/therzog92/stl-search:latest

echo '==> Starting container...'
cd "`$DIR"
sudo docker compose -f "`$DIR/docker-compose.yml" up -d --force-recreate

sleep 3
sudo docker ps --filter name=stl- --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
curl -fsS -m 8 -o /dev/null -w 'nas:%{http_code}\n' http://127.0.0.1:8787/health || echo 'nas:wait'
curl -fsS -m 8 -o /dev/null -w 'gateway:%{http_code}\n' http://127.0.0.1:8788/_gateway/status || echo 'gateway:wait'
echo 'DONE'
"@

$tmp = Join-Path $env:TEMP "stl-finish-nas.sh"
[System.IO.File]::WriteAllText($tmp, ($remoteSh -replace "`r`n", "`n"))
scp -O $tmp "${SshHost}:${RemoteDir}/finish-deploy.sh"
Remove-Item $tmp -Force

Write-Host ""
Write-Host "Enter your NAS sudo password when prompted..."
Write-Host ""
# -t = allocate TTY so sudo can ask for a password
ssh -t $SshHost "chmod +x '$RemoteDir/finish-deploy.sh' && bash '$RemoteDir/finish-deploy.sh'"

Write-Host ""
Write-Host "Open http://192.168.0.165:8787  (NAS direct)"
Write-Host "     http://192.168.0.165:8788  (gateway — Windows first)"
Write-Host "Reverse proxy should target localhost:8788 for remote HTTPS."
Write-Host ""
