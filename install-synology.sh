#!/bin/bash
# STL Search — one-shot Synology installer (SSH or Task Scheduler as root)
# Repo: https://github.com/therzog92/stl-search
set -euo pipefail

REPO_RAW="https://raw.githubusercontent.com/therzog92/stl-search/main"
IMAGE="ghcr.io/therzog92/stl-search:latest"
DIR="${STL_INSTALL_DIR:-/volume1/docker/stl-search}"
DOWNLOAD_DIR="${STL_DOWNLOAD_DIR_HOST:-/volume1/NAS_Shared/Telegram STLs}"

echo ""
echo "========================================"
echo "  STL Search — Synology installer"
echo "========================================"
echo "Install folder: $DIR"
echo "Downloads:      $DOWNLOAD_DIR"
echo ""

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found."
  echo "Install Container Manager from Package Center, then re-run."
  exit 1
fi

mkdir -p "$DIR/data" "$DOWNLOAD_DIR"
cd "$DIR"

echo "→ Downloading compose file..."
curl -fsSL "$REPO_RAW/docker-compose.synology.yml" -o docker-compose.yml

if [[ ! -f channels.txt ]]; then
  echo "→ Creating empty channels.txt (seeds)..."
  curl -fsSL "$REPO_RAW/channels.example.txt" -o channels.txt || touch channels.txt
fi

if [[ ! -f .env ]]; then
  echo "→ Creating .env template (first run)..."
  curl -fsSL "$REPO_RAW/.env.example" -o .env
  echo ""
  echo "============================================================"
  echo "  FIRST-TIME SETUP — edit your API keys, then run again"
  echo "============================================================"
  echo ""
  echo "1) Open this file on the NAS:"
  echo "     $DIR/.env"
  echo ""
  echo "2) Set YOUR values from https://my.telegram.org/apps :"
  echo "     TELEGRAM_API_ID=........"
  echo "     TELEGRAM_API_HASH=........"
  echo ""
  echo "   Optional: TELEMETR_API_KEY=... for Discover"
  echo ""
  echo "3) Save the file."
  echo ""
  echo "4) Run this installer ONE more time:"
  echo "     curl -fsSL $REPO_RAW/install-synology.sh | bash"
  echo ""
  echo "Or copy API keys + session from your PC:"
  echo "     .\\sync-to-synology.ps1"
  echo ""
  exit 0
fi

# Sanity-check placeholders
if grep -q "replace_with_your_api_hash\|TELEGRAM_API_ID=12345678" .env; then
  echo "ERROR: $DIR/.env still has placeholder API values."
  echo "Edit TELEGRAM_API_ID and TELEGRAM_API_HASH, then run this script again."
  echo "Or from your PC (with OpenSSH):  .\\sync-to-synology.ps1"
  exit 1
fi

echo "→ Pulling image $IMAGE ..."
docker pull "$IMAGE"

echo "→ Starting container..."
if docker compose version >/dev/null 2>&1; then
  docker compose up -d
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d
else
  echo "ERROR: docker compose not available."
  exit 1
fi

NAS_IP="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}' || true)"
if [[ -z "${NAS_IP:-}" ]]; then
  NAS_IP="<NAS-IP>"
fi

echo ""
echo "========================================"
echo "  DONE"
echo "========================================"
echo "Open on your home Wi‑Fi:"
echo "  http://${NAS_IP}:8787"
echo ""
echo "Data:       $DIR/data"
echo "Downloads:  $DOWNLOAD_DIR"
echo ""
if [[ ! -f "$DIR/data/stl_search.session" ]]; then
  echo "No Telegram session yet — log in once in the web UI,"
  echo "or copy your PC session with:  .\\sync-to-synology.ps1"
  echo ""
fi
echo "Outside access: add a Reverse Proxy for HTTPS"
echo "(same way you do Jellyfin) → port 8787 on localhost"
echo "========================================"
echo ""
