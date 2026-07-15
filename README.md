# STL Search (Docker)

Small FastAPI + Telethon app to search Telegram STL/3D channels, browse favorites,
and download/extract files to a folder on your Synology (or any Docker host).

**This public repo does not contain API keys, `.env`, or Telegram session files.**

---

## Synology — super simple install (Marius-style)

### Requirements
- Synology with **Container Manager** (Docker)
- Telegram API ID + Hash from https://my.telegram.org/apps

### Install (2 runs of one command)

**1. SSH into your NAS** (Control Panel → Terminal & SNMP → Enable SSH), then:

```bash
curl -fsSL https://raw.githubusercontent.com/therzog92/stl-search/main/install-synology.sh | bash
```

**2. First run only creates files.** Edit this file in File Station:

` /volume1/docker/stl-search/.env `

Set:

```env
TELEGRAM_API_ID=your_id
TELEGRAM_API_HASH=your_hash
```

**3. Run the same command again:**

```bash
curl -fsSL https://raw.githubusercontent.com/therzog92/stl-search/main/install-synology.sh | bash
```

**4. Open on your home Wi‑Fi:**

`http://YOUR-NAS-IP:8787`

Log in with Telegram once. Downloads go to:

`/volume1/NAS_Shared/Telegram STLs`

### Outside your house (like Jellyfin)

Use the **gateway** (published on NAS port **8787**) so remote HTTPS prefers your
Windows PC (PC-folder downloads) and falls back to the NAS container when the PC
is offline.

DSM → **Login Portal / Reverse Proxy** → rule:

| | |
|---|---|
| Source | `https://stl.yourdomain.com` port `443` |
| Destination | `http://localhost` port **`8787`** |

Assign your Let's Encrypt certificate to that hostname (same workflow as Jellyfin).

Optional in `/volume1/docker/stl-search/.env` if your PC’s LAN IP changes:

```env
STL_WINDOWS_UPSTREAM=http://192.168.0.88:8787
```

Give the PC a DHCP reservation so that IP stays stable. Allow Windows Firewall TCP **8787** from the NAS (see `install-windows-firewall.ps1`).

> Do **not** port-forward `8787` to the internet. Use HTTPS reverse proxy (or Tailscale).

**Telegram session:** Windows and the NAS must not both stay connected. With the
gateway stack, the NAS puts Telegram in **standby** while your PC’s `/health`
answers (see `STL_PEER_HEALTH_URL`). Opening the home page also stops any
running discover/join job.

The UI header shows **Windows · PC folder** or **Synology · NAS folder** for whichever backend answered.

---

## Moving from Windows → Synology

If you already logged in on your PC, copy API keys + Telegram session (no re-login):

```powershell
.\sync-to-synology.ps1
```

Uses your OpenSSH host `synology-nas` (or set `-SshHost`). Transfers `.env`,
`data/stl_search.session`, channel lists, and join log; pulls `:latest` and restarts.

Don’t run the Windows app and the NAS container on the **same session file** at once.
Remote HTTPS via the gateway (`:8788`) will prefer Windows when it’s up — quit the
Windows app if you need the NAS instance to own Telegram.

---

## Portainer / Dockge (paste stack)

1. Create folders: `/volume1/docker/stl-search/data` and `/volume1/NAS_Shared/Telegram STLs`
2. Create `/volume1/docker/stl-search/.env` from [`.env.example`](.env.example)
3. Paste [`docker-compose.synology.yml`](docker-compose.synology.yml) as a stack
4. Deploy

Image: `ghcr.io/therzog92/stl-search:latest`

If the image pull says denied, make the package **Public** once under  
GitHub → Packages → `stl-search` → Package settings → Change visibility.

---

## PC / Docker Desktop

```bash
cp .env.example .env
# edit .env
docker compose -f docker-compose.synology.yml up -d
```

(Edit volume paths for Windows/macOS.)

---

## Security note
The app stores a Telegram **session** under `/data`. Treat the web UI like access to your Telegram account — keep it behind HTTPS or VPN.
