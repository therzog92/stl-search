from pathlib import Path
import os

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "stl_search")
SESSION_PATH = ROOT / "data" / SESSION_NAME
CHANNELS_FILE = ROOT / "channels.txt"
DISCOVERED_FILE = ROOT / "data" / "discovered_channels.json"
CHANNEL_CACHE_FILE = ROOT / "data" / "channel_cache.json"
BLACKLIST_FILE = ROOT / "data" / "blacklist.json"
DOWNLOAD_HISTORY_FILE = ROOT / "data" / "download_history.json"
DOWNLOAD_INDEX_FILE = ROOT / "data" / "download_index.json"
THUMBS_DIR = ROOT / "data" / "thumbs"
DOWNLOAD_CACHE_DIR = ROOT / "data" / "downloads"
# PC save location when you pick "Desktop folder" / PC folder from the UI
_default_dl = Path(r"C:\Users\Tommy\Documents\3D Files\Telegram  Unorganized")
DOWNLOAD_DIR = Path(os.getenv("STL_DOWNLOAD_DIR", str(_default_dl))).expanduser()
# Parallel download workers (official Telegram uses several; Telethon default is 1)
DOWNLOAD_CONNECTIONS = int(os.getenv("STL_DOWNLOAD_CONNECTIONS", "12"))
DOWNLOAD_PART_KB = int(os.getenv("STL_DOWNLOAD_PART_KB", "512"))  # max Telegram allows

DISCOVERY_QUERIES = (
    "stl",
    "free stl",
    "3d printing",
    "3d print",
    "stl files",
    "3mf",
    "stl channel",
)
MIN_CHANNEL_MEMBERS = int(os.getenv("MIN_CHANNEL_MEMBERS", "2500"))
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "730"))
SEARCH_DELAY_SECONDS = float(os.getenv("SEARCH_DELAY_SECONDS", "1.2"))
# How many URL / t.me posts to scan per seed during deep crawl
LINK_CRAWL_LIMIT = int(os.getenv("LINK_CRAWL_LIMIT", "800"))

# Telemetr.io catalog API (https://t.me/telemetrio_api_bot → /api_key)
TELEMETR_API_KEY = os.getenv("TELEMETR_API_KEY", "")
TELEMETR_API_BASE = os.getenv("TELEMETR_API_BASE", "https://api.telemetr.io").rstrip("/")

FILE_EXTENSIONS = {".stl", ".3mf", ".zip", ".rar", ".7z"}

THUMBS_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
(ROOT / "data").mkdir(parents=True, exist_ok=True)


def load_seed_channels() -> list[str]:
    if not CHANNELS_FILE.exists():
        return []
    names: list[str] = []
    for line in CHANNELS_FILE.read_text(encoding="utf-8").splitlines():
        name = line.strip().lstrip("@")
        if name and not name.startswith("#"):
            names.append(name)
    return names
